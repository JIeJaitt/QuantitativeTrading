"""
股票/ETF基本面数据获取工具 (使用 BaoStock)
使用 BaoStock 获取指定股票或ETF的30日基本面数据，并输出为Markdown格式
支持技术指标：量比、MACD、RSI、KDJ等
"""

import baostock as bs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import time
from functools import wraps


def retry(max_retries: int = 2, delay: float = 1.0, default=None):
    """
    重试装饰器：请求失败时自动重试，全部失败后返回默认值

    Args:
        max_retries: 最大重试次数，默认2次
        delay: 重试间隔秒数，默认1秒
        default: 全部失败后的默认返回值
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            result = None
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    # 检查返回值是否有效
                    if result is not None:
                        if isinstance(result, pd.DataFrame) and not result.empty:
                            return result
                        elif isinstance(result, dict) and result:
                            return result
                        elif isinstance(result, (pd.DataFrame, dict)):
                            # DataFrame 或 dict 为空，继续重试
                            if attempt < max_retries:
                                print(
                                    f"    数据为空，重试中 ({attempt + 1}/{max_retries})..."
                                )
                                time.sleep(delay)
                                continue
                        else:
                            return result
                    if attempt < max_retries:
                        print(f"    重试中 ({attempt + 1}/{max_retries})...")
                        time.sleep(delay)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        print(f"    请求失败，重试中 ({attempt + 1}/{max_retries})...")
                        time.sleep(delay)

            # 所有重试都失败，返回默认值而不是抛出异常
            if last_exception:
                print(f"    所有重试均失败: {type(last_exception).__name__}")

            # 根据函数返回类型返回合适的默认值
            if default is not None:
                return default
            # 尝试推断默认返回值
            if result is not None:
                return result
            return (
                {}
                if "dict" in str(func.__annotations__.get("return", ""))
                else pd.DataFrame()
            )

        return wrapper

    return decorator


def is_etf(stock_code: str) -> bool:
    """
    判断代码是否为ETF基金

    Args:
        stock_code: 证券代码

    Returns:
        是否为ETF
    """
    # 深圳ETF: 15xxxx, 16xxxx
    # 上海ETF: 51xxxx, 56xxxx, 58xxxx, 588xxx
    prefixes = ("15", "16", "51", "56", "58", "588")
    return stock_code.startswith(prefixes)


def format_stock_code(stock_code: str) -> str:
    """
    将股票代码转换为 BaoStock 格式

    Args:
        stock_code: 原始股票代码，如 "000001"

    Returns:
        BaoStock 格式，如 "sz.000001" 或 "sh.600519"
    """
    if stock_code.startswith(("6", "5", "9")):  # 上海
        return f"sh.{stock_code}"
    else:  # 深圳
        return f"sz.{stock_code}"


@retry(max_retries=2, delay=1.0, default={})
def get_stock_info(stock_code: str) -> dict:
    """
    获取股票/ETF基本信息

    Args:
        stock_code: 股票代码，如 "000001" 或 "600519"

    Returns:
        股票基本信息字典
    """
    bs_code = format_stock_code(stock_code)

    # 获取证券基本资料
    rs = bs.query_stock_basic(code=bs_code)
    if rs.error_code != "0":
        return {"股票代码": stock_code, "股票简称": stock_code}

    data_list = []
    while (rs.error_code == "0") & rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        return {"股票代码": stock_code, "股票简称": stock_code}

    # 字段: code, code_name, ipoDate, outDate, type, status
    row = data_list[0]

    info_dict = {
        "股票代码": stock_code,
        "股票简称": row[1] if len(row) > 1 else stock_code,
        "上市时间": row[2] if len(row) > 2 else "N/A",
        "股票类型": row[4] if len(row) > 4 else "N/A",
        "状态": row[5] if len(row) > 5 else "N/A",
    }

    return info_dict


@retry(max_retries=2, delay=1.0, default=pd.DataFrame())
def get_stock_history(stock_code: str, days: int = 30) -> pd.DataFrame:
    """
    获取股票历史行情数据

    Args:
        stock_code: 股票代码
        days: 获取的天数，默认30天

    Returns:
        历史行情DataFrame
    """
    bs_code = format_stock_code(stock_code)

    # 计算起止日期
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days + 30)).strftime("%Y-%m-%d")

    # 获取日线数据
    # frequency="d" 日线, adjustflag="2" 前复权
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,open,high,low,close,volume,amount,turn,pctChg",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2",
    )

    if rs.error_code != "0":
        return pd.DataFrame()

    # 转换为DataFrame
    data_list = []
    while (rs.error_code == "0") & rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        return pd.DataFrame()

    df = pd.DataFrame(data_list, columns=rs.fields)

    # 统一列名
    df = df.rename(
        columns={
            "date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
            "amount": "成交额",
            "turn": "换手率",
            "pctChg": "涨跌幅",
        }
    )

    # 转换数据类型
    for col in ["开盘", "最高", "最低", "收盘"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce")
    df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce")
    df["换手率"] = pd.to_numeric(df["换手率"], errors="coerce")
    df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")

    # 计算振幅
    df["振幅"] = ((df["最高"] - df["最低"]) / df["最低"] * 100).round(2)

    # 只取最近N个交易日
    df = df.tail(days)

    return df


def get_etf_history(stock_code: str, days: int = 30) -> pd.DataFrame:
    """
    获取ETF历史行情数据（BaoStock 对股票和ETF使用相同接口）

    Args:
        stock_code: ETF代码
        days: 获取的天数，默认30天

    Returns:
        历史行情DataFrame
    """
    return get_stock_history(stock_code, days)


@retry(max_retries=2, delay=1.0, default={})
def get_realtime_quote(stock_code: str) -> dict:
    """
    获取股票实时行情（使用最新一天的历史数据 + 估值指标）

    Args:
        stock_code: 股票代码

    Returns:
        实时行情字典
    """
    bs_code = format_stock_code(stock_code)

    # 获取基本行情数据
    df = get_stock_history(stock_code, days=1)

    if df.empty:
        return {}

    latest = df.iloc[-1]

    result = {
        "代码": stock_code,
        "最新价": latest.get("收盘"),
        "涨跌幅": latest.get("涨跌幅"),
        "涨跌额": (
            latest.get("收盘", 0) * latest.get("涨跌幅", 0) / 100
            if latest.get("收盘") and latest.get("涨跌幅")
            else 0
        ),
        "今开": latest.get("开盘"),
        "最高": latest.get("最高"),
        "最低": latest.get("最低"),
        "成交量": latest.get("成交量"),
        "成交额": latest.get("成交额"),
        "换手率": latest.get("换手率"),
        "振幅": latest.get("振幅"),
    }

    # 获取估值指标（市盈率、市净率等）
    try:
        # 计算日期
        trade_date = latest.get("日期", datetime.now().strftime("%Y-%m-%d"))

        # 查询估值指标
        rs_valuation = bs.query_history_k_data_plus(
            bs_code,
            "date,peTTM,pbMRQ,psTTM,pcfNcfTTM",  # 市盈率、市净率、市销率、市现率
            start_date=trade_date,
            end_date=trade_date,
            frequency="d",
            adjustflag="3",  # 不复权
        )

        if rs_valuation.error_code == "0":
            valuation_list = []
            while (rs_valuation.error_code == "0") & rs_valuation.next():
                valuation_list.append(rs_valuation.get_row_data())

            if valuation_list:
                val_data = valuation_list[0]
                # 字段: date, peTTM, pbMRQ, psTTM, pcfNcfTTM
                if len(val_data) > 1:
                    result["市盈率TTM"] = val_data[1] if val_data[1] else "N/A"
                if len(val_data) > 2:
                    result["市净率"] = val_data[2] if val_data[2] else "N/A"
                if len(val_data) > 3:
                    result["市销率TTM"] = val_data[3] if val_data[3] else "N/A"
                if len(val_data) > 4:
                    result["市现率TTM"] = val_data[4] if val_data[4] else "N/A"
    except Exception as e:
        # 估值指标获取失败不影响基本行情
        pass

    return result


def get_etf_realtime_quote(stock_code: str) -> dict:
    """
    获取ETF实时行情

    Args:
        stock_code: ETF代码

    Returns:
        实时行情字典
    """
    return get_realtime_quote(stock_code)


@retry(max_retries=2, delay=1.0, default=pd.DataFrame())
def get_financial_indicators(stock_code: str) -> pd.DataFrame:
    """
    获取股票财务指标数据

    Args:
        stock_code: 股票代码

    Returns:
        财务指标DataFrame
    """
    bs_code = format_stock_code(stock_code)

    # 获取最近4个季度的财务指标
    # 计算年份和季度
    today = datetime.now()
    year = today.year
    quarter = (today.month - 1) // 3 + 1

    all_data = []

    for i in range(4):
        # 计算对应的年份和季度
        q = quarter - i
        y = year
        while q <= 0:
            q += 4
            y -= 1

        # 获取季度财务指标
        rs = bs.query_profit_data(code=bs_code, year=y, quarter=q)

        if rs.error_code == "0":
            while (rs.error_code == "0") & rs.next():
                row_data = rs.get_row_data()
                if row_data:
                    all_data.append(row_data)
                    break

    if not all_data:
        return pd.DataFrame()

    # 构建DataFrame（简化版）
    df = pd.DataFrame(all_data, columns=rs.fields if all_data else [])

    return df.head(4)


def calculate_statistics(df: pd.DataFrame) -> dict:
    """
    计算统计指标

    Args:
        df: 历史行情DataFrame

    Returns:
        统计指标字典
    """
    if df.empty:
        return {}

    stats = {
        "30日最高价": df["最高"].max(),
        "30日最低价": df["最低"].min(),
        "30日均价": df["收盘"].mean(),
        "30日涨跌幅": (
            (df["收盘"].iloc[-1] - df["收盘"].iloc[0]) / df["收盘"].iloc[0] * 100
        ),
        "30日成交量均值": df["成交量"].mean(),
        "30日成交额均值": df["成交额"].mean(),
        "30日振幅均值": df["振幅"].mean() if "振幅" in df.columns else 0,
        "30日换手率均值": df["换手率"].mean() if "换手率" in df.columns else 0,
    }
    return stats


def calculate_technical_indicators(df: pd.DataFrame) -> dict:
    """
    计算技术指标：量比、MACD、RSI、KDJ等

    Args:
        df: 历史行情DataFrame，需包含收盘价和成交量

    Returns:
        技术指标字典
    """
    if df.empty or len(df) < 10:
        return {}

    indicators = {}
    close = df["收盘"].astype(float)
    volume = df["成交量"].astype(float)

    # ========== 量比 ==========
    if len(volume) >= 5:
        avg_volume_5 = volume.iloc[-6:-1].mean()
        current_volume = volume.iloc[-1]
        if avg_volume_5 > 0:
            indicators["量比"] = current_volume / avg_volume_5
        else:
            indicators["量比"] = 0

    # ========== MACD ==========
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd_bar = (dif - dea) * 2

    indicators["MACD_DIF"] = dif.iloc[-1]
    indicators["MACD_DEA"] = dea.iloc[-1]
    indicators["MACD柱"] = macd_bar.iloc[-1]

    # MACD 金叉/死叉判断
    if len(dif) >= 2:
        if dif.iloc[-1] > dea.iloc[-1] and dif.iloc[-2] <= dea.iloc[-2]:
            indicators["MACD信号"] = "金叉 ↑"
        elif dif.iloc[-1] < dea.iloc[-1] and dif.iloc[-2] >= dea.iloc[-2]:
            indicators["MACD信号"] = "死叉 ↓"
        elif dif.iloc[-1] > dea.iloc[-1]:
            indicators["MACD信号"] = "多头"
        else:
            indicators["MACD信号"] = "空头"

    # ========== RSI (14日) ==========
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    indicators["RSI_14"] = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 0

    # RSI 信号
    rsi_val = indicators["RSI_14"]
    if rsi_val >= 80:
        indicators["RSI信号"] = "超买 ⚠️"
    elif rsi_val >= 70:
        indicators["RSI信号"] = "偏强"
    elif rsi_val <= 20:
        indicators["RSI信号"] = "超卖 ⚠️"
    elif rsi_val <= 30:
        indicators["RSI信号"] = "偏弱"
    else:
        indicators["RSI信号"] = "中性"

    # ========== KDJ ==========
    if len(df) >= 9:
        low_min = df["最低"].astype(float).rolling(window=9).min()
        high_max = df["最高"].astype(float).rolling(window=9).max()

        rsv = (close - low_min) / (high_max - low_min).replace(0, np.nan) * 100
        rsv = rsv.fillna(50)

        k = rsv.ewm(com=2, adjust=False).mean()
        d = k.ewm(com=2, adjust=False).mean()
        j = 3 * k - 2 * d

        indicators["KDJ_K"] = k.iloc[-1]
        indicators["KDJ_D"] = d.iloc[-1]
        indicators["KDJ_J"] = j.iloc[-1]

        # KDJ 信号
        if k.iloc[-1] > d.iloc[-1] and k.iloc[-2] <= d.iloc[-2]:
            indicators["KDJ信号"] = "金叉 ↑"
        elif k.iloc[-1] < d.iloc[-1] and k.iloc[-2] >= d.iloc[-2]:
            indicators["KDJ信号"] = "死叉 ↓"
        elif j.iloc[-1] > 100:
            indicators["KDJ信号"] = "超买区"
        elif j.iloc[-1] < 0:
            indicators["KDJ信号"] = "超卖区"
        else:
            indicators["KDJ信号"] = "中性"

    # ========== 均线 ==========
    if len(close) >= 5:
        indicators["MA5"] = close.rolling(window=5).mean().iloc[-1]
    if len(close) >= 10:
        indicators["MA10"] = close.rolling(window=10).mean().iloc[-1]
    if len(close) >= 20:
        indicators["MA20"] = close.rolling(window=20).mean().iloc[-1]

    # 均线多空判断
    current_price = close.iloc[-1]
    ma5 = indicators.get("MA5", 0)
    ma10 = indicators.get("MA10", 0)
    ma20 = indicators.get("MA20", 0)

    if current_price > ma5 > ma10 > ma20:
        indicators["均线形态"] = "多头排列 ↑"
    elif current_price < ma5 < ma10 < ma20:
        indicators["均线形态"] = "空头排列 ↓"
    else:
        indicators["均线形态"] = "震荡整理"

    return indicators


def format_number(value, fmt: str = ".2f", default: str = "N/A") -> str:
    """
    安全格式化数值

    Args:
        value: 要格式化的值
        fmt: 格式化字符串
        default: 默认值

    Returns:
        格式化后的字符串
    """
    if (
        value is None
        or value == "N/A"
        or (isinstance(value, float) and np.isnan(value))
    ):
        return default
    try:
        if fmt == ",.0f":
            return f"{float(value):,.0f}"
        elif fmt == ".2f":
            return f"{float(value):.2f}"
        else:
            return str(value)
    except (ValueError, TypeError):
        return default


def generate_markdown(
    stock_code: str,
    stock_info: dict,
    realtime: dict,
    history_df: pd.DataFrame,
    stats: dict,
    financial_df: pd.DataFrame,
    indicators: dict = None,
) -> str:
    """
    生成Markdown格式报告

    Args:
        stock_code: 股票代码
        stock_info: 股票基本信息
        realtime: 实时行情
        history_df: 历史行情数据
        stats: 统计指标
        financial_df: 财务指标
        indicators: 技术指标

    Returns:
        Markdown格式字符串
    """
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stock_name = stock_info.get("股票简称", stock_code)

    md = f"""# {stock_name}（{stock_code}）基本面分析报告

> 报告生成时间：{report_date}
> 数据来源：BaoStock

---

## 一、股票基本信息

| 项目 | 内容 |
|------|------|
| 股票代码 | {stock_code} |
| 股票名称 | {stock_name} |
| 上市时间 | {stock_info.get('上市时间', 'N/A')} |
| 股票类型 | {stock_info.get('股票类型', 'N/A')} |

---

## 二、最新行情（最近交易日）

| 指标 | 数值 |
|------|------|
| 最新价 | {realtime.get('最新价', 'N/A')} |
| 涨跌幅 | {realtime.get('涨跌幅', 'N/A')}% |
| 涨跌额 | {format_number(realtime.get('涨跌额'))} |
| 今开 | {realtime.get('今开', 'N/A')} |
| 最高 | {realtime.get('最高', 'N/A')} |
| 最低 | {realtime.get('最低', 'N/A')} |
| 成交量 | {format_number(realtime.get('成交量'), ',.0f')} |
| 成交额 | {format_number(realtime.get('成交额'), ',.0f')} |
| 换手率 | {realtime.get('换手率', 'N/A')}% |
| 振幅 | {format_number(realtime.get('振幅'))}% |
| 市盈率TTM | {realtime.get('市盈率TTM', 'N/A')} |
| 市净率 | {realtime.get('市净率', 'N/A')} |
| 市销率TTM | {realtime.get('市销率TTM', 'N/A')} |
| 市现率TTM | {realtime.get('市现率TTM', 'N/A')} |

---

## 三、30日行情统计

| 统计指标 | 数值 |
|----------|------|
| 30日最高价 | {format_number(stats.get('30日最高价'))} |
| 30日最低价 | {format_number(stats.get('30日最低价'))} |
| 30日均价 | {format_number(stats.get('30日均价'))} |
| 30日涨跌幅 | {format_number(stats.get('30日涨跌幅'))}% |
| 30日成交量均值 | {format_number(stats.get('30日成交量均值'), ',.0f')} |
| 30日成交额均值 | {format_number(stats.get('30日成交额均值'), ',.0f')} |
| 30日振幅均值 | {format_number(stats.get('30日振幅均值'))}% |
| 30日换手率均值 | {format_number(stats.get('30日换手率均值'))}% |

---

## 三.5、技术指标分析

"""

    # 添加技术指标
    if indicators:
        md += """| 指标 | 数值 | 信号 |
|------|------|------|
"""
        # 量比
        md += f"| 量比 | {format_number(indicators.get('量比'))} | {'放量' if indicators.get('量比', 0) > 1.5 else '缩量' if indicators.get('量比', 0) < 0.8 else '正常'} |\n"

        # MACD
        md += f"| MACD (DIF) | {format_number(indicators.get('MACD_DIF'))} | {indicators.get('MACD信号', 'N/A')} |\n"
        md += f"| MACD (DEA) | {format_number(indicators.get('MACD_DEA'))} | - |\n"
        md += f"| MACD 柱 | {format_number(indicators.get('MACD柱'))} | - |\n"

        # RSI
        md += f"| RSI (14) | {format_number(indicators.get('RSI_14'))} | {indicators.get('RSI信号', 'N/A')} |\n"

        # KDJ
        md += f"| KDJ (K) | {format_number(indicators.get('KDJ_K'))} | {indicators.get('KDJ信号', 'N/A')} |\n"
        md += f"| KDJ (D) | {format_number(indicators.get('KDJ_D'))} | - |\n"
        md += f"| KDJ (J) | {format_number(indicators.get('KDJ_J'))} | - |\n"

        # 均线
        md += f"| MA5 | {format_number(indicators.get('MA5'))} | {indicators.get('均线形态', 'N/A')} |\n"
        md += f"| MA10 | {format_number(indicators.get('MA10'))} | - |\n"
        md += f"| MA20 | {format_number(indicators.get('MA20'))} | - |\n"
    else:
        md += "*暂无技术指标数据*\n"

    md += """
---

## 四、30日历史行情明细

| 日期 | 开盘 | 收盘 | 最高 | 最低 | 涨跌幅 | 成交量 | 换手率 |
|------|------|------|------|------|--------|--------|--------|
"""

    # 添加历史行情数据
    if not history_df.empty:
        for _, row in history_df.iterrows():
            date = row.get("日期", "N/A")
            open_price = row.get("开盘", "N/A")
            close_price = row.get("收盘", "N/A")
            high = row.get("最高", "N/A")
            low = row.get("最低", "N/A")
            change_pct = row.get("涨跌幅", "N/A")
            volume = row.get("成交量", "N/A")
            turnover = row.get("换手率", "N/A")

            # 安全格式化成交量
            volume_str = format_number(volume, ",.0f") if volume != "N/A" else "N/A"
            turnover_str = format_number(turnover) if turnover != "N/A" else "N/A"

            md += f"| {date} | {open_price} | {close_price} | {high} | {low} | {change_pct}% | {volume_str} | {turnover_str}% |\n"
    else:
        md += "| - | - | - | - | - | - | - | - |\n"

    md += """
---

## 五、财务指标（最近4个报告期）

"""

    if not financial_df.empty:
        # 转置表格更好展示
        md += (
            "| 指标 | "
            + " | ".join([str(col) for col in financial_df.columns])
            + " |\n"
        )
        md += "|------" + "|------" * len(financial_df.columns) + "|\n"
        for idx, row in financial_df.iterrows():
            md += f"| {idx} | " + " | ".join([str(val) for val in row.values]) + " |\n"
    else:
        md += "*暂无财务数据*\n"

    md += """
---

## 六、风险提示

1. 本报告数据来源于公开市场数据，仅供参考，不构成投资建议
2. 股市有风险，投资需谨慎
3. 历史业绩不代表未来表现

---

*本报告由 BaoStock 数据接口自动生成*
"""

    return md

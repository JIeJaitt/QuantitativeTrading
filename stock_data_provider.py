"""
统一股票数据接口层 — 掘金量化

数据源: 掘金量化 (MyQuant / GoldMiner)
  - SDK 安装: pip install gm
  - 需要掘金终端运行中
  - token 配置: config/config.yaml 中设置 gm_token 或环境变量 GM_TOKEN

主要功能:
  - 实时行情快照 (current)
  - 历史日K线 (history_n)
  - 当日分时数据 (history, 1分钟频率)
"""

import os

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ============================================================
# 掘金量化 SDK
# ============================================================

_GM_AVAILABLE = False

try:
    from gm.api import (
        set_token,
        current,
        history,
        history_n,
        get_symbol_infos,
        stk_get_daily_mktvalue_pt,
        stk_get_daily_valuation_pt,
        stk_get_daily_basic_pt,
        ADJUST_PREV,
    )

    _GM_AVAILABLE = True
except ImportError:
    pass

_GM_INITIALIZED = False


def _load_token() -> str:
    """读取掘金 token，优先级: 环境变量 > .env 文件 > config.yaml"""
    token = os.environ.get("GM_TOKEN", "")
    if token:
        return token

    # 从 .env 文件读取
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("GM_TOKEN=") and not line.startswith("#"):
                    token = line.split("=", 1)[1].strip().strip("'\"")
                    if token and token != "your_token_here":
                        return token

    # 兜底: config.yaml
    config_path = os.path.join(os.path.dirname(__file__), "config", "config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("gm_token:") and not line.startswith("#"):
                    val = line.split(":", 1)[1].strip().strip("'\"")
                    if val and val != "your_token_here":
                        return val
    return ""


def _ensure_init():
    """确保 gm SDK 已初始化（set_token 只需调用一次）"""
    global _GM_INITIALIZED
    if _GM_INITIALIZED:
        return
    if not _GM_AVAILABLE:
        raise RuntimeError("掘金量化 SDK 未安装，请执行: pip install gm")
    token = _load_token()
    if not token:
        raise RuntimeError(
            "掘金 token 未配置！\n"
            "  方式一: config/config.yaml 中设置 gm_token: your_token\n"
            "  方式二: 环境变量 GM_TOKEN=your_token"
        )
    set_token(token)
    _GM_INITIALIZED = True


def check_data_sources():
    """检查掘金量化 SDK 可用性与连通性"""
    print("=" * 50)

    if not _GM_AVAILABLE:
        raise RuntimeError(
            "掘金量化 SDK 未安装！请执行:\n" "  pip install gm\n" "并确保掘金终端已运行"
        )

    token = _load_token()
    if not token:
        raise RuntimeError(
            "掘金量化 token 未配置！请选择以下方式之一:\n"
            "  1. 在 config/config.yaml 中设置: gm_token: your_token_here\n"
            "  2. 设置环境变量: set GM_TOKEN=your_token_here\n"
            "token 可在掘金终端 > 用户 > 密钥管理 中获取"
        )

    print("数据源: 掘金量化 (MyQuant)")
    print("\n连通性检测:")
    try:
        _ensure_init()
        test = current(symbols="SHSE.600519")
        if test and len(test) > 0 and test[0].get("price", 0) > 0:
            price = test[0]["price"]
            print(f"  [OK] 掘金量化 — 连接成功 (测试: 贵州茅台 {price:.2f})")
        else:
            print("  [!!] 掘金量化 — 数据异常（请确保掘金终端已运行）")
    except Exception as e:
        print(f"  [!!] 掘金量化 — 连接失败: {e}")
        print("  请确保:")
        print("    1. 掘金终端已启动并登录")
        print("    2. token 正确（用户 > 密钥管理）")

    print("=" * 50)
    print()


# ============================================================
# 工具函数
# ============================================================


def is_etf(stock_code: str) -> bool:
    prefixes = ("15", "16", "51", "56", "58", "588")
    return stock_code.startswith(prefixes)


def _gm_symbol(code: str) -> str:
    """转换为掘金格式: 600519 → SHSE.600519, 000001 → SZSE.000001"""
    return f"SHSE.{code}" if code.startswith(("6", "5", "9")) else f"SZSE.{code}"


def get_data_source_summary() -> str:
    return "掘金量化"


_BOARD_MAP = {
    10100101: "主板",
    10100102: "创业板",
    10100103: "科创板",
    10100104: "北交所",
    10200101: "股票ETF",
    10200102: "债券ETF",
    10200103: "商品ETF",
    10200104: "跨境ETF",
    10200105: "货币ETF",
    10300101: "普通可转债",
    10300102: "可交换债券",
}


# ============================================================
# 数据获取
# ============================================================


def get_stock_info(stock_code: str) -> dict:
    """获取股票/ETF基本信息（名称、上市日期、板块等）"""
    _ensure_init()
    symbol = _gm_symbol(stock_code)
    sec_types = [1020, 1010] if is_etf(stock_code) else [1010, 1020]
    for sec_type in sec_types:
        try:
            infos = get_symbol_infos(sec_type1=sec_type, symbols=symbol, df=False)
            if infos and len(infos) > 0:
                info = infos[0]
                listed = info.get("listed_date")
                board_code = info.get("board", 0)
                return {
                    "股票代码": stock_code,
                    "股票简称": info.get("sec_name", stock_code),
                    "上市时间": str(listed)[:10] if listed else "N/A",
                    "板块": _BOARD_MAP.get(board_code, "N/A"),
                }
        except Exception:
            continue
    return {"股票代码": stock_code, "股票简称": stock_code}


def _get_prev_close(symbol: str) -> float:
    """获取上一交易日收盘价"""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        prev = history_n(
            symbol=symbol,
            frequency="1d",
            count=1,
            end_time=f"{today} 09:29:00",
            fields="close",
            df=False,
        )
        if prev and len(prev) > 0:
            return prev[0].get("close", 0)
    except Exception:
        pass
    return 0


def get_realtime_quote(stock_code: str) -> dict:
    """获取实时行情快照"""
    _ensure_init()
    symbol = _gm_symbol(stock_code)
    try:
        data = current(symbols=symbol)
        if not data or len(data) == 0:
            return {}
        d = data[0]
        price = d.get("price", 0)
        open_p = d.get("open", 0)
        high = d.get("high", 0)
        low = d.get("low", 0)
        cum_volume = d.get("cum_volume", 0)
        cum_amount = d.get("cum_amount", 0)

        prev_close = _get_prev_close(symbol)

        pct_change = 0
        change_amount = 0
        amplitude = 0
        if prev_close > 0:
            pct_change = round((price - prev_close) / prev_close * 100, 4)
            change_amount = round(price - prev_close, 4)
            if high > 0 and low > 0:
                amplitude = round((high - low) / prev_close * 100, 2)

        return {
            "代码": stock_code,
            "最新价": price,
            "今开": open_p,
            "最高": high,
            "最低": low,
            "成交量": cum_volume,
            "成交额": cum_amount,
            "涨跌幅": pct_change,
            "涨跌额": change_amount,
            "振幅": amplitude,
        }
    except Exception as e:
        print(f"  [掘金] 获取实时行情失败: {e}")
        return {}


def get_etf_realtime_quote(stock_code: str) -> dict:
    return get_realtime_quote(stock_code)


def get_stock_history(stock_code: str, days: int = 30) -> pd.DataFrame:
    """获取历史日K线数据"""
    _ensure_init()
    symbol = _gm_symbol(stock_code)
    try:
        df = history_n(
            symbol=symbol,
            frequency="1d",
            count=days,
            fields="symbol,open,high,low,close,volume,amount,eob,pre_close",
            adjust=ADJUST_PREV,
            df=True,
        )
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(
            columns={
                "eob": "日期",
                "open": "开盘",
                "close": "收盘",
                "high": "最高",
                "low": "最低",
                "volume": "成交量",
                "amount": "成交额",
                "pre_close": "昨收",
            }
        )

        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df["涨跌幅"] = ((df["收盘"] - df["昨收"]) / df["昨收"] * 100).round(4)
        df["振幅"] = ((df["最高"] - df["最低"]) / df["昨收"] * 100).round(2)
        df["换手率"] = np.nan

        return df[
            [
                "日期",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "涨跌幅",
                "成交量",
                "成交额",
                "振幅",
                "换手率",
            ]
        ]
    except Exception as e:
        print(f"  [掘金] 获取历史行情失败: {e}")
        return pd.DataFrame()


def get_etf_history(stock_code: str, days: int = 30) -> pd.DataFrame:
    return get_stock_history(stock_code, days)


def get_intraday_data(stock_code: str) -> pd.DataFrame:
    """获取今日分时数据（1分钟K线）"""
    _ensure_init()
    symbol = _gm_symbol(stock_code)
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        df = history(
            symbol=symbol,
            frequency="60s",
            start_time=f"{today} 09:30:00",
            end_time=f"{today} 15:00:00",
            fields="symbol,open,high,low,close,volume,amount,eob",
            adjust=ADJUST_PREV,
            df=True,
        )
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(
            columns={
                "eob": "时间",
                "open": "开盘",
                "close": "收盘",
                "high": "最高",
                "low": "最低",
                "volume": "成交量",
                "amount": "成交额",
            }
        )

        df["时间"] = pd.to_datetime(df["时间"]).dt.strftime("%H:%M")

        return df[["时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额"]]
    except Exception as e:
        print(f"  [掘金] 获取分时数据失败: {e}")
        return pd.DataFrame()


def get_financial_indicators(stock_code: str) -> pd.DataFrame:
    """获取财务指标（掘金免费版不提供，ETF无财务数据）"""
    return pd.DataFrame()


def get_stock_extra_data(stock_code: str) -> dict:
    """获取扩展数据: 市值、估值、换手率等（股票全量，ETF仅市值/换手率）"""
    _ensure_init()
    symbol = _gm_symbol(stock_code)
    etf = is_etf(stock_code)
    result = {}

    # 市值指标（股票和ETF都尝试）
    try:
        mv = stk_get_daily_mktvalue_pt(
            symbols=symbol, fields="tot_mv,a_mv", df=False
        )
        if mv and len(mv) > 0:
            d = mv[0]
            tot = d.get("tot_mv", 0)
            circ = d.get("a_mv", 0)
            if tot and tot > 0:
                result["总市值"] = tot
            if circ and circ > 0:
                result["流通市值"] = circ
    except Exception:
        pass

    # 估值指标（仅股票，ETF无市盈率等）
    if not etf:
        try:
            val = stk_get_daily_valuation_pt(
                symbols=symbol, fields="pe_ttm,pb_mrq,ps_ttm,dy_ttm", df=False
            )
            if val and len(val) > 0:
                d = val[0]
                for k, cn in [
                    ("pe_ttm", "市盈率TTM"),
                    ("pb_mrq", "市净率"),
                    ("ps_ttm", "市销率TTM"),
                    ("dy_ttm", "股息率"),
                ]:
                    v = d.get(k)
                    if v is not None and v != 0:
                        result[cn] = round(v, 2)
        except Exception:
            pass

    # 基础指标（股票和ETF都尝试）
    try:
        basic = stk_get_daily_basic_pt(
            symbols=symbol, fields="turnrate,ttl_shr,circ_shr", df=False
        )
        if basic and len(basic) > 0:
            d = basic[0]
            tr = d.get("turnrate")
            if tr is not None:
                result["换手率"] = round(tr, 2)
            ts = d.get("ttl_shr")
            if ts and ts > 0:
                result["总股本"] = ts
            cs = d.get("circ_shr")
            if cs and cs > 0:
                result["流通股本"] = cs
    except Exception:
        pass

    return result


# ============================================================
# 多周期K线与均线
# ============================================================

_MA_PERIODS = [5, 10, 20, 30, 60]


def _aggregate_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """将日K聚合为周K"""
    d = df.copy()
    d["_date"] = pd.to_datetime(d["日期"])
    d["_week"] = (
        d["_date"].dt.isocalendar().year.astype(str)
        + "-W"
        + d["_date"].dt.isocalendar().week.astype(str).str.zfill(2)
    )
    weekly = (
        d.groupby("_week", sort=True)
        .agg(
            日期=("日期", "last"),
            开盘=("开盘", "first"),
            收盘=("收盘", "last"),
            最高=("最高", "max"),
            最低=("最低", "min"),
            成交量=("成交量", "sum"),
            成交额=("成交额", "sum"),
        )
        .reset_index(drop=True)
    )
    return weekly


def _aggregate_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """将日K聚合为月K"""
    d = df.copy()
    d["_date"] = pd.to_datetime(d["日期"])
    d["_month"] = d["_date"].dt.to_period("M")
    monthly = (
        d.groupby("_month", sort=True)
        .agg(
            日期=("日期", "last"),
            开盘=("开盘", "first"),
            收盘=("收盘", "last"),
            最高=("最高", "max"),
            最低=("最低", "min"),
            成交量=("成交量", "sum"),
            成交额=("成交额", "sum"),
        )
        .reset_index(drop=True)
    )
    return monthly


def _calc_mas(df: pd.DataFrame, periods: list = None) -> dict:
    """计算均线值和方向，返回 {period: (value, direction)}"""
    if periods is None:
        periods = _MA_PERIODS
    if df.empty:
        return {}
    close = df["收盘"].astype(float)
    result = {}
    for p in periods:
        if len(close) < p:
            continue
        ma = close.rolling(window=p).mean()
        val = ma.iloc[-1]
        prev = ma.iloc[-2] if len(ma) >= 2 and not pd.isna(ma.iloc[-2]) else val
        direction = "↑" if val > prev else ("↓" if val < prev else "→")
        result[p] = (round(val, 4), direction)
    return result


def get_multi_period_ma(stock_code: str) -> dict:
    """获取日K/周K/月K的多周期均线数据

    返回:
        {
            "日K": {5: (value, "↑"), 10: (...), ...},
            "周K": {5: (value, "↓"), ...},
            "月K": {5: (value, "→"), ...},
            "latest_price": float,
        }
    """
    _ensure_init()
    symbol = _gm_symbol(stock_code)

    # 月K MA60 需要 ≥60 个月数据 ≈ 1300 个交易日；取 1500 留余量
    try:
        df = history_n(
            symbol=symbol,
            frequency="1d",
            count=1500,
            fields="symbol,open,high,low,close,volume,amount,eob",
            adjust=ADJUST_PREV,
            df=True,
        )
    except Exception as e:
        print(f"  [掘金] 获取扩展历史数据失败: {e}")
        return {}

    if df is None or df.empty:
        return {}

    df = df.rename(
        columns={
            "eob": "日期",
            "open": "开盘",
            "close": "收盘",
            "high": "最高",
            "low": "最低",
            "volume": "成交量",
            "amount": "成交额",
        }
    )
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")

    daily_ma = _calc_mas(df)
    weekly_ma = _calc_mas(_aggregate_to_weekly(df))
    monthly_ma = _calc_mas(_aggregate_to_monthly(df))

    return {
        "日K": daily_ma,
        "周K": weekly_ma,
        "月K": monthly_ma,
        "latest_price": float(df["收盘"].iloc[-1]),
    }


# ============================================================
# 计算函数（数据源无关）
# ============================================================


def calculate_statistics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    n = len(df)
    return {
        f"{n}日最高价": df["最高"].max(),
        f"{n}日最低价": df["最低"].min(),
        f"{n}日均价": df["收盘"].mean(),
        f"{n}日涨跌幅": (
            (df["收盘"].iloc[-1] - df["收盘"].iloc[0]) / df["收盘"].iloc[0] * 100
        ),
        f"{n}日成交量均值": df["成交量"].mean(),
        f"{n}日成交额均值": df["成交额"].mean(),
        f"{n}日振幅均值": df["振幅"].mean() if "振幅" in df.columns else 0,
        f"{n}日换手率均值": df["换手率"].mean() if "换手率" in df.columns else 0,
    }


def calculate_technical_indicators(df: pd.DataFrame) -> dict:
    if df.empty or len(df) < 10:
        return {}

    indicators = {}
    close = df["收盘"].astype(float)
    volume = df["成交量"].astype(float)

    # 量比
    if len(volume) >= 6:
        avg5 = volume.iloc[-6:-1].mean()
        if avg5 > 0:
            indicators["量比"] = volume.iloc[-1] / avg5

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd_bar = (dif - dea) * 2

    indicators["MACD_DIF"] = dif.iloc[-1]
    indicators["MACD_DEA"] = dea.iloc[-1]
    indicators["MACD柱"] = macd_bar.iloc[-1]

    if len(dif) >= 2:
        if dif.iloc[-1] > dea.iloc[-1] and dif.iloc[-2] <= dea.iloc[-2]:
            indicators["MACD信号"] = "金叉 ↑"
        elif dif.iloc[-1] < dea.iloc[-1] and dif.iloc[-2] >= dea.iloc[-2]:
            indicators["MACD信号"] = "死叉 ↓"
        elif dif.iloc[-1] > dea.iloc[-1]:
            indicators["MACD信号"] = "多头"
        else:
            indicators["MACD信号"] = "空头"

    # RSI (14)
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs_val = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs_val))
    indicators["RSI_14"] = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 0

    rsi_v = indicators["RSI_14"]
    if rsi_v >= 80:
        indicators["RSI信号"] = "超买"
    elif rsi_v >= 70:
        indicators["RSI信号"] = "偏强"
    elif rsi_v <= 20:
        indicators["RSI信号"] = "超卖"
    elif rsi_v <= 30:
        indicators["RSI信号"] = "偏弱"
    else:
        indicators["RSI信号"] = "中性"

    # KDJ
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

    # 均线
    if len(close) >= 5:
        indicators["MA5"] = close.rolling(window=5).mean().iloc[-1]
    if len(close) >= 10:
        indicators["MA10"] = close.rolling(window=10).mean().iloc[-1]
    if len(close) >= 20:
        indicators["MA20"] = close.rolling(window=20).mean().iloc[-1]

    price = close.iloc[-1]
    ma5 = indicators.get("MA5", 0)
    ma10 = indicators.get("MA10", 0)
    ma20 = indicators.get("MA20", 0)
    if price > ma5 > ma10 > ma20:
        indicators["均线形态"] = "多头排列 ↑"
    elif price < ma5 < ma10 < ma20:
        indicators["均线形态"] = "空头排列 ↓"
    else:
        indicators["均线形态"] = "震荡整理"

    return indicators


# ============================================================
# 格式化与报告生成
# ============================================================


def format_number(value, fmt: str = ".2f", default: str = "N/A") -> str:
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
        return str(value)
    except (ValueError, TypeError):
        return default


def _fmt_market_cap(val) -> str:
    """将市值（元）格式化为可读字符串"""
    if not val or val == "N/A":
        return "N/A"
    try:
        v = float(val)
        if v >= 1e12:
            return f"{v / 1e12:.2f} 万亿"
        if v >= 1e8:
            return f"{v / 1e8:.2f} 亿"
        if v >= 1e4:
            return f"{v / 1e4:.2f} 万"
        return f"{v:.2f}"
    except (ValueError, TypeError):
        return "N/A"


def generate_markdown(
    stock_code: str,
    stock_info: dict,
    realtime: dict,
    history_df: pd.DataFrame,
    stats: dict,
    financial_df: pd.DataFrame,
    indicators: dict = None,
    intraday_df: pd.DataFrame = None,
    multi_ma: dict = None,
    extra_data: dict = None,
) -> str:
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stock_name = stock_info.get("股票简称", realtime.get("名称", stock_code))
    source = get_data_source_summary()
    n = len(history_df)

    if extra_data is None:
        extra_data = {}

    pe = extra_data.get(
        "市盈率TTM",
        realtime.get("市盈率-动态", realtime.get("动态市盈率", "N/A")),
    )
    pb = extra_data.get("市净率", realtime.get("市净率", "N/A"))
    turnover = extra_data.get("换手率", realtime.get("换手率", "N/A"))
    turnover_str = f"{turnover}%" if turnover != "N/A" else "N/A"

    md = f"""# {stock_name}（{stock_code}）基本面分析报告

> 报告生成时间：{report_date}
> 数据来源：{source}

---

## 一、股票基本信息

| 项目 | 内容 |
|------|------|
| 股票代码 | {stock_code} |
| 股票名称 | {stock_name} |
| 板块 | {stock_info.get('板块', 'N/A')} |
| 上市时间 | {stock_info.get('上市时间', 'N/A')} |
| 总市值 | {_fmt_market_cap(extra_data.get('总市值'))} |
| 流通市值 | {_fmt_market_cap(extra_data.get('流通市值'))} |

---

## 二、实时行情

| 指标 | 数值 |
|------|------|
| 最新价 | {realtime.get('最新价', 'N/A')} |
| 涨跌幅 | {realtime.get('涨跌幅', 'N/A')}% |
| 涨跌额 | {realtime.get('涨跌额', 'N/A')} |
| 今开 | {realtime.get('今开', 'N/A')} |
| 最高 | {realtime.get('最高', 'N/A')} |
| 最低 | {realtime.get('最低', 'N/A')} |
| 成交量 | {format_number(realtime.get('成交量'), ',.0f')} |
| 成交额 | {format_number(realtime.get('成交额'), ',.0f')} |
| 换手率 | {turnover_str} |
| 振幅 | {format_number(realtime.get('振幅'))}% |
| 市盈率(TTM) | {pe} |
| 市净率 | {pb} |
| 市销率(TTM) | {extra_data.get('市销率TTM', 'N/A')} |
| 股息率 | {extra_data.get('股息率', 'N/A')}{'%' if extra_data.get('股息率', 'N/A') != 'N/A' else ''} |

---

"""

    # 今日分时数据
    md += "## 三、今日分时数据\n\n"
    if intraday_df is not None and not intraday_df.empty:
        md += "| 时间 | 开盘 | 收盘 | 最高 | 最低 | 成交量 | 成交额 |\n"
        md += "|------|------|------|------|------|--------|--------|\n"
        for _, row in intraday_df.iterrows():
            md += (
                f"| {row.get('时间', 'N/A')} "
                f"| {row.get('开盘', 'N/A')} "
                f"| {row.get('收盘', 'N/A')} "
                f"| {row.get('最高', 'N/A')} "
                f"| {row.get('最低', 'N/A')} "
                f"| {format_number(row.get('成交量'), ',.0f')} "
                f"| {format_number(row.get('成交额'), ',.0f')} |\n"
            )
    else:
        md += "*暂无分时数据（非交易时段或数据不可用）*\n"

    md += f"""
---

## 四、{n}日行情统计

| 统计指标 | 数值 |
|----------|------|
"""

    for key, val in stats.items():
        if "成交量" in key or "成交额" in key:
            md += f"| {key} | {format_number(val, ',.0f')} |\n"
        elif "幅" in key or "率" in key:
            md += f"| {key} | {format_number(val)}% |\n"
        else:
            md += f"| {key} | {format_number(val)} |\n"

    md += """
---

## 五、技术指标分析

"""

    if indicators:
        md += """| 指标 | 数值 | 信号 |
|------|------|------|
"""
        vr = indicators.get("量比", 0)
        vr_sig = "放量" if vr > 1.5 else ("缩量" if vr < 0.8 else "正常")
        md += f"| 量比 | {format_number(vr)} | {vr_sig} |\n"
        md += f"| MACD (DIF) | {format_number(indicators.get('MACD_DIF'))} | {indicators.get('MACD信号', 'N/A')} |\n"
        md += f"| MACD (DEA) | {format_number(indicators.get('MACD_DEA'))} | - |\n"
        md += f"| MACD 柱 | {format_number(indicators.get('MACD柱'))} | - |\n"
        md += f"| RSI (14) | {format_number(indicators.get('RSI_14'))} | {indicators.get('RSI信号', 'N/A')} |\n"
        md += f"| KDJ (K) | {format_number(indicators.get('KDJ_K'))} | {indicators.get('KDJ信号', 'N/A')} |\n"
        md += f"| KDJ (D) | {format_number(indicators.get('KDJ_D'))} | - |\n"
        md += f"| KDJ (J) | {format_number(indicators.get('KDJ_J'))} | - |\n"
        md += f"| MA5 | {format_number(indicators.get('MA5'))} | {indicators.get('均线形态', 'N/A')} |\n"
        md += f"| MA10 | {format_number(indicators.get('MA10'))} | - |\n"
        md += f"| MA20 | {format_number(indicators.get('MA20'))} | - |\n"
    else:
        md += "*暂无技术指标数据*\n"

    # 多周期均线
    md += "\n---\n\n## 六、多周期均线分析\n\n"
    if multi_ma and any(multi_ma.get(k) for k in ("日K", "周K", "月K")):
        price = multi_ma.get("latest_price", 0)
        for label in ("日K", "周K", "月K"):
            ma_data = multi_ma.get(label, {})
            if not ma_data:
                continue
            md += f"### {label}均线\n\n"
            md += "| 均线 | 数值 | 方向 | 与现价偏离 |\n"
            md += "|------|------|------|----------|\n"
            for p in _MA_PERIODS:
                if p not in ma_data:
                    continue
                val, direction = ma_data[p]
                deviation = ""
                if price > 0 and val > 0:
                    dev_pct = round((price - val) / val * 100, 2)
                    deviation = f"{dev_pct:+.2f}%"
                md += f"| MA{p} | {format_number(val, '.4f')} | {direction} | {deviation} |\n"
            md += "\n"
    else:
        md += "*暂无均线数据*\n"

    md += f"""
---

## 七、{n}日历史行情明细

| 日期 | 开盘 | 收盘 | 最高 | 最低 | 涨跌幅 | 成交量 | 换手率 |
|------|------|------|------|------|--------|--------|--------|
"""

    if not history_df.empty:
        for _, row in history_df.iterrows():
            md += (
                f"| {row.get('日期', 'N/A')} "
                f"| {row.get('开盘', 'N/A')} "
                f"| {row.get('收盘', 'N/A')} "
                f"| {row.get('最高', 'N/A')} "
                f"| {row.get('最低', 'N/A')} "
                f"| {row.get('涨跌幅', 'N/A')}% "
                f"| {format_number(row.get('成交量'), ',.0f')} "
                f"| {format_number(row.get('换手率'))}% |\n"
            )
    else:
        md += "| - | - | - | - | - | - | - | - |\n"

    md += """
---

## 八、财务指标（最近4个报告期）

"""

    if not financial_df.empty:
        md += "| 指标 | " + " | ".join(str(c) for c in financial_df.columns) + " |\n"
        md += "|------" + "|------" * len(financial_df.columns) + "|\n"
        for idx, row in financial_df.iterrows():
            md += f"| {idx} | " + " | ".join(str(v) for v in row.values) + " |\n"
    else:
        md += "*暂无财务数据*\n"

    md += f"""
---

## 九、风险提示

1. 本报告数据来源于公开市场数据，仅供参考，不构成投资建议
2. 股市有风险，投资需谨慎
3. 历史业绩不代表未来表现

---

*本报告由 {source} 数据接口自动生成*
"""
    return md


# ============================================================
# 清理资源
# ============================================================


def cleanup():
    pass

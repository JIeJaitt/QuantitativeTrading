"""
基金净值数据接口层

数据源:
  - akshare: 历史净值 (天天基金)
  - 天天基金 fundgz API: 实时估算净值
  - 天天基金 fund_open_fund_daily_em: 业绩统计

使用方法:
  python main.py 000001 --type fund
"""

import re
import json
import requests
import numpy as np
import pandas as pd
from datetime import datetime

_AK_AVAILABLE = False
try:
    import akshare as ak
    _AK_AVAILABLE = True
except ImportError:
    pass

_EM_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0"
    ),
    "Referer": "https://fund.eastmoney.com/",
}


def _em_get(url, params=None, timeout=10):
    try:
        resp = requests.get(url, params=params, headers=_EM_UA, timeout=timeout)
        if resp.status_code == 200:
            return resp
    except Exception:
        pass
    return None


# ============================================================
# 实时估算净值
# ============================================================

def get_fund_realtime_estimate(fund_code: str) -> dict:
    """获取基金实时估算净值（天天基金 fundgz 接口）

    返回字段:
        基金代码, 基金名称, 上一净值日期, 上一单位净值,
        估算净值, 估算涨跌幅(%), 估算时间
    """
    url = f"https://fundgz.1234567.com.cn/js/{fund_code}.js"
    try:
        resp = _em_get(url)
        if resp:
            m = re.search(r"jsonpgz\((.+)\)", resp.text)
            if m:
                data = json.loads(m.group(1))
                return {
                    "基金代码": data.get("fundcode", fund_code),
                    "基金名称": data.get("name", fund_code),
                    "上一净值日期": data.get("jzrq", "N/A"),
                    "上一单位净值": _safe_float(data.get("dwjz")),
                    "估算净值": _safe_float(data.get("gsz")),
                    "估算涨跌幅(%)": _safe_float(data.get("gszzl")),
                    "估算时间": data.get("gztime", "N/A"),
                }
    except Exception as e:
        print(f"  [天天基金] 获取实时估值失败: {e}")
    return {"基金代码": fund_code, "基金名称": fund_code}


# ============================================================
# 历史净值
# ============================================================

def get_fund_nav_history(fund_code: str, days: int = 90) -> pd.DataFrame:
    """获取基金单位净值历史（akshare → 天天基金）

    返回 DataFrame 列: 净值日期, 单位净值, 累计净值, 日涨跌幅(%)
    """
    if not _AK_AVAILABLE:
        print("  [akshare] 未安装，请执行: pip install akshare")
        return pd.DataFrame()

    nav_df = pd.DataFrame()
    acc_df = pd.DataFrame()

    # 单位净值
    try:
        raw = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势", period="全部时间")
        if raw is not None and not raw.empty:
            raw = raw.rename(columns=_col_mapper({
                "净值日期": ["净值日期", "date"],
                "单位净值": ["单位净值", "nav", "unit_nav"],
                "日涨跌幅(%)": ["日增长率", "日涨跌幅", "涨跌幅"],
                "申购状态": ["申购状态"],
                "赎回状态": ["赎回状态"],
            }, raw.columns))
            nav_df = raw
    except Exception as e:
        print(f"  [akshare] 获取单位净值失败: {e}")

    # 累计净值
    try:
        raw2 = ak.fund_open_fund_info_em(symbol=fund_code, indicator="累计净值走势", period="全部时间")
        if raw2 is not None and not raw2.empty:
            raw2 = raw2.rename(columns=_col_mapper({
                "净值日期": ["净值日期", "date"],
                "累计净值": ["累计净值", "acc_nav", "累计"],
            }, raw2.columns))
            acc_df = raw2[["净值日期", "累计净值"]] if "累计净值" in raw2.columns else pd.DataFrame()
    except Exception as e:
        print(f"  [akshare] 获取累计净值失败: {e}")

    if nav_df.empty:
        return pd.DataFrame()

    # 合并累计净值
    if not acc_df.empty and "净值日期" in nav_df.columns and "净值日期" in acc_df.columns:
        nav_df = nav_df.merge(acc_df, on="净值日期", how="left")

    # 排序并截取最近 days 条
    if "净值日期" in nav_df.columns:
        nav_df["净值日期"] = nav_df["净值日期"].astype(str)
        nav_df = nav_df.sort_values("净值日期").tail(days).reset_index(drop=True)

    return nav_df


# ============================================================
# 业绩表现
# ============================================================

def get_fund_performance(fund_code: str) -> dict:
    """获取基金业绩表现（akshare 天天基金全量数据筛选）

    返回: {近1周: x%, 近1月: x%, ..., 成立来: x%}
    """
    if not _AK_AVAILABLE:
        return {}

    try:
        df = ak.fund_open_fund_daily_em()
        if df is None or df.empty:
            return {}

        code_col = _find_col(df.columns, ["基金代码", "代码", "code"])
        if code_col is None:
            return {}

        row = df[df[code_col].astype(str) == fund_code]
        if row.empty:
            return {}

        r = row.iloc[0]
        perf_keys = [
            ("近1周",  ["近1周", "1周"]),
            ("近1月",  ["近1月", "1月"]),
            ("近3月",  ["近3月", "3月"]),
            ("近6月",  ["近6月", "6月"]),
            ("近1年",  ["近1年", "1年"]),
            ("近2年",  ["近2年", "2年"]),
            ("近3年",  ["近3年", "3年"]),
            ("今年来", ["今年来"]),
            ("成立来", ["成立来", "成立以来"]),
        ]
        result = {}
        for label, candidates in perf_keys:
            for c in candidates:
                if c in df.columns:
                    val = r.get(c)
                    if val is not None and str(val) not in ("", "-", "nan", "None", "--"):
                        result[label] = val
                    break
        return result

    except Exception as e:
        print(f"  [akshare] 获取业绩数据失败: {e}")
        return {}


# ============================================================
# 统计指标
# ============================================================

def calculate_fund_statistics(df: pd.DataFrame) -> dict:
    """根据净值历史计算区间统计指标"""
    if df.empty or "单位净值" not in df.columns:
        return {}

    nav = df["单位净值"].astype(float)
    n = len(nav)
    first, last = nav.iloc[0], nav.iloc[-1]

    stats = {
        f"{n}日最高净值": nav.max(),
        f"{n}日最低净值": nav.min(),
        f"{n}日平均净值": nav.mean(),
        f"{n}日区间涨跌幅(%)": round((last - first) / first * 100, 4) if first > 0 else 0,
    }

    if "日涨跌幅(%)" in df.columns:
        chg = df["日涨跌幅(%)"].astype(float)
        pos_days = (chg > 0).sum()
        neg_days = (chg < 0).sum()
        stats[f"{n}日上涨天数"] = int(pos_days)
        stats[f"{n}日下跌天数"] = int(neg_days)
        stats[f"最大单日涨幅(%)"] = chg.max()
        stats[f"最大单日跌幅(%)"] = chg.min()

    return stats


# ============================================================
# 报告生成
# ============================================================

def generate_fund_markdown(
    fund_code: str,
    estimate: dict,
    nav_df: pd.DataFrame,
    performance: dict,
    stats: dict,
) -> str:
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fund_name = estimate.get("基金名称", fund_code)
    n = len(nav_df)

    # ── 基本信息 ──
    md = f"""# {fund_name}（{fund_code}）基金净值分析报告

> 报告生成时间：{report_date}
> 数据来源：akshare / 天天基金

---

## 一、基金基本信息

| 项目 | 内容 |
|------|------|
| 基金代码 | {fund_code} |
| 基金名称 | {fund_name} |
| 上一净值日期 | {estimate.get('上一净值日期', 'N/A')} |
| 上一单位净值 | {_fmt(estimate.get('上一单位净值'))} |

---

## 二、实时估算净值

| 指标 | 数值 |
|------|------|
| 估算净值 | {_fmt(estimate.get('估算净值'))} |
| 估算涨跌幅 | {_fmt(estimate.get('估算涨跌幅(%)'))}% |
| 估算时间 | {estimate.get('估算时间', 'N/A')} |
| 上一单位净值 | {_fmt(estimate.get('上一单位净值'))} |

---

## 三、业绩表现

"""

    if performance:
        md += "| 区间 | 涨跌幅 |\n|------|--------|\n"
        for label, val in performance.items():
            md += f"| {label} | {val}% |\n"
    else:
        md += "*暂无业绩数据*\n"

    md += f"""
---

## 四、近{n}日净值统计

| 统计指标 | 数值 |
|----------|------|
"""
    for key, val in stats.items():
        if "%" in key or "涨跌" in key:
            md += f"| {key} | {_fmt(val)}% |\n"
        elif "天数" in key:
            md += f"| {key} | {val} 天 |\n"
        else:
            md += f"| {key} | {_fmt(val)} |\n"

    md += f"""
---

## 五、近{n}日历史净值明细

| 净值日期 | 单位净值 | 累计净值 | 日涨跌幅(%) | 申购状态 | 赎回状态 |
|----------|----------|----------|------------|----------|----------|
"""
    if not nav_df.empty:
        display_cols = {
            "净值日期": "净值日期",
            "单位净值": "单位净值",
            "累计净值": "累计净值",
            "日涨跌幅(%)": "日涨跌幅(%)",
            "申购状态": "申购状态",
            "赎回状态": "赎回状态",
        }
        for _, row in nav_df.iloc[::-1].iterrows():  # 最新在前
            md += (
                f"| {row.get('净值日期', '-')} "
                f"| {_fmt(row.get('单位净值'))} "
                f"| {_fmt(row.get('累计净值', '-'))} "
                f"| {_fmt(row.get('日涨跌幅(%)', '-'))}{'%' if row.get('日涨跌幅(%)') not in (None, '-', '') else ''} "
                f"| {row.get('申购状态', '-')} "
                f"| {row.get('赎回状态', '-')} |\n"
            )
    else:
        md += "| - | - | - | - | - | - |\n"

    md += """
---

## 六、风险提示

1. 基金净值数据来源于公开市场数据，T+1 日公布，实时估算仅供参考
2. 基金投资有风险，过往业绩不代表未来表现
3. 本报告不构成任何投资建议

---

*本报告由 akshare / 天天基金 数据接口自动生成*
"""
    return md


# ============================================================
# 内部工具函数
# ============================================================

def _safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _fmt(val, decimals=4) -> str:
    if val is None or val == "" or val == "-":
        return "N/A"
    try:
        v = float(val)
        if np.isnan(v):
            return "N/A"
        return f"{v:.{decimals}f}"
    except (TypeError, ValueError):
        return str(val)


def _find_col(columns, candidates: list):
    for c in candidates:
        if c in columns:
            return c
    return None


def _col_mapper(target_map: dict, columns) -> dict:
    """构建 {原始列名: 目标列名} 的重命名映射"""
    mapping = {}
    for target, candidates in target_map.items():
        for c in candidates:
            if c in columns and c != target:
                mapping[c] = target
                break
    return mapping

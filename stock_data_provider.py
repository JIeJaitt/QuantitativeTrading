"""
统一股票数据接口层 — 多数据源多级回退

数据源优先级:
  1. efinance       (实时，轻量级，基于东方财富)
  2. 东方财富HTTP    (实时，直接请求JSON API，无第三方库依赖)
  3. AKShare        (实时，功能全面，但接口不稳定)
  4. BaoStock       (T+1延时，最后兜底)

确保在任一数据源不可用或请求失败时，自动切换到下一个。
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ============================================================
# 数据源可用性检测
# ============================================================

_EF_AVAILABLE = False
_AK_AVAILABLE = False
_BS_AVAILABLE = False
_HTTP_AVAILABLE = False

try:
    import efinance as ef

    _EF_AVAILABLE = True
except ImportError:
    pass

try:
    import akshare as ak

    _AK_AVAILABLE = True
except ImportError:
    pass

try:
    import baostock as bs

    _BS_AVAILABLE = True
except ImportError:
    pass

try:
    import requests as _requests

    _HTTP_AVAILABLE = True
except ImportError:
    pass


def check_data_sources():
    """检查数据源安装情况 + 实际连通性测试，不通的源直接标记跳过"""
    print("=" * 50)

    proxies = _get_proxies()
    if proxies:
        proxy_addr = proxies.get("http") or proxies.get("https")
        print(f"代理: {proxy_addr}")
    else:
        print("代理: 未配置（如需绕过公司网络，请设置 PROXY_URL 或环境变量）")

    lib_info = [
        (_EF_AVAILABLE, "efinance", "pip install efinance"),
        (_HTTP_AVAILABLE, "东方财富HTTP", "pip install requests"),
        (_AK_AVAILABLE, "AKShare", "pip install akshare"),
        (_BS_AVAILABLE, "BaoStock", "pip install baostock"),
    ]
    installed = sum(1 for a, *_ in lib_info if a)
    if installed == 0:
        raise RuntimeError(
            "没有可用的数据源！请至少安装一个:\n"
            "  pip install efinance     (推荐)\n"
            "  pip install akshare\n"
            "  pip install baostock"
        )

    # --- 连通性测试 ---
    print("\n连通性检测:")

    # 1. 东方财富 API（efinance/东方财富HTTP/AKShare 都依赖它）
    em_ok = False
    if _HTTP_AVAILABLE:
        print("  测试东方财富API...", end="", flush=True)
        try:
            data = _em_get(
                "http://push2.eastmoney.com/api/qt/stock/get?"
                "secid=1.600519&fields=f57,f58"
                "&ut=fa5fd1943c7b386f172d6893dbfba10b",
                timeout=8,
            )
            em_ok = bool(data.get("data"))
        except Exception:
            pass

        if em_ok:
            print(" OK")
        else:
            print(" 不通")

    if em_ok:
        # 东方财富通了，逐个确认各库
        if _EF_AVAILABLE:
            print("  [OK] efinance      — 可用")
        else:
            print("  [--] efinance      — 未安装")
        print("  [OK] 东方财富HTTP  — 可用")
        if _AK_AVAILABLE:
            print("  [OK] AKShare       — 可用")
        else:
            print("  [--] AKShare       — 未安装")
    else:
        # 东方财富不通，全部标记跳过
        if _EF_AVAILABLE:
            _DISABLED_SOURCES.add("efinance")
            print("  [!!] efinance      — 跳过（东方财富API不通）")
        else:
            print("  [--] efinance      — 未安装")
        _DISABLED_SOURCES.add("东方财富HTTP")
        print("  [!!] 东方财富HTTP  — 跳过（东方财富API不通）")
        if _AK_AVAILABLE:
            _DISABLED_SOURCES.update({"AKShare", "AKShare-ETF"})
            print("  [!!] AKShare       — 跳过（东方财富API不通）")
        else:
            print("  [--] AKShare       — 未安装")

    # 2. BaoStock
    if _BS_AVAILABLE:
        print("  测试BaoStock...", end="", flush=True)
        if _BSSession.ensure_login():
            print(" OK")
            print("  [OK] BaoStock      — 可用（T+1延时）")
        else:
            print(" 不通")
            _DISABLED_SOURCES.add("BaoStock")
            print("  [!!] BaoStock      — 跳过（登录失败）")
    else:
        print("  [--] BaoStock      — 未安装")

    available = installed - len(
        {
            s
            for s in _DISABLED_SOURCES
            if s in {"efinance", "东方财富HTTP", "AKShare", "BaoStock"}
        }
    )
    print(
        f"\n可用: {available}  跳过: {installed - available}  未安装: {4 - installed}"
    )
    if available == 0:
        print("  [警告] 没有任何数据源可用！请检查网络或代理配置")
    print("=" * 50)
    print()


# ============================================================
# BaoStock 会话管理（懒加载）
# ============================================================


class _BSSession:
    _logged_in = False

    @classmethod
    def ensure_login(cls) -> bool:
        if not _BS_AVAILABLE:
            return False
        if cls._logged_in:
            return True
        try:
            lg = bs.login()
            if lg.error_code == "0":
                cls._logged_in = True
                return True
            print(f"  [BaoStock] 登录失败: {lg.error_msg}")
        except Exception as e:
            print(f"  [BaoStock] 登录异常: {e}")
        return False

    @classmethod
    def logout(cls):
        if cls._logged_in and _BS_AVAILABLE:
            try:
                bs.logout()
            except Exception:
                pass
            cls._logged_in = False


# ============================================================
# 数据源追踪
# ============================================================

_source_tracker: dict[str, str] = {}
_DISABLED_SOURCES: set[str] = set()


def get_data_source_summary() -> str:
    if not _source_tracker:
        return "未知"
    sources = set(_source_tracker.values())
    if len(sources) == 1:
        return list(sources)[0]
    details = ", ".join(f"{k}→{v}" for k, v in _source_tracker.items())
    return f"混合 ({details})"


# ============================================================
# 通用工具
# ============================================================


def is_etf(stock_code: str) -> bool:
    prefixes = ("15", "16", "51", "56", "58", "588")
    return stock_code.startswith(prefixes)


def _bs_code(code: str) -> str:
    """转换为 BaoStock 格式 (sh./sz.)"""
    return f"sh.{code}" if code.startswith(("6", "5", "9")) else f"sz.{code}"


def _em_market(code: str) -> int:
    """东方财富市场代码: 0=深圳, 1=上海"""
    return 1 if code.startswith(("6", "5", "9")) else 0


_NUM_COLS = [
    "开盘",
    "收盘",
    "最高",
    "最低",
    "成交量",
    "成交额",
    "换手率",
    "涨跌幅",
    "振幅",
]
_HTTP_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0"
    )
}

# ============================================================
# 代理配置
# 如果公司网络屏蔽了东方财富等数据接口，可以在这里配置本地代理绕过。
# 支持 HTTP/HTTPS/SOCKS5 代理，格式示例:
#   "http://127.0.0.1:7890"         — Clash/V2Ray 默认HTTP代理
#   "socks5://127.0.0.1:1080"       — SOCKS5 代理
#   ""                              — 不使用代理（留空）
#
# 也可以通过环境变量设置（优先级更高）:
#   set HTTP_PROXY=http://127.0.0.1:7890
#   set HTTPS_PROXY=http://127.0.0.1:7890
# ============================================================
PROXY_URL = "http://127.0.0.1:7897"  # <-- 在这里填写你的代理地址，留空则不使用代理


def _get_proxies() -> dict | None:
    """获取代理配置，环境变量优先于代码中的 PROXY_URL"""
    import os

    env_http = os.environ.get("HTTP_PROXY", os.environ.get("http_proxy", ""))
    env_https = os.environ.get("HTTPS_PROXY", os.environ.get("https_proxy", ""))

    if env_http or env_https:
        return {
            "http": env_http or env_https,
            "https": env_https or env_http,
        }
    if PROXY_URL:
        return {"http": PROXY_URL, "https": PROXY_URL}
    return None


def _apply_proxy_to_env():
    """
    将代理配置写入环境变量，使 akshare、efinance 等第三方库也走代理。
    requests 库会自动读取 HTTP_PROXY / HTTPS_PROXY 环境变量。
    """
    import os

    # 如果环境变量已经设置了，不覆盖
    if os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY"):
        return
    if PROXY_URL:
        os.environ["HTTP_PROXY"] = PROXY_URL
        os.environ["HTTPS_PROXY"] = PROXY_URL
        os.environ["http_proxy"] = PROXY_URL
        os.environ["https_proxy"] = PROXY_URL


# 模块加载时立即应用，确保所有第三方库都走代理
_apply_proxy_to_env()


def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in _NUM_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _is_valid(result) -> bool:
    if result is None:
        return False
    if isinstance(result, pd.DataFrame):
        return not result.empty
    if isinstance(result, dict):
        return bool(result)
    return True


def _try_sources(label, sources, default=None):
    """
    按优先级尝试多个数据源，返回第一个成功的结果。
    跳过连通性测试中已标记为不可用的数据源。
    sources: [(source_name, callable), ...]
    """
    for i, (name, func) in enumerate(sources):
        if name in _DISABLED_SOURCES:
            continue
        try:
            result = func()
            if _is_valid(result):
                _source_tracker[label] = name
                if i > 0:
                    suffix = "（数据延时T+1）" if name == "BaoStock" else ""
                    print(f"  [回退] {label} → {name}{suffix}")
                return result
        except Exception as e:
            err_msg = str(e)
            if not err_msg or err_msg == "None":
                err_msg = type(e).__name__
            print(f"  [{name}] {label}失败: {err_msg}")
    print(f"  [错误] 所有数据源均无法获取{label}")
    return default if default is not None else pd.DataFrame()


def _em_val(d: dict, key: str):
    """安全提取东方财富API字段值，过滤无效数据"""
    val = d.get(key)
    if val is None or val == "-" or val == "":
        return "N/A"
    return val


def _em_get(url: str, timeout: int = 10) -> dict:
    """
    请求东方财富API，自动处理代理、HTTPS/HTTP回退和SSL问题。
    尝试顺序:
      1. http://  + 代理（如有）
      2. http://  无代理
      3. https:// + verify=False + 代理（如有）
    全部失败时抛出异常，便于上层看到具体原因。
    """
    proxies = _get_proxies()
    http_url = url.replace("https://", "http://")
    errors = []

    # 尝试 HTTP + 代理
    try:
        resp = _requests.get(
            http_url,
            headers=_HTTP_UA,
            timeout=timeout,
            proxies=proxies,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        tag = "HTTP+代理" if proxies else "HTTP"
        errors.append(f"{tag}:{type(e).__name__}")

    # 如果配了代理但失败了，再试不带代理的
    if proxies:
        try:
            resp = _requests.get(http_url, headers=_HTTP_UA, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            errors.append(f"HTTP无代理:{type(e).__name__}")

    # 回退: HTTPS + 跳过证书验证
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        https_url = url.replace("http://", "https://")
        resp = _requests.get(
            https_url,
            headers=_HTTP_UA,
            timeout=timeout,
            verify=False,
            proxies=proxies,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        errors.append(f"HTTPS:{type(e).__name__}")

    raise ConnectionError(f"东方财富API连接全部失败 [{', '.join(errors)}]")


# ============================================================
# efinance 数据获取
# ============================================================


def _ef_stock_info(code):
    if not _EF_AVAILABLE:
        return None
    rt = _ef_realtime(code)
    if not rt:
        return None
    return {
        "股票简称": rt.get("名称", code),
        "股票代码": code,
        "总市值": rt.get("总市值", "N/A"),
        "流通市值": rt.get("流通市值", "N/A"),
    }


def _ef_history(code, days):
    if not _EF_AVAILABLE:
        return None
    end = datetime.now().strftime("%Y%m%d")
    beg = (datetime.now() - timedelta(days=days + 30)).strftime("%Y%m%d")
    # ETF 和股票使用相同的 stock 接口获取历史K线
    df = ef.stock.get_quote_history(code, beg=beg, end=end, klt=101, fqt=1)
    if df is None or df.empty:
        return None
    df = _ensure_numeric(df)
    return df.tail(days)


_EF_KEY_MAP = {
    "动态市盈率": "市盈率-动态",
    "股票代码": "代码",
    "股票名称": "名称",
    "基金代码": "代码",
    "基金名称": "名称",
}


def _ef_realtime(code):
    """efinance 实时行情：股票用 stock 模块，ETF 用 fund 模块"""
    if not _EF_AVAILABLE:
        return None

    if is_etf(code):
        # ETF: 先尝试 fund 模块
        try:
            df = ef.fund.get_realtime_quotes()
            if df is not None and not df.empty:
                col = "基金代码" if "基金代码" in df.columns else "代码"
                match = df[df[col] == code]
                if not match.empty:
                    d = match.iloc[0].to_dict()
                    return {_EF_KEY_MAP.get(k, k): v for k, v in d.items()}
        except Exception:
            pass
        # ETF 回退：尝试 stock 模块
        try:
            df = ef.stock.get_realtime_quotes([code])
            if df is not None and not df.empty:
                d = df.iloc[0].to_dict()
                return {_EF_KEY_MAP.get(k, k): v for k, v in d.items()}
        except Exception:
            pass
        return None

    # 普通股票
    df = ef.stock.get_realtime_quotes([code])
    if df is None or df.empty:
        return None
    d = df.iloc[0].to_dict()
    return {_EF_KEY_MAP.get(k, k): v for k, v in d.items()}


# ============================================================
# 东方财富 HTTP 直连
# ============================================================


def _em_stock_info(code):
    """从实时行情中提取基本信息"""
    rt = _em_realtime(code)
    if not rt:
        return None
    return {
        "股票简称": rt.get("名称", code),
        "股票代码": code,
        "总市值": rt.get("总市值", "N/A"),
        "流通市值": rt.get("流通市值", "N/A"),
    }


def _em_history(code, days):
    """东方财富 HTTP — 历史日K线"""
    if not _HTTP_AVAILABLE:
        return None
    market = _em_market(code)
    end = datetime.now().strftime("%Y%m%d")
    beg = (datetime.now() - timedelta(days=days + 30)).strftime("%Y%m%d")

    url = (
        f"http://push2his.eastmoney.com/api/qt/stock/kline/get?"
        f"secid={market}.{code}"
        f"&fields1=f1,f2,f3,f4,f5,f6"
        f"&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&klt=101&fqt=1&beg={beg}&end={end}"
        f"&ut=fa5fd1943c7b386f172d6893dbfba10b"
    )

    data = _em_get(url)

    if not data.get("data") or not data["data"].get("klines"):
        return None

    records = []
    for line in data["data"]["klines"]:
        p = line.split(",")
        if len(p) < 11:
            continue
        try:
            records.append(
                {
                    "日期": p[0],
                    "开盘": float(p[1]),
                    "收盘": float(p[2]),
                    "最高": float(p[3]),
                    "最低": float(p[4]),
                    "成交量": float(p[5]),
                    "成交额": float(p[6]),
                    "振幅": float(p[7]),
                    "涨跌幅": float(p[8]),
                    "涨跌额": float(p[9]),
                    "换手率": float(p[10]),
                }
            )
        except (ValueError, IndexError):
            continue

    if not records:
        return None
    return pd.DataFrame(records).tail(days)


def _em_realtime(code):
    """东方财富 HTTP — 实时行情（单只）"""
    if not _HTTP_AVAILABLE:
        return None
    market = _em_market(code)

    url = (
        f"http://push2.eastmoney.com/api/qt/stock/get?"
        f"secid={market}.{code}"
        f"&fields=f18,f43,f44,f45,f46,f47,f48,f50,f57,f58,"
        f"f116,f117,f162,f167,f168,f170,f171"
        f"&ut=fa5fd1943c7b386f172d6893dbfba10b"
        f"&fltt=2&invt=2"
    )

    data = _em_get(url)

    if not data.get("data"):
        return None

    d = data["data"]
    price = _em_val(d, "f43")
    if price == "N/A":
        return None

    high = _em_val(d, "f44")
    low = _em_val(d, "f45")
    prev_close = _em_val(d, "f18")

    amplitude = "N/A"
    if (
        isinstance(prev_close, (int, float))
        and isinstance(high, (int, float))
        and isinstance(low, (int, float))
        and prev_close > 0
    ):
        amplitude = round((high - low) / prev_close * 100, 2)

    return {
        "代码": _em_val(d, "f57"),
        "名称": _em_val(d, "f58"),
        "最新价": price,
        "最高": high,
        "最低": low,
        "今开": _em_val(d, "f46"),
        "成交量": _em_val(d, "f47"),
        "成交额": _em_val(d, "f48"),
        "量比": _em_val(d, "f50"),
        "换手率": _em_val(d, "f168"),
        "涨跌幅": _em_val(d, "f170"),
        "涨跌额": _em_val(d, "f171"),
        "振幅": amplitude,
        "市盈率-动态": _em_val(d, "f162"),
        "市净率": _em_val(d, "f167"),
        "总市值": _em_val(d, "f116"),
        "流通市值": _em_val(d, "f117"),
    }


# ============================================================
# AKShare 数据获取
# ============================================================


def _ak_stock_info(code):
    if not _AK_AVAILABLE:
        return None
    if is_etf(code):
        df = ak.fund_etf_spot_em()
        if df is not None and not df.empty:
            match = df[df["代码"] == code]
            if not match.empty:
                row = match.iloc[0]
                return {
                    "股票简称": row.get("名称", code),
                    "股票代码": code,
                    "上市时间": "N/A",
                    "股票类型": "ETF",
                }
        return None
    info_df = ak.stock_individual_info_em(symbol=code)
    if info_df is not None and not info_df.empty:
        return dict(zip(info_df["item"], info_df["value"]))
    return None


def _ak_history(code, days):
    if not _AK_AVAILABLE:
        return None
    end = datetime.now().strftime("%Y%m%d")
    beg = (datetime.now() - timedelta(days=days + 30)).strftime("%Y%m%d")
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=beg,
        end_date=end,
        adjust="qfq",
    )
    if df is None or df.empty:
        return None
    df = _ensure_numeric(df)
    return df.tail(days)


def _ak_etf_history(code, days):
    if not _AK_AVAILABLE:
        return None
    end = datetime.now().strftime("%Y%m%d")
    beg = (datetime.now() - timedelta(days=days + 30)).strftime("%Y%m%d")
    df = ak.fund_etf_hist_em(
        symbol=code,
        period="daily",
        start_date=beg,
        end_date=end,
        adjust="qfq",
    )
    if df is None or df.empty:
        return None
    df = _ensure_numeric(df)
    return df.tail(days)


def _ak_realtime(code):
    if not _AK_AVAILABLE:
        return None
    df = ak.stock_zh_a_spot_em()
    if df is None or df.empty:
        return None
    match = df[df["代码"] == code]
    if match.empty:
        return None
    return match.iloc[0].to_dict()


def _ak_etf_realtime(code):
    if not _AK_AVAILABLE:
        return None
    df = ak.fund_etf_spot_em()
    if df is None or df.empty:
        return None
    match = df[df["代码"] == code]
    if match.empty:
        return None
    return match.iloc[0].to_dict()


def _ak_financial(code):
    if not _AK_AVAILABLE:
        return None
    df = ak.stock_financial_abstract_ths(symbol=code, indicator="按报告期")
    if df is None or df.empty:
        return None
    return df.head(4)


# ============================================================
# BaoStock 数据获取
# ============================================================


def _bs_stock_info(code):
    if not (_BS_AVAILABLE and _BSSession.ensure_login()):
        return None
    rs = bs.query_stock_basic(code=_bs_code(code))
    if rs.error_code != "0":
        return None
    rows = []
    while (rs.error_code == "0") & rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return None
    row = rows[0]
    return {
        "股票代码": code,
        "股票简称": row[1] if len(row) > 1 else code,
        "上市时间": row[2] if len(row) > 2 else "N/A",
        "股票类型": row[4] if len(row) > 4 else "N/A",
    }


def _bs_history(code, days):
    if not (_BS_AVAILABLE and _BSSession.ensure_login()):
        return None
    start = (datetime.now() - timedelta(days=days + 30)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        _bs_code(code),
        "date,open,high,low,close,volume,amount,turn,pctChg",
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="2",
    )
    if rs.error_code != "0":
        return None
    rows = []
    while (rs.error_code == "0") & rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=rs.fields)
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
    df = _ensure_numeric(df)
    df["振幅"] = ((df["最高"] - df["最低"]) / df["最低"] * 100).round(2)
    return df.tail(days)


def _bs_realtime(code):
    if not (_BS_AVAILABLE and _BSSession.ensure_login()):
        return None
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        _bs_code(code),
        "date,open,high,low,close,volume,amount,turn,pctChg",
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="2",
    )
    if rs.error_code != "0":
        return None
    rows = []
    while (rs.error_code == "0") & rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return None

    latest = pd.DataFrame(rows, columns=rs.fields).iloc[-1]

    close_v = float(latest["close"]) if latest["close"] else 0
    pct_v = float(latest["pctChg"]) if latest["pctChg"] else 0
    high_v = float(latest["high"]) if latest["high"] else 0
    low_v = float(latest["low"]) if latest["low"] else 0

    result = {
        "代码": code,
        "最新价": close_v,
        "涨跌幅": pct_v,
        "涨跌额": round(close_v * pct_v / 100, 4) if close_v and pct_v else 0,
        "今开": float(latest["open"]) if latest["open"] else "N/A",
        "最高": high_v,
        "最低": low_v,
        "成交量": float(latest["volume"]) if latest["volume"] else "N/A",
        "成交额": float(latest["amount"]) if latest["amount"] else "N/A",
        "换手率": float(latest["turn"]) if latest["turn"] else "N/A",
        "振幅": round((high_v - low_v) / low_v * 100, 2) if low_v > 0 else "N/A",
    }

    # 估值指标
    try:
        trade_date = latest["date"]
        rs_val = bs.query_history_k_data_plus(
            _bs_code(code),
            "date,peTTM,pbMRQ",
            start_date=trade_date,
            end_date=trade_date,
            frequency="d",
            adjustflag="3",
        )
        if rs_val.error_code == "0":
            val_rows = []
            while (rs_val.error_code == "0") & rs_val.next():
                val_rows.append(rs_val.get_row_data())
            if val_rows:
                v = val_rows[0]
                result["市盈率-动态"] = v[1] if len(v) > 1 and v[1] else "N/A"
                result["市净率"] = v[2] if len(v) > 2 and v[2] else "N/A"
    except Exception:
        pass

    return result


def _bs_financial(code):
    if not (_BS_AVAILABLE and _BSSession.ensure_login()):
        return None
    bsc = _bs_code(code)
    today = datetime.now()
    year = today.year
    quarter = (today.month - 1) // 3 + 1

    all_data = []
    fields = None
    for i in range(4):
        q = quarter - i
        y = year
        while q <= 0:
            q += 4
            y -= 1
        rs = bs.query_profit_data(code=bsc, year=y, quarter=q)
        if rs.error_code == "0":
            if fields is None:
                fields = rs.fields
            while (rs.error_code == "0") & rs.next():
                row = rs.get_row_data()
                if row:
                    all_data.append(row)
                    break

    if not all_data or not fields:
        return None
    return pd.DataFrame(all_data, columns=fields).head(4)


# ============================================================
# 主 API 函数（多级回退）
# ============================================================


def get_stock_info(stock_code: str) -> dict:
    return _try_sources(
        "基本信息",
        [
            ("efinance", lambda: _ef_stock_info(stock_code)),
            ("东方财富HTTP", lambda: _em_stock_info(stock_code)),
            ("AKShare", lambda: _ak_stock_info(stock_code)),
            ("BaoStock", lambda: _bs_stock_info(stock_code)),
        ],
        default={"股票代码": stock_code, "股票简称": stock_code},
    )


def get_stock_history(stock_code: str, days: int = 30) -> pd.DataFrame:
    sources = [
        ("efinance", lambda: _ef_history(stock_code, days)),
        ("东方财富HTTP", lambda: _em_history(stock_code, days)),
        ("AKShare", lambda: _ak_history(stock_code, days)),
    ]
    if is_etf(stock_code):
        sources.append(("AKShare-ETF", lambda: _ak_etf_history(stock_code, days)))
    sources.append(("BaoStock", lambda: _bs_history(stock_code, days)))
    return _try_sources("历史行情", sources)


def get_etf_history(stock_code: str, days: int = 30) -> pd.DataFrame:
    return get_stock_history(stock_code, days)


def get_realtime_quote(stock_code: str) -> dict:
    return _try_sources(
        "实时行情",
        [
            ("efinance", lambda: _ef_realtime(stock_code)),
            ("东方财富HTTP", lambda: _em_realtime(stock_code)),
            ("AKShare", lambda: _ak_realtime(stock_code)),
            ("BaoStock", lambda: _bs_realtime(stock_code)),
        ],
        default={},
    )


def get_etf_realtime_quote(stock_code: str) -> dict:
    return _try_sources(
        "实时行情",
        [
            ("efinance", lambda: _ef_realtime(stock_code)),
            ("东方财富HTTP", lambda: _em_realtime(stock_code)),
            ("AKShare-ETF", lambda: _ak_etf_realtime(stock_code)),
            ("AKShare", lambda: _ak_realtime(stock_code)),
            ("BaoStock", lambda: _bs_realtime(stock_code)),
        ],
        default={},
    )


def get_financial_indicators(stock_code: str) -> pd.DataFrame:
    if is_etf(stock_code):
        return pd.DataFrame()
    return _try_sources(
        "财务指标",
        [
            ("AKShare", lambda: _ak_financial(stock_code)),
            ("BaoStock", lambda: _bs_financial(stock_code)),
        ],
    )


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


def generate_markdown(
    stock_code: str,
    stock_info: dict,
    realtime: dict,
    history_df: pd.DataFrame,
    stats: dict,
    financial_df: pd.DataFrame,
    indicators: dict = None,
) -> str:
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stock_name = stock_info.get("股票简称", realtime.get("名称", stock_code))
    source = get_data_source_summary()
    n = len(history_df)

    pe = realtime.get(
        "市盈率-动态",
        realtime.get("动态市盈率", realtime.get("市盈率TTM", "N/A")),
    )
    pb = realtime.get("市净率", "N/A")

    md = f"""# {stock_name}（{stock_code}）基本面分析报告

> 报告生成时间：{report_date}
> 数据来源：{source}

---

## 一、股票基本信息

| 项目 | 内容 |
|------|------|
| 股票代码 | {stock_code} |
| 股票名称 | {stock_name} |
| 所属行业 | {stock_info.get('行业', stock_info.get('所属行业', 'N/A'))} |
| 上市时间 | {stock_info.get('上市时间', 'N/A')} |
| 总市值 | {stock_info.get('总市值', 'N/A')} |
| 流通市值 | {stock_info.get('流通市值', 'N/A')} |

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
| 换手率 | {realtime.get('换手率', 'N/A')}% |
| 振幅 | {format_number(realtime.get('振幅'))}% |
| 市盈率 | {pe} |
| 市净率 | {pb} |

---

## 三、{n}日行情统计

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

## 四、技术指标分析

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

    md += f"""
---

## 五、{n}日历史行情明细

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

## 六、财务指标（最近4个报告期）

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

## 七、风险提示

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
    _BSSession.logout()

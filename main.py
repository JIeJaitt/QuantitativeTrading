import argparse
import pandas as pd

from stock_data_provider import (
    check_data_sources,
    get_stock_info,
    get_etf_realtime_quote,
    get_realtime_quote,
    get_etf_history,
    get_stock_history,
    get_intraday_data,
    get_multi_period_ma,
    get_stock_extra_data,
    get_stock_news,
    get_stock_boards,
    calculate_statistics,
    calculate_technical_indicators,
    generate_markdown,
    is_etf,
    get_financial_indicators,
    cleanup,
)
from fund_data_provider import (
    get_fund_realtime_estimate,
    get_fund_nav_history,
    get_fund_performance,
    calculate_fund_statistics,
    generate_fund_markdown,
)


def run_stock_report(stock_code: str, days: int, custom_output: str):
    """生成股票/ETF分析报告"""
    check_data_sources()

    try:
        is_etf_code = is_etf(stock_code)
        security_type = "ETF" if is_etf_code else "股票"

        print(f"正在获取{security_type} {stock_code} 的数据...")

        print("  - 获取基本信息...")
        stock_info = get_stock_info(stock_code)

        print("  - 获取实时行情...")
        if is_etf_code:
            realtime = get_etf_realtime_quote(stock_code)
        else:
            realtime = get_realtime_quote(stock_code)

        print(f"  - 获取最近{days}日历史行情...")
        if is_etf_code:
            history_df = get_etf_history(stock_code, days)
        else:
            history_df = get_stock_history(stock_code, days)

        print("  - 获取今日分时数据...")
        intraday_df = get_intraday_data(stock_code)

        print("  - 计算多周期均线 (日K/周K/月K)...")
        multi_ma = get_multi_period_ma(stock_code)

        print("  - 获取市值/估值/股本数据...")
        extra_data = get_stock_extra_data(stock_code)

        print("  - 计算统计指标...")
        stats = calculate_statistics(history_df)

        print("  - 计算技术指标 (量比/MACD/RSI/KDJ)...")
        indicators = calculate_technical_indicators(history_df)

        print("  - 获取财务指标...")
        if is_etf_code:
            financial_df = pd.DataFrame()
            print("    (ETF无财务指标)")
        else:
            financial_df = get_financial_indicators(stock_code)

        print("  - 获取行业/概念板块...")
        boards = get_stock_boards(stock_code)

        print("  - 获取相关资讯...")
        news = get_stock_news(stock_code)

        if custom_output:
            output_file = custom_output
        else:
            stock_name = stock_info.get("股票简称", realtime.get("名称", stock_code))
            safe_name = stock_name.replace("/", "_").replace("\\", "_")
            output_file = f"{safe_name}({stock_code})_report.md"

        print("正在生成Markdown报告...")
        markdown_content = generate_markdown(
            stock_code,
            stock_info,
            realtime,
            history_df,
            stats,
            financial_df,
            indicators,
            intraday_df,
            multi_ma,
            extra_data,
            news,
            boards,
        )

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(markdown_content)

        print(f"报告已生成：{output_file}")
        print("\n" + "=" * 60)
        print(markdown_content)

    finally:
        cleanup()
        print("\n资源已清理")


def run_fund_report(fund_code: str, days: int, custom_output: str):
    """生成场外公募基金净值分析报告"""
    print(f"正在获取基金 {fund_code} 的数据...")

    print("  - 获取实时估算净值...")
    estimate = get_fund_realtime_estimate(fund_code)

    print(f"  - 获取近{days}日历史净值（akshare）...")
    nav_df = get_fund_nav_history(fund_code, days)

    print("  - 获取业绩表现...")
    performance = get_fund_performance(fund_code)

    print("  - 计算净值统计指标...")
    stats = calculate_fund_statistics(nav_df)

    fund_name = estimate.get("基金名称", fund_code)

    if custom_output:
        output_file = custom_output
    else:
        safe_name = fund_name.replace("/", "_").replace("\\", "_")
        output_file = f"{safe_name}({fund_code})_fund_report.md"

    print("正在生成基金报告...")
    markdown_content = generate_fund_markdown(
        fund_code,
        estimate,
        nav_df,
        performance,
        stats,
    )

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"报告已生成：{output_file}")
    print("\n" + "=" * 60)
    print(markdown_content)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="获取股票/ETF/基金数据并生成Markdown报告")
    parser.add_argument("code", type=str, help="股票/ETF/基金代码，如 000001、600519、159530")
    parser.add_argument(
        "--type",
        "-t",
        choices=["auto", "stock", "etf", "fund"],
        default="auto",
        help="证券类型: auto(自动检测) / stock(股票) / etf / fund(场外基金)，默认auto",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="输出文件名，默认自动生成",
    )
    parser.add_argument(
        "--days", "-d", type=int, default=50, help="获取历史天数，默认50天"
    )

    args = parser.parse_args()
    code = args.code
    sec_type = args.type
    custom_output = args.output
    days = args.days

    # 自动判断类型
    if sec_type == "auto":
        if is_etf(code):
            sec_type = "etf"
        elif code.startswith(("6", "0", "3", "9", "4", "2", "8")):
            # 粗判：以常见股票前缀开头且不是ETF前缀，当作股票处理
            # 若用户明确传入基金代码请用 --type fund
            sec_type = "stock"
        else:
            sec_type = "stock"

    if sec_type == "fund":
        run_fund_report(code, days, custom_output)
    else:
        run_stock_report(code, days, custom_output)


if __name__ == "__main__":
    main()

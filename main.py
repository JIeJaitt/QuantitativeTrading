import argparse
import pandas as pd
import baostock as bs

from stock_fundamentals_baostock import (
    get_stock_info,
    get_etf_realtime_quote,
    get_realtime_quote,
    get_etf_history,
    get_stock_history,
    calculate_statistics,
    calculate_technical_indicators,
    generate_markdown,
    is_etf,
    get_financial_indicators,
)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="获取股票基本面数据并生成Markdown报告")
    parser.add_argument("stock_code", type=str, help="股票代码，如 000001 或 600519")
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="输出文件名，默认为 {股票代码}_report.md",
    )
    parser.add_argument(
        "--days", "-d", type=int, default=30, help="获取的历史天数，默认30天"
    )

    args = parser.parse_args()

    stock_code = args.stock_code
    output_file = args.output or f"{stock_code}_report.md"
    days = args.days

    # 登录 BaoStock
    print("正在连接 BaoStock...")
    lg = bs.login()
    if lg.error_code != "0":
        print(f"BaoStock 登录失败: {lg.error_msg}")
        return

    try:
        # 判断是ETF还是股票
        is_etf_code = is_etf(stock_code)
        security_type = "ETF" if is_etf_code else "股票"

        print(f"正在获取{security_type} {stock_code} 的数据...")

        # 获取各类数据
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

        print("  - 计算统计指标...")
        stats = calculate_statistics(history_df)

        print("  - 计算技术指标 (量比/MACD/RSI/KDJ)...")
        indicators = calculate_technical_indicators(history_df)

        print("  - 获取财务指标...")
        if is_etf_code:
            # ETF没有财务指标
            financial_df = pd.DataFrame()
            print("    (ETF无财务指标)")
        else:
            financial_df = get_financial_indicators(stock_code)

        # 生成报告
        print("正在生成Markdown报告...")
        markdown_content = generate_markdown(
            stock_code,
            stock_info,
            realtime,
            history_df,
            stats,
            financial_df,
            indicators,
        )

        # 保存文件
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(markdown_content)

        print(f"报告已生成：{output_file}")

        # 同时输出到控制台
        print("\n" + "=" * 60)
        print(markdown_content)

    finally:
        # 登出 BaoStock
        bs.logout()
        print("\n已断开 BaoStock 连接")


if __name__ == "__main__":
    main()

from fund_data_provider import get_fund_nav_history

df = get_fund_nav_history("024749", days=90)
print(df)

import datetime
import pandas as pd


def get_stock_data(stock: str) -> pd.DataFrame:
    url = f"https://stooq.pl/q/d/l/?s={stock}&i=d"
    df = pd.read_csv(url)
    df.dropna(inplace=True)
    return df

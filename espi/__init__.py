import requests
import pandas as pd


def get_espi_data(ticker: str) -> pd.DataFrame:
    www = f"https://www.stockwatch.pl/komunikaty-spolek/wszystkie.aspx?page=0&type=&c=&t={ticker}"
    r = requests.get(www)

    df = pd.read_html(r.text, attrs={"id": "msgTable"})[0]
    df.rename({"Data Publikacji": "DataPublikacji", "Tytuł komunikatu": "TytulKomunikatu"}, axis=1, inplace=True)
    return df

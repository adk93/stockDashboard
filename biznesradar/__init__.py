# Standard library imports
from enum import Enum
import re

# Third party imports
import pandas as pd


class REPORT_TYPES(Enum):
    rzis: str = "rachunek-zyskow-i-strat"
    bilans: str = "bilans"
    rpp: str = "przeplywy-pieniezne"


def download_data(report_type: REPORT_TYPES, ticker: str) -> pd.DataFrame:
    www = f"https://www.biznesradar.pl/raporty-finansowe-{report_type.value}/{ticker},Q"

    df = pd.read_html(www, attrs={"class": "report-table"}, header=0)[0]
    df.rename({"Unnamed: 0": "Pozycja"}, axis=1, inplace=True)
    df.drop(df.columns[-1], axis=1, inplace=True)

    df.set_index("Pozycja", inplace=True)
    df = df.fillna("0")

    df = df.applymap(lambda x: re.split(r"k/k|r/r", x)[0])
    df = df.applymap(lambda x: str(x).replace(" ", ""))
    df[df.columns] = df[df.columns].apply(pd.to_numeric, errors="coerce", axis=1)

    return df


def get_data(ticker: str) -> pd.DataFrame:
    df = pd.DataFrame()
    for report_type in REPORT_TYPES:
        df = pd.concat([df, download_data(report_type, ticker)])

    df = df.transpose()
    df.reset_index(inplace=True)
    return df.fillna(0)

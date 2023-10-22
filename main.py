import pandas as pd

import biznesradar
import bigquery_client
import espi
import stock
import re

COMPANIES = {
    "Spyrosoft": {
        "biznesradar": "SPR",
        "espi": "SPYROSOFT",
        "notowania": "spr"
    },
    "TenderHut": {
        "biznesradar": "THG",
        "espi": "TENDERHUT",
        "notowania": "thg"
    },
    "Ailleron": {
        "biznesradar": "ALL",
        "espi": "AILLERON",
        "notowania": "all"
    },
    "Makolab": {
        "biznesradar": "MLB",
        "espi": "MAKOLAB",
        "notowania": "mlb"
    },
    "Comarch": {
        "biznesradar": "CMR",
        "espi": "COMARCH",
        "notowania": "cmr"
    },
    "Asseco Poland": {
        "biznesradar": "ACP",
        "espi": "ASSECOPOL",
        "notowania": "acp"
    },
    "Asseco BS": {
        "biznesradar": "ABS",
        "espi": "ASSECOBS",
        "notowania": "abs"
    }
}

# finances_df = pd.DataFrame()
# for company in COMPANIES.keys():
#     df = biznesradar.get_data(COMPANIES[company]['biznesradar'])
#
#     df['NAME'] = company
#
#     print(f"Company {company} has {len(df.columns)} columns which are: {df.columns}")
#     finances_df = pd.concat([finances_df, df])
#
# bigquery_client.save_to_bq_dataset("finances", finances_df)

espi_df = pd.DataFrame()
for company in COMPANIES.keys():
    df = espi.get_espi_data(COMPANIES[company]["espi"])
    df['NAME'] = company

    espi_df = pd.concat([espi_df, df])

bigquery_client.save_to_bq_dataset("espi", espi_df)

stock_df = pd.DataFrame()
for company in COMPANIES.keys():
    df = stock.get_stock_data(COMPANIES[company]['notowania'])
    df['NAME'] = company

    stock_df = pd.concat([stock_df, df])

bigquery_client.save_to_bq_dataset("notowania", stock_df)

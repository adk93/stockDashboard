# Standard library imports
import os
from typing import List, Dict
import time
import json

# Third party imports
import google.api_core.exceptions
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv


load_dotenv()

client = bigquery.Client.from_service_account_info(json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")))

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")


def save_to_bq_dataset(table_name: str, rows_to_insert: pd.DataFrame,
                       project_id: str = None, dataset_id: str = None) -> None:
    """
    Function that saves data to a bigquery table
    :param table_name: table name in bigquery as string
    :param rows_to_insert: rows to append to a table. Should be a list of dicts
    :param project_id: project id in BigQuery
    :param dataset_id: dataset id in BigQuery
    :return: None
    """

    project_id = project_id if project_id is not None else PROJECT_ID
    dataset_id = dataset_id if dataset_id is not None else DATASET_ID

    print(project_id)

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    schema = [bigquery.SchemaField(name, "STRING") for name in rows_to_insert.columns]
    print(schema)


    # Check if the table already exists
    table_ref = client.dataset(dataset_id).table(table_name)

    table = bigquery.Table(table_ref, schema=schema)

    bq = client.query(f"SELECT * FROM {table_id}").to_dataframe()

    print(bq)

    print(rows_to_insert)
    final_df = pd.concat([bq, rows_to_insert]).drop_duplicates()

    print(final_df)
    # Insert only non-duplicate rows
    errors = client.insert_rows_json(table, final_df.dropna().to_dict("records"))

    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print("New rows added")

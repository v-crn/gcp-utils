from typing import Optional

import google.auth
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJob
from google.cloud.exceptions import NotFound

credentials, project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
client = bigquery.Client(credentials=credentials, project=project_id)


def create_table(
    project_id: str,
    dataset_id: str,
    table_name: str,
    schema: Optional[list[bigquery.SchemaField]] = None,
) -> None:
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def read_from_bq_table(query: str) -> QueryJob:
    return client.query(query)


def read_dataframe_from_bq_table(query: str) -> pd.DataFrame:
    return client.query(query).to_dataframe()


def does_table_exist(project_id: str, dataset_id: str, table_name: str) -> bool:
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def upload_dataframe(
    df: pd.DataFrame,
    table_id: str | None,
    project_id: str | None,
    dataset_id: str | None,
    table_name: str | None,
    method: str = "append",
    schema: list[bigquery.SchemaField] | None = None,
    time_format_dict: Optional[dict[str, str]] = None,
    unit_size: int = 10000,
) -> list[str]:
    df = df.where(df.notna(), None)
    table_id = (
        f"{project_id}.{dataset_id}.{table_name}" if table_id is None else table_id
    )
    table = bigquery.Table(table_id, schema=schema)

    len_df = len(df)
    if len_df > unit_size:
        n_split = len_df // unit_size + 1
        df_list = np.array_split(df, n_split)
    else:
        df_list = [df]

    errors = []
    for df_s in df_list:
        if method == "replace":
            if does_table_exist(project_id, dataset_id, table_name):
                client.delete_table(table_id, not_found_ok=True)

            table = client.create_table(table, exists_ok=True)

        for col in df_s.columns:
            if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(df_s[col]):
                if (time_format_dict is None) or (col not in time_format_dict.keys()):
                    time_format_dict = {col: "%Y-%m-%dT%H:%M:%S"}

                if (
                    isinstance(time_format_dict, dict)
                    and col in time_format_dict.keys()
                ):
                    df_s[col] = df_s[col].apply(
                        lambda x: x.strftime(time_format_dict[col])
                    )

        rows = df_s.to_dict(orient="records")
        errors.extend(client.insert_rows_json(table=table, json_rows=rows))

    return errors

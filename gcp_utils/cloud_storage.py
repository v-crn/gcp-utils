import pickle

import cloudpickle
import gcsfs
import pandas as pd
from google.cloud import storage as gcs

client = gcs.Client()


def upload_as_pickle_to_gcs(
    project_id: str,
    df: pd.DataFrame,
    filepath: str,
) -> None:
    print(f"Uploading")
    fs = gcsfs.GCSFileSystem(project=project_id)
    with fs.open(filepath, "wb") as handle:
        pickle.dump(df, handle)


def upload_file(bucket_name: str, gcs_path: str, filepath: str) -> None:
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(filepath)


def upload_dataframe(bucket_name: str, gcs_path: str, df: pd.DataFrame) -> None:
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(df.to_csv(index=False, header=True, sep=","))


def download_as_pickle(bucket_name: str, gcs_path: str) -> str:
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return cloudpickle.loads(blob.download_as_string())

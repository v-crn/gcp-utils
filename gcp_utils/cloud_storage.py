import pickle
from io import BytesIO

import cloudpickle
import gcsfs
import pandas as pd
from google.cloud import storage as gcs

client = gcs.Client()


def read_csv_from_gcs(
    bucket_name: str,
    gcs_path: str,
) -> pd.DataFrame:
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    content = blob.download_as_bytes()
    return pd.read_csv(BytesIO(content))


def download_blob(
    bucket_name: str,
    source_blob_filepath: str,
    destination_filepath: str,
) -> None:
    """
    Downloads a blob (GCS object) from the bucket.
    """
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_filepath)
    blob.download_to_filename(destination_filepath)
    print(f"Blob {source_blob_filepath} downloaded to {destination_filepath}")


def download_blobs(
    bucket_name: str,
    source_blob_dirpath: str,
    destination_dirpath: str,
) -> None:
    """
    Downloads blobs (GCS object path like directory path) from the bucket.
    """
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_blob_dirpath)
    blob_list = list(blobs)
    if len(blob_list) == 0:
        raise ValueError(f"No blobs found in {source_blob_dirpath}")

    for blob in blob_list:
        filename = blob.name.split("/")[-1]
        gcs_filepath = destination_dirpath + "/" + filename
        blob.download_to_filename(gcs_filepath)


def upload_as_pickle_to_gcs(
    project_id: str,
    df: pd.DataFrame,
    filepath: str,
) -> None:
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


def delete_gcs_object(
    bucket_name: str,
    gcs_path: str,
) -> None:
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.delete()

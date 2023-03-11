from pathlib import Path
import pandas as pd
import pandas_gbq
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame,color:str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
        print(df.head(2))
        print(f"columns: {df.dtypes}")
        print(f"rows: {len(df)}")
        return df
    elif color == "green":
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        print(df.head(2))
        print(f"columns: {df.dtypes}")
        print(f"rows: {len(df)}")
        return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcpzoombucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@task()
def extract_from_gcs(color: str,year: int, month:int) -> Path:
    """Download trip data from gcs"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("gcpzoombucket")
    gcs_block.get_directory(from_path = gcs_path, local_path = f"./")
    return Path(f"./{gcs_path}")

@task()
def transform(path: Path)-> pd.DataFrame:
    """Data Cleaning example transformation"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame,color: str) -> None:
    """Write data to BigQuery"""
     
    gcp_credentials_block = GcpCredentials.load("gcpzoombucketcredentials")
    
    df.to_gbq(
        destination_table=f"trips_data_all.{color}_taxi_trips",
        project_id= "luminous-cubist-375311",
        credentials= gcp_credentials_block.get_credentials_from_service_account(), 
        chunksize=500_000,
        if_exists="append"
     )


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df,color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df_clean,color)



@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)

    


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2]
    year = 2021
    etl_parent_flow(months, year, color)
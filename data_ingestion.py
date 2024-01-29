import pandas as pd
import requests
import pyarrow as pa
from argparse import ArgumentParser
from sqlalchemy import create_engine
import logging
import pyarrow.parquet as pq
import os
import yaml
from box import Box

# set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_file_extension(file_name):
    base, extension = os.path.splitext(file_name)
    if extension == '.gz':
        base, extension = os.path.splitext(base)
        extension += '.gz'
    return extension


def stream_and_save_file(url, local_data_file):
    chunk_size_mb = 10
    logger.debug(
        f"Streaming data from {url} to {local_data_file} with chunks of {chunk_size_mb} MBs"
    )
    chunk_size = chunk_size_mb * 2**20
    with requests.get(url, stream=True) as response:
        if response.status_code == 200:
            with open(local_data_file, 'wb') as f:
                for num, chunk in enumerate(response.iter_content(chunk_size=chunk_size)):
                    logger.debug(f"Writing chunk {num}")
                    if chunk:
                        f.write(chunk)


def ingest_parquet_data(db_engine, local_file_path, data_name: str) -> None:
    """Ingest data from local file: write in chunks to database"""
    logger.debug(f"Ingesting data from {local_file_path} to database")
    parquet_file = pq.ParquetFile(local_file_path)
    chunk_size = 10*2**20
    for num, batch in enumerate(parquet_file.iter_batches(batch_size=chunk_size)):
        logger.debug(f"Writing chunk {num}")
        table = pa.Table.from_batches([batch])
        chunk_df = table.to_pandas()
        chunk_df.to_sql(data_name, db_engine, if_exists="append")


def ingest_csv_data(db_engine, local_file_path, data_name: str) -> None:
    """Ingest data from local file: write in chunks to database"""
    logger.debug(f"Ingesting data from {local_file_path} to database")
    chunk_size = 10*2**20
    for num, chunk_df in enumerate(pd.read_csv(local_file_path, chunksize=chunk_size)):
        logger.debug(f"Writing chunk {num}")
        chunk_df.to_sql(data_name, db_engine, if_exists="append")


def ingest_data(db_engine: Engine, local_file_path: str, data_name: str) -> None:
    """Orchestrate data ingestion

    Identifies file type and calls appropriate ingestion function
    """
    if local_file_path.endswith(".parquet"):
        ingest_parquet_data(db_engine, local_file_path, data_name)
    else:
        ingest_csv_data(db_engine, local_file_path, data_name)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--data_url", help="URL to download data from")
    parser.add_argument("--data_name", help="Name of data (like `locations` or `trips`)")
    parser.add_argument("--local_file_path", help="Path to save data to", default="data/")
    return parser.parse_args()


def get_config():
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    return Box(cfg)


if __name__ == "__main__":
    args = parse_args()
    cfg = get_config()
    data_extension = get_file_extension(args.data_url)
    local_data_file = args.local_file_path + args.data_name + data_extension
    # stream_and_save_file(args.data_url, local_data_file)
    uri = f"postgresql://{cfg.pg_user}:{cfg.pg_password}@{cfg.pg_host}:{cfg.pg_port}/{cfg.pg_db}"
    engine = create_engine(uri)
    print(uri)
    # ingest_data(engine, local_data_file, data_name=args.data_name)
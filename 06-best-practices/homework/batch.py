#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pickle
import datetime
from typing import Dict, List

import pandas as pd


def set_options() -> Dict:
    options = {}
    S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")

    if S3_ENDPOINT_URL is not None:
        options["client_kwargs"] = {}
        options["client_kwargs"]["endpoint_url"] = S3_ENDPOINT_URL
    return options


def read_data(filename: str, categorical: List) -> pd.DataFrame:
    df = pd.read_parquet(filename, storage_options=set_options())
    return prepare_data(df, categorical)


def save_data(filename: str, df: pd.DataFrame) -> None:
    df.to_parquet(filename, storage_options=set_options())


def prepare_data(df: pd.DataFrame, categorical: List) -> pd.DataFrame:
    df["duration"] = df.dropOff_datetime - df.pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def get_input_path(year: int, month: int) -> str:
    default_input_pattern = (
        "https://raw.githubusercontent.com/"
        f"alexeygrigorev/datasets/master/nyc-tlc/fhv/fhv_tripdata_{year:04d}-{month:02d}.parquet"
    )
    input_pattern = os.getenv("INPUT_FILE_PATTERN", default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year: int, month: int) -> str:
    default_output_dir = f"data/taxi_type=fhv/year={year:04d}/month={month:02d}"
    default_output_pattern = f"{default_output_dir}/predictions.parquet"
    output_pattern = os.getenv("OUTPUT_FILE_PATTERN", default_output_pattern)

    if output_pattern == default_output_pattern:
        os.makedirs(default_output_dir, exist_ok=True)

    return output_pattern.format(year=year, month=month)


def main(year: int, month: int) -> None:
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    with open("model.bin", "rb") as f_in:
        dv, lr = pickle.load(f_in)

    categorical = ["PUlocationID", "DOlocationID"]
    df = read_data(input_file, categorical)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print("predicted mean duration:", y_pred.mean())

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["predicted_duration"] = y_pred

    save_data(output_file, df_result)


if __name__ == "__main__":

    year = int(sys.argv[1])
    month = int(sys.argv[2])

    try:
        datetime.datetime.strptime(f"{year:04d}-{month:02d}-01", "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Incorrect year or month: {e}") from e

    main(year, month)

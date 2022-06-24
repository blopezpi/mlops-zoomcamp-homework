#!/usr/bin/env python

import pickle
import pandas as pd
import numpy as np
import os
import click
from typing import Dict, Tuple
import boto3
from io import BytesIO




categorical = ['PUlocationID', 'DOlocationID']

@click.command()
@click.option('--year', default=2021, help='The year to get the FHV data.')
@click.option('--month', default=2,
              help='The month to get the FHV data.')
def read_data(year: int, month: int) -> Tuple[pd.DataFrame, int, int]:

    print("Reading dataset...")

    df = pd.read_parquet(f'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{year:04d}-{month:02d}.parquet')

    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return (df, year, month)



def write_results_to_cloud(df: pd.DataFrame, predictions: np.array, year: int, month: int) -> None:
    print("Writing results directly to s3")
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df_result = pd.DataFrame({'ride_id': df['ride_id'].values, 'predictions': predictions})

    bucket = os.getenv("BUCKET_NAME", 'test')

    parquet_buffer = BytesIO()
    df_result.to_parquet(parquet_buffer)
    s3_resource = boto3.resource('s3')

    s3_resource.Object(bucket,
        f'outputs/{year:04d}/{month:02d}/predictions.parquet').put(Body=parquet_buffer.getvalue())



def predict(df: pd.DataFrame, year: int, month: int) -> int:

    print("Predicting results")

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    dicts = df[categorical].to_dict(orient='records')

    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    mean_duration = np.mean(y_pred)

    write_results_to_cloud(df, y_pred, year, month)

    return mean_duration



if __name__ == '__main__':
    df, year, month = read_data(standalone_mode=False)
    print(f'The prediction mean is: {predict(df, year, month)}')

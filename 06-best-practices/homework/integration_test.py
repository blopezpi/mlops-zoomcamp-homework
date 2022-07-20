import sys
import subprocess
from datetime import datetime

import pandas as pd

import batch
from tests.utils import create_dataframe


def save_dataframe(year: int, month: int):
    df_input = create_dataframe()
    input_file = batch.get_input_path(year, month)
    batch.save_data(input_file, df_input)
    subprocess.run(["python", "./batch.py", f"{year}", f"{month}"], check=True)
    return get_sum_predictions(year, month)


def get_sum_predictions(year: int, month: int) -> int:
    options = batch.set_options()
    output_file = batch.get_output_path(year, month)
    return pd.read_parquet(output_file, storage_options=options)[
        "predicted_duration"
    ].sum()


# 69.28

if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    try:
        datetime.strptime(f"{year:04d}-{month:02d}-01", "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Incorrect year or month: {e}") from e

    print(f"SUM PREDICTIONS: {save_dataframe(year, month)}")

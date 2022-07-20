import pandas as pd
import pytest

import batch
from tests.utils import dt, create_dataframe


@pytest.fixture(name="df_test")
def dataframe():
    yield create_dataframe()


def test_prepare_data(df_test):
    """2"""
    columns = [
        "PUlocationID",
        "DOlocationID",
        "pickup_datetime",
        "dropOff_datetime",
        "duration",
    ]
    data = [
        ("-1", "-1", dt(1, 2), dt(1, 10), 8.000000000000002),
        ("1", "1", dt(1, 2), dt(1, 10), 8.000000000000002),
    ]
    df_result = pd.DataFrame(data, columns=columns).to_dict()

    categorical = ["PUlocationID", "DOlocationID"]
    df_batch = batch.prepare_data(df_test, categorical).to_dict()

    assert df_batch == df_result

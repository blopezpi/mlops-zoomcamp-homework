#!/bin/bash

mkdir -p src/data

for month in {1..2}; do
    curl -s -o "src/data/fhv_tripdata_2021-0${month}.parquet" "https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2021-0${month}.parquet"
done


.EXPORT_ALL_VARIABLES:
INPUT_FILE_PATTERN = s3://nyc-duration/in/{year:04d}-{month:02d}.parquet
OUTPUT_FILE_PATTERN = s3://nyc-duration/out/{year:04d}-{month:02d}.parquet
S3_ENDPOINT_URL = http://localhost:4566
AWS_ACCESS_KEY_ID = test
AWS_SECRET_ACCESS_KEY = test
AWS_DEFAULT_REGION = us-east-1


setup:
	pipenv install --dev


quality_checks: setup
	pipenv run isort .
	pipenv run black .
	pipenv run pylint --recursive=y .


unit_test: setup
	pipenv run pytest -vv


integration_test: setup
	docker-compose up -d
	sleep 5
	aws s3 --endpoint-url $${S3_ENDPOINT_URL} mb s3://nyc-duration
	pipenv run python integration_test.py 2021 1
	docker-compose down


clean:
	rm -rf .pytest_cache/


all: setup quality_checks unit_test integration_test clean
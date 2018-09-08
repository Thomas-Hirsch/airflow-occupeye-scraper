#!/usr/bin/env bash
AWS_ACCESS_KEY_ID=$(aws --profile default configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws --profile default configure get aws_secret_access_key)

docker build -t ods .

docker run \
   -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
   -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
      ods python -m unittest discover


docker tag ods robinlinacre/airflow-occupeye-scraper:v10

docker push robinlinacre/airflow-occupeye-scraper

docker run \
   -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
   -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
      robinlinacre/airflow-occupeye-scraper:v10 python main.py --scrape_type=daily --scrape_datetime=2018-01-01T15:00:00+00:00 --next_execution_date=2018-01-01T15:00:00+00:00
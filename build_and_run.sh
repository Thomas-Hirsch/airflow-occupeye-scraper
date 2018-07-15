#!/usr/bin/env bash
AWS_ACCESS_KEY_ID=$(aws --profile default configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws --profile default configure get aws_secret_access_key)

docker build -t ods .

docker tag ods robinlinacre/airflow-occupeye-scraper:v4

docker push robinlinacre/airflow-occupeye-scraper

docker run \
   -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
   -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
      robinlinacre/airflow-occupeye-scraper:v4 python main.py --scrape_type=hourly --scrape_datetime=2018-07-15T17:00:00

import argparse
import json
import datetime
import logging
from dateutil.parser import parse
import sys

from dataengineeringutils import s3
from api_requests import get_surveys_from_api, get_sensors_dimension_from_api, get_survey_facts_from_api
from refresh_partitions import refresh_glue_partitions

from transfer_to_s3 import surveys_to_s3, sensor_dimension_to_s3, survey_fact_to_s3
from time_utils import scrape_date_in_surveydays, next_execution_is_in_future, get_survey_dates
argp = argparse.ArgumentParser(description='Optional app description')

argp.add_argument('--survey_id', type=int, help='the id of the survey you want to rescrape')

args = argp.parse_args()
survey_id = args.survey_id

surveys = get_surveys_from_api()

survey = [s for s in surveys if s["SurveyID"] == survey_id]

if len(survey) == 1:
    survey = survey[0]
else:
    raise ValueError("Survey doesn't exist")

surveydays = get_survey_dates(survey)

for day in surveydays:

    scrape_date_string = datetime.datetime.strftime(day, "%Y-%m-%d")
    print(f"Scraping date {scrape_date_string}")
    survey_fact = get_survey_facts_from_api(survey, scrape_date_string)
    survey_fact_to_s3(survey_fact, survey, scrape_date_string)

refresh_glue_partitions()
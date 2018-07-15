import argparse
import json
import datetime
import logging
from dateutil.parser import parse
import sys
logger = logging.getLogger(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

from dataengineeringutils import s3
from api_requests import get_surveys_from_api, get_sensors_dimension_from_api, get_survey_facts_from_api
from time_utils import num_days_between_scrape_ts_and_now
from transfer_to_s3 import surveys_to_s3, sensor_dimension_to_s3, survey_fact_to_s3
from time_utils import scrape_date_in_surveydays
argp = argparse.ArgumentParser(description='Optional app description')

argp.add_argument('--scrape_type', type=str, help='daily or hourly')
argp.add_argument('--scrape_datetime', type=str, help='Datetime for the hourly scraper')

args = argp.parse_args()

scrape_datetime = parse(args.scrape_datetime)
scrape_date = scrape_datetime.date()
scrape_date_string = scrape_date.isoformat()
scrape_hour = scrape_datetime.hour

# We daily scrape at 6am rather than midnight just to make sure all the data's in the db for the previous day
scrape_date_yesterday = scrape_date - datetime.timedelta(days=1)
scrape_date_string_yesterday = scrape_date_yesterday.isoformat()


surveys = get_surveys_from_api()


if args.scrape_type == 'daily':
    surveys_to_s3(surveys)
    for survey in surveys:

        if scrape_date_in_surveydays(scrape_date_string, survey):
            sensor_dimension = get_sensors_dimension_from_api(survey)
            sensor_dimension_to_s3(sensor_dimension)

            # Need a daily task anyway to refresh the Athena partitions
            if num_days_between_scrape_ts_and_now(scrape_datetime) < 2:
                pass

            survey_fact = get_survey_facts_from_api(survey, scrape_date_string_yesterday)
            survey_fact_to_s3(survey_fact, survey, scrape_date_string)

if args.scrape_type == 'hourly':
    for survey in surveys:
        survey_fact = get_survey_facts_from_api(survey, scrape_date_string)
        survey_fact_to_s3(survey_fact, survey, scrape_date_string)


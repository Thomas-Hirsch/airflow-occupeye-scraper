import argparse
import job_utils
import json
import datetime
import logging
logger = logging.getLogger(__name__)

from dataengineeringutils import s3

parser = argparse.ArgumentParser(description='Optional app description')

parser.add_argument('--scrape_type', type=str, help='Daily or hourly')
parser.add_argument('--scrape_date', type=str, help='Date of day just completed')
parser.add_argument('--scrape_datetime', type=str, help='Datetime for the hourly scraper')

args = parser.parse_args()

scrape_date_string = args.scrape_date
scrape_date = datetime.datetime.strptime(scrape_date_string, '%Y-%m-%d').date()

surveys = get_surveys()
surveys_to_s3(surveys)

for survey in surveys:
    if scrape_date_in_surveydays(scrape_date_string, survey)
        sensor_dimension = get_sensors_dimension_from_api(survey)
        sensor_dimension_to_s3(sensor_dimension)

        # If the execution time is near to the date we're scrape yesterday's data as well to make sure it's complete
        # If the time is before 3am, then scrape yesterday as well

        # Need a daily task anyway to refresh the Athena partitions

        survey_fact = get_survey_facts_from_api(survey, args.scrape_date)
        survey_fact_to_s3(survey_fact, survey)

# Houry:
# surveys = get_surveys()
# for survey in surveys:
#     sensor_fact = get_sensor_facts_from_api(survey, args.scrape_date)
#     sensor_fact_to_s3(sensor_fact)


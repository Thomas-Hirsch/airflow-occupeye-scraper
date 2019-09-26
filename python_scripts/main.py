import argparse
import datetime
import logging
from dateutil.parser import parse
import sys
from api_requests import (
    get_surveys_from_api,
    get_sensors_dimension_from_api,
    get_survey_facts_from_api,
)
from refresh_partitions import refresh_glue_partitions

from transfer_to_s3 import (
    surveys_to_s3,
    sensor_dimension_to_s3,
    survey_fact_to_s3,
)
from time_utils import (
    scrape_date_in_surveydays,
    survey_not_scraped,
)
from rescrape_entire_survey import rescrape_entire_survey

logger = logging.getLogger(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

argp = argparse.ArgumentParser(description="Optional app description")

argp.add_argument("--scrape_type", type=str, help="daily or hourly")
argp.add_argument("--scrape_datetime", type=str, help="Scrape datetime")
argp.add_argument(
    "--next_execution_date", type=str, help="Datetime for next scrape"
)

args = argp.parse_args()

scrape_datetime = parse(args.scrape_datetime)
scrape_date = scrape_datetime.date()
scrape_date_string = scrape_date.isoformat()
scrape_hour = scrape_datetime.hour
utc_next_execution_date = parse(args.next_execution_date)

# We daily scrape at 3am rather than midnight
# just to make sure all the data's in the db for the previous day
scrape_date_yesterday = scrape_date  # scrape_date is already yesterday
scrape_date_string_yesterday = scrape_date_yesterday.isoformat()

surveys = get_surveys_from_api()

if args.scrape_type == "daily":
    surveys_to_s3(surveys)
    for survey in surveys:
        if survey_not_scraped(survey):
            rescrape_entire_survey(survey)
        elif scrape_date_in_surveydays(scrape_date_string, survey):
            sensor_dimension = get_sensors_dimension_from_api(survey)
            sensor_dimension_to_s3(sensor_dimension)

            survey_fact = get_survey_facts_from_api(
                survey, scrape_date_string_yesterday
            )
            survey_fact_to_s3(
                survey_fact, survey, scrape_date_string_yesterday
            )

    # Need a daily task anyway to refresh the Athena partitions
    # On a non backfill day, scrape will execute the day after
    # scrape_date_string e.g. if scrape_date_string is 2018-09-16,
    # this will run on 2018-09-17 at 3am
    # We only want refresh_glue_partitions to run on a regular scrape
    # (not a backfill) so
    yesterday_datetime = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday_date_string = yesterday_datetime.date().isoformat()
    if scrape_date_string == yesterday_date_string:
        logger.info(
            "Next execution date is in future, refreshing Athena partitions"
        )
        refresh_glue_partitions()
        logger.info("Succesfully refreshed partitions")

if args.scrape_type == "hourly":

    for survey in surveys:
        if scrape_date_in_surveydays(scrape_date_string, survey):
            survey_fact = get_survey_facts_from_api(
                survey, scrape_date_string
            )
            survey_fact_to_s3(survey_fact, survey, scrape_date_string)

import datetime
from api_requests import (
    get_surveys_from_api,
    get_sensors_dimension_from_api,
    get_survey_facts_from_api,
)
from refresh_partitions import refresh_glue_partitions

from transfer_to_s3 import sensor_dimension_to_s3, survey_fact_to_s3
from time_utils import get_survey_dates


def get_survey_from_id(survey_id):
    surveys = get_surveys_from_api()
    survey = [s for s in surveys if s["SurveyID"] == survey_id]

    if len(survey) == 1:
        survey = survey[0]
    else:
        raise ValueError("Survey doesn't exist")
    return survey


def rescrape_entire_survey(survey):
    surveydays = get_survey_dates(survey)

    sensor_dimension = get_sensors_dimension_from_api(survey)
    sensor_dimension_to_s3(sensor_dimension)

    for day in surveydays:

        scrape_date_string = datetime.datetime.strftime(day, "%Y-%m-%d")
        print(f"Scraping date {scrape_date_string}")
        survey_fact = get_survey_facts_from_api(survey, scrape_date_string)
        survey_fact_to_s3(survey_fact, survey, scrape_date_string)

    refresh_glue_partitions()

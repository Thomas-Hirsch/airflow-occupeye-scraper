import argparse
import job_utils
import json
import datetime
import logging
logger = logging.getLogger(__name__)

from dataengineeringutils import s3

parser = argparse.ArgumentParser(description='Optional app description')

parser.add_argument('--task_name', type=str, help='Which task to run?')
parser.add_argument('--survey_id', type=int, help='For fact scrape, which survey id')
parser.add_argument('--scrape_date', type=str, help='Which task to run?')

args = parser.parse_args()


if args.task_name == 'surveys_to_s3':
    surveys = job_utils.get_surveys()
    job_utils.surveys_to_s3(surveys)

if args.task_name == 'surveys_to_xcom':
    surveys = job_utils.get_surveys()
    survey_ids = [s['SurveyID'] for s in surveys]
    with open('/airflow/xcom/return.json', 'w') as outfile:
        json.dump(survey_ids, outfile)

if args.task_name == 'sensor_dimension_to_s3':
    surveys = job_utils.get_surveys()
    job_utils.sensors_dimension_to_s3(surveys)

if args.task_name == 'sensor_fact_to_s3':
    survey_id = args.survey_id
    surveys = job_utils.get_surveys()

    for s in surveys:
        if s['SurveyID'] == survey_id:
            survey = s

    survey_date = datetime.datetime.strptime(args.scrape_date, '%Y-%m-%d').date()
    df_full = job_utils.get_sensor_fact_df(survey, survey_date)

    if df_full is None:
        df_full = pd.DataFrame()

    # Note compression arg only used when first arg is filename https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html
    df_full.to_csv("temp_df_for_upload.csv.gz", index=False, compression='gzip')

    bucket = "alpha-dag-occupeye"
    path = f"raw_data_v4/sensor_observations/survey_id={survey['SurveyID']}/{args.scrape_date}.csv.gz"
    s3.upload_file_to_s3_from_path("temp_df_for_upload.csv.gz", bucket, path)

# Using local airflow, need to see how template works
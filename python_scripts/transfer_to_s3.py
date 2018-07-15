from transform_data import get_surveys_df, get_sensor_dimension_df, get_survey_fact_df
from dataengineeringutils import s3

def surveys_to_s3(surveys):
    df = get_surveys_df(surveys)
    bucket = "alpha-dag-occupeye"
    path = f"raw_data_v4/surveys/data.csv"
    full_path = bucket + "/" +  path
    s3.pd_write_csv_s3(df, full_path, index=False)


def sensor_dimension_to_s3(sensor_dimension):
        surveyid = sensor_dimension[0]["SurveyID"]
        df = get_sensor_dimension_df(sensor_dimension)
        bucket = "alpha-dag-occupeye"
        path = f"raw_data_v4/sensors/survey_id={surveyid}/data.csv"
        full_path = bucket + "/" +  path
        if len(df) > 0:
            s3.pd_write_csv_s3(df, full_path, index=False)

def survey_fact_to_s3(sensor_fact, survey):

    date_string = sensor_fact[0]['TriggerDate']
    df_survey_fact = get_survey_fact_df(sensor_fact)

    if df_survey_fact is None:
        df_survey_fact = pd.DataFrame()

    # Note compression arg only used when first arg is filename https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html
    df_survey_fact.to_csv("temp_df_for_upload.csv.gz", index=False, compression='gzip')

    bucket = "alpha-dag-occupeye"
    path = f"raw_data_v4/sensor_observations/survey_id={survey['SurveyID']}/{date_string}.csv.gz"
    s3.upload_file_to_s3_from_path("temp_df_for_upload.csv.gz", bucket, path)
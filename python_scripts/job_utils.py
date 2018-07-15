import boto3
import botocore
import datetime
import json
import logging
import pandas as pd
import requests
import urllib.parse

from dataengineeringutils import s3

logger = logging.getLogger(__name__)

def read_json_from_s3(s3_path):
    bucket, key = s3_path_to_bucket_key(s3_path)
    obj = s3_resource.Object(bucket, key)
    text = obj.get()['Body'].read().decode('utf-8')
    return json.loads(text)

def s3_path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    return bucket, key

def get_headers():
    url = base_url + "token"

    headers = {"Grant_type": "password",
               "Username": secrets["Username"],
               "Password": secrets["Password"]}

    r = requests.post(url, data=headers)
    token = json.loads(r.text)

    headers = {"Authorization":"Bearer {}".format(token["access_token"])}
    return headers

def get_surveys():

    url = base_url + "justice/api/surveys"

    logger.info(f"Getting URL: {url}")

    headers = get_headers()

    r = requests.get(url, headers=headers)
    data = json.loads(r.text)
    return data

def get_sensors(survey):
    surveyid = survey["SurveyID"]

    url = base_url + f"Justice/api/SurveyDevices/?surveyid={surveyid}"
    logger.debug(f"Getting URL: {url}")
    r = requests.get(url, headers=headers)
    data = json.loads(r.text)
    return data

def get_sensor_data(survey, obs_date=None):

    urlparams = {}
    keys = ["StartTime", "StartDate", "EndDate", "EndTime", "SurveyID"]
    for k in keys:
        urlparams[k] = survey[k]

    if obs_date:
        urlparams["StartDate"] = obs_date.isoformat()
        urlparams["EndDate"] = obs_date.isoformat()

    urlparams["Deployment"] = "Justice"
    urlparams["QueryType"] = "SensorActivity"

    url = base_url + "Justice/api/Query?" + urllib.parse.urlencode(urlparams, True)
    logger.debug(f"Getting URL: {url}")
    r = requests.get(url, headers=get_headers())
    data = json.loads(r.text)
    return data

def get_survey_dates(survey):

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days + 1)):  # +1 because range(0) = []
            yield start_date + datetime.timedelta(n)

    start_date = datetime.datetime.strptime(survey["StartDate"], "%Y-%M-%d").date()
    end_date_1 = datetime.datetime.strptime(survey["EndDate"], "%Y-%M-%d").date()
    end_date_2 = datetime.datetime.now().date() - datetime.timedelta(days=1)
    end_date = min(end_date_1, end_date_2)

    return daterange(start_date, end_date)

def s3_object_exists(bucket, path):
    try:
        s3_resource.Object(bucket, path).load()
        return True
    except botocore.exceptions.ClientError as e:
        return False

def get_sensor_df(survey):
    sensors = get_sensors(survey)
    sensors = strip_commas_from_api_response(sensors)
    df_sensors = pd.DataFrame(sensors)
    return df_sensors

def sensors_dimension_to_s3(surveys):
    for survey in surveys:
        df = get_sensor_df(survey)

        bucket = "alpha-dag-occupeye"
        path = f"raw_data_v4/sensors/survey_id={survey['SurveyID']}/data.csv"
        full_path = bucket + "/" +  path
        if len(df) > 0:
            try:
                del df["SurveyID"]
            except:
                print(df)
            s3.pd_write_csv_s3(df, full_path, index=False)

def sensor_datum_to_long_format(sensor_datum, times):
    """
    Sensor data has one row per day, with a column for the sensor's observation for each time period.
    We want the data in tidy format, so we want one record per observation

    Since there will be 100s of millions of observations in total, we want to make this data as succinct as possible
    (put all unnecessary data in dimension tables rather than in main fact table)
    """

    new_rows = []

    for time in times:
        this_row = {}

        this_row["SurveyDeviceID"] = sensor_datum["SurveyDeviceID"]
        this_row["TriggerDate"] = sensor_datum["TriggerDate"]

        this_row["time"] = time
        sensor_value = sensor_datum[time]
        this_row["sensor_value"] = sensor_value

        # Convert date and time into single datetime
        datetimestring = this_row["TriggerDate"]+time
        obs_datetime = datetime.datetime.strptime(datetimestring, "%Y-%m-%dt%H%M")
        this_row["obs_datetime"] = obs_datetime.strftime("%Y-%m-%d %H:%M:%S")

        del this_row["time"]
        del this_row["TriggerDate"]
        new_rows.append(this_row)

        previous_value = sensor_value
    return new_rows

def sensor_data_to_long_format(sensor_data):

    new_sensor_data = []

    times = [f"t{h:02d}{m:02d}" for h in range(0,24) for m in range(0,60,10)]

    for sensor_datum in sensor_data:
        long_format = sensor_datum_to_long_format(sensor_datum, times)
        new_sensor_data.extend(long_format)

    return new_sensor_data

def get_sensor_fact_df(survey, obs_date=None):
    sensor_data = get_sensor_data(survey, obs_date=obs_date)
    sensor_data = sensor_data_to_long_format(sensor_data)
    return pd.DataFrame(sensor_data)

def surveys_to_s3(surveys):
    surveys = strip_commas_from_api_response(surveys)
    df = pd.DataFrame(surveys)

    bucket = "alpha-dag-occupeye"
    path = f"raw_data_v4/surveys/data.csv"
    full_path = bucket + "/" +  path
    s3.pd_write_csv_s3(df, full_path, index=False)

def strip_commas_from_api_response(list_of_dicts):
    for my_dict in list_of_dicts:
        for k,v in my_dict.items():
            if type(v) == str:
                my_dict[k] = v.replace(",", ";")
    return list_of_dicts


s3_resource = boto3.resource('s3')

base_url = "https://cloud.occupeye.com/OccupEye/"

secrets = read_json_from_s3("s3://alpha-dag-occupeye/api_secret/api_secrets.json")

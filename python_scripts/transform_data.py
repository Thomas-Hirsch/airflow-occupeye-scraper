import datetime
import pandas as pd

from dataengineeringutils.pd_metadata_conformance import (
    impose_exact_conformance_on_pd_df,
    impose_metadata_column_order_on_pd_df,
)
from dataengineeringutils.utils import read_json


def strip_commas_from_api_response(list_of_dicts):
    for my_dict in list_of_dicts:
        for k, v in my_dict.items():
            if type(v) == str:
                my_dict[k] = v.replace(",", ";")
    return list_of_dicts


def get_surveys_df(surveys):
    surveys = strip_commas_from_api_response(surveys)
    surveys_df = pd.DataFrame(surveys)

    # Rename columns to conform to metadata
    renames = read_json("column_renames/surveys_renames.json")
    surveys_df = surveys_df.rename(columns=renames)

    # Impose metadata - i.e. ensure all expected columns are present and in correct order
    surveys_metadata = read_json("glue/meta_data/occupeye_db/surveys.json")
    surveys_df = impose_exact_conformance_on_pd_df(
        surveys_df, surveys_metadata
    )

    return surveys_df


def get_sensor_dimension_df(sensor_dimension):
    sensor_dimension = strip_commas_from_api_response(sensor_dimension)
    sensors_df = pd.DataFrame(sensor_dimension)
    del sensors_df[
        "SurveyID"
    ]  # Because it's a partition so we don't need to duplicate

    # Rename columns to conform to metadata
    renames = read_json("column_renames/sensors_renames.json")
    sensors_df = sensors_df.rename(columns=renames)

    # Impose metadata - i.e. ensure all expected columns are present and in correct order
    sensors_metadata = read_json("glue/meta_data/occupeye_db/sensors.json")
    sensors_df = impose_exact_conformance_on_pd_df(
        sensors_df, sensors_metadata
    )

    return sensors_df


def get_survey_fact_df(survey_fact):
    survey_fact_long = survey_fact_to_long_format(survey_fact)
    sensor_observations_metadata = read_json(
        "glue/meta_data/occupeye_db/sensor_observations.json"
    )
    renames = read_json("column_renames/sensor_observations_renames.json")
    sensor_observations_df = pd.DataFrame(survey_fact_long).rename(
        columns=renames
    )
    sensor_observations_df = impose_metadata_column_order_on_pd_df(
        sensor_observations_df, sensor_observations_metadata
    )

    return sensor_observations_df


def survey_fact_to_long_format(survey_fact):

    new_sensor_data = []

    for sensor_data in survey_fact:
        long_format = sensor_fact_data_to_long_format(sensor_data)
        new_sensor_data.extend(long_format)

    return new_sensor_data


def sensor_fact_data_to_long_format(sensor_data):
    """
    Sensor data has one row per day, with a column for the sensor's observation
    for each time period.
    We want the data in tidy format, so we want one record per observation

    Since there will be 100s of millions of observations in total,
    we want to make this data as succinct as possible
    (put all unnecessary data in dimension tables
    rather than in main fact table)
    """

    new_rows = []

    times = [
        f"t{h:02d}{m:02d}" for h in range(0, 24) for m in range(0, 60, 10)
    ]

    for time in times:
        this_row = {}

        this_row["SurveyDeviceID"] = sensor_data["SurveyDeviceID"]
        this_row["TriggerDate"] = sensor_data["TriggerDate"]

        this_row["time"] = time
        sensor_value = sensor_data[time]
        this_row["sensor_value"] = sensor_value

        # Convert date and time into single datetime
        datetimestring = this_row["TriggerDate"] + time
        obs_datetime = datetime.datetime.strptime(
            datetimestring, "%Y-%m-%dt%H%M"
        )
        this_row["obs_datetime"] = obs_datetime.strftime("%Y-%m-%d %H:%M:%S")

        del this_row["time"]
        del this_row["TriggerDate"]
        new_rows.append(this_row)

    return new_rows

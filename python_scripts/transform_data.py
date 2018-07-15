import datetime
import pandas as pd


from api_requests import get_surveys_from_api, get_sensors_dimension_from_api, get_survey_facts_from_api

def strip_commas_from_api_response(list_of_dicts):
    for my_dict in list_of_dicts:
        for k,v in my_dict.items():
            if type(v) == str:
                my_dict[k] = v.replace(",", ";")
    return list_of_dicts

def get_surveys_df(surveys):
    surveys = strip_commas_from_api_response(surveys)
    return pd.DataFrame(surveys)


def get_sensor_dimension_df(sensor_dimension):
    sensor_dimension = strip_commas_from_api_response(sensor_dimension)
    df_sensor_dimension = pd.DataFrame(sensor_dimension)
    return df_sensor_dimension

def get_survey_fact_df(survey_fact):

    if survey_fact is None:
        return pd.DataFrame()

    survey_fact_long = survey_fact_to_long_format(survey_fact)
    return pd.DataFrame(survey_fact_long)

def survey_fact_to_long_format(survey_fact):

    new_sensor_data = []

    for sensor_data in survey_fact:
        long_format = sensor_fact_data_to_long_format(sensor_data)
        new_sensor_data.extend(long_format)

    return new_sensor_data

def sensor_fact_data_to_long_format(sensor_data):
    """
    Sensor data has one row per day, with a column for the sensor's observation for each time period.
    We want the data in tidy format, so we want one record per observation

    Since there will be 100s of millions of observations in total, we want to make this data as succinct as possible
    (put all unnecessary data in dimension tables rather than in main fact table)
    """

    new_rows = []

    times = [f"t{h:02d}{m:02d}" for h in range(0,24) for m in range(0,60,10)]

    for time in times:
        this_row = {}

        this_row["SurveyDeviceID"] = sensor_data["SurveyDeviceID"]
        this_row["TriggerDate"] = sensor_data["TriggerDate"]

        this_row["time"] = time
        sensor_value = sensor_data[time]
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


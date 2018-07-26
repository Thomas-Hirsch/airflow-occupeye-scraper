from pyathenajdbc import connect
staging_dir = 's3://alpha-dag-occupeye/query_temp_dir/'

import pandas as pd

conn = connect(
              s3_staging_dir=staging_dir,
              region_name='eu-west-1')
              
import feather
from dataengineeringutils import s3

def get_df_sql(survey_id, category_1=None, category_2=None,category_3=None,floor=None):
    
    category_filter = ""
    
    if category_1 is not None:
        category_filter = f"{category_filter} and se.category_1 = '{category_1}'"
    
    if category_2 is not None:
        category_filter = f"{category_filter} and se.category_2 = '{category_2}'"
        
    if category_3 is not None:
        category_filter = f"{category_filter} and se.category_3 = '{category_3}'"
        
    if floor is not None:
        category_filter = f"{category_filter} and se.floor = '{floor}'"
    
    out_string = f"""select so.sensor_value, so.obs_datetime, so.survey_device_id 
    from occupeye_db_live.sensor_observations as so
    left join occupeye_db_live.sensors as se
    on so.survey_device_id = se.surveydeviceid
    where so.survey_id = {survey_id}
    {category_filter}"""
    
    return out_string


def featherify_and_upload_df_to_s3(df,bucket,path):
    feather.write_dataframe(df, "temp_file_for_upload.feather")
    s3.upload_file_to_s3_from_path("temp_file_for_upload.feather",bucket,path)
    
def aggregate_floor(survey_id,floor):
    bucket = "alpha-app-occupeye-automation"
    path = f"surveys/{survey_id}/by_floor/{floor}.feather"
    df = pd.read_sql(get_df_sql(survey_id,floor=floor),conn)
    featherify_and_upload_df_to_s3(df,bucket,path)
    
def aggregate_team(survey_id,category_1,category_2=None):
    
    team = category_1
    
    if category_2 is not None:
        team += f" - {category_2}"
    
    bucket = "alpha-app-occupeye-automation"
    path = f"surveys/{survey_id}/by_team/{team}.feather"
    df = pd.read_sql(get_df_sql(survey_id,category_1=category_1,category_2=category_2),conn)
    featherify_and_upload_df_to_s3(df,bucket,path)

def aggregate_survey(survey_id):
    bucket = "alpha-app-occupeye-automation"
    path = f"surveys/{survey_id}/all.feather"
    df = pd.read_sql(get_df_sql(survey_id

def scrape_surveys():
    surveys = pd.read_sql("select * from occupeye_db_live.surveys",conn)
    bucket = "alpha-app-occupeye-automation"
    path = "surveys.feather"
    featherify_and_upload_df_to_s3(surveys,bucket,path)

def scrape_sensors():
    sensors = pd.read_sql("select * from occupeye_db_live.sensors",conn)
    bucket = "alpha-app-occupeye-automation"
    path = "sensors.feather"
    featherify_and_upload_df_to_s3(sensors,bucket,path)
    
    
def scrape_all():
    
    scrape_surveys()
    scrape_sensors()
    
    
    survey_categories = pd.read_sql("select distinct survey_id, floor, category_1, category_2 from occupeye_db.sensors",conn)
    survey_ids = sensors.survey_id.unique()
    
    for survey_id in survey_ids:
        aggregate_survey(survey_id)
        
        selected_survey_categories = survey_categories[(survey_categories.survey_id == survey_id)]
        
        survey_floors = selected_survey_categories.floor.unique()
        
        for floor in survey_floors:
            aggregate_floor(survey_id,floor)
        
        survey_teams = selected_survey_categories[['category_1','category_2']].drop_duplicates()
        
        for category_1 in survey_teams.category_1.unique():
            aggregate_team(survey_id,category_1)
            
            category_2s = survey_teams[(survey_teams.category_1==category_1)].category_2
            for category_2 in category_2s:
                aggregate_team(survey_id,category_1,category_2)

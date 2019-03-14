from pydbtools import get_athena_query_response

def refresh_glue_partitions():

    get_athena_query_response("""
        MSCK REPAIR TABLE occupeye_db_live.sensors;
        """)

    get_athena_query_response("""
        MSCK REPAIR TABLE occupeye_db_live.sensor_observations;
        """)
    
    get_athena_query_response("""
        MSCK REPAIR TABLE occupeye_app_db.sensors;
        """)

    get_athena_query_response("""
        MSCK REPAIR TABLE occupeye_app_db.sensor_observations;
        """)


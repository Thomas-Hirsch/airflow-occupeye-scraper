from get_athena_query_response import get_athena_query_response

def refresh_glue_partitions():

    get_athena_query_response("""
        MSCK REPAIR TABLE occupeye_db.sensors;
        """,  out_path="s3://alpha-dag-occupeye/query_temp_dir")

    get_athena_query_response("""
        MSCK REPAIR TABLE occupeye_db.sensor_observations;
        """, out_path="s3://alpha-dag-occupeye/query_temp_dir")

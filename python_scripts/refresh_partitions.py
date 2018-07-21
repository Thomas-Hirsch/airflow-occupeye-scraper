from pyathenajdbc import connect

def refresh_glue_partitions():
    conn = connect(s3_staging_dir='s3://alpha-dag-occupeye/partition_temp_dir',
                region_name='eu-west-1')

    with conn.cursor() as cursor:
        cursor.execute("""
        MSCK REPAIR TABLE occupeye_db.sensors;
        """)

    with conn.cursor() as cursor:
        cursor.execute("""
        MSCK REPAIR TABLE occupeye_db.sensor_observations;
        """)

    conn.close()
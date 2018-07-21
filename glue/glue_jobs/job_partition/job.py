import os
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import DateType, StringType, IntegerType, StructField, StructType

# Functions from our custom glue job lib at https://github.com/moj-analytical-services/gluejobutils
from gluejobutils.s3 import spark_read_csv_using_metadata_path

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'metadata_base_path'])


metadata_base_path = args['metadata_base_path']
print "metadata_base_path is {}".format(metadata_base_path)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# To begin with, let's just read some data in and write it back out in partitions to s3
paths_dict = {}
for name in ['surveys', 'sensors', 'sensor_observations']:
    paths_dict[name] = {}
    paths_dict[name]['md'] =  os.path.join(metadata_base_path, "occupeye_db/{}.json".format(name))
    paths_dict[name]['csv'] =  "s3://alpha-dag-occupeye/raw_data_v3/{}".format(name)

surveys = spark_read_csv_using_metadata_path(spark, paths_dict['surveys']['md'], paths_dict['surveys']['csv'], header=True)
sensors = spark_read_csv_using_metadata_path(spark, paths_dict['sensors']['md'], paths_dict['sensors']['csv'], header=True)
sensor_observations = spark_read_csv_using_metadata_path(spark, paths_dict['sensor_observations']['md'], paths_dict['sensor_observations']['csv'], header=True)

sensors.registerTempTable('sensors')
surveys.registerTempTable('surveys')
sensor_observations.registerTempTable('sensor_observations')

sql = """

select se.*, so.obs_datetime, so.sensor_value from
surveys as su
left join sensors as se
on su.survey_id = se.survey_id
left join sensor_observations as so
on se.surveydeviceid = so.SurveyDeviceID
"""

df = spark.sql(sql)

write_path = "s3://alpha-dag-occupeye/partitions2/"

df.write.partitionBy(['category_2', 'floor']).csv(write_path, mode='overwrite')
# df.coalesce(8).write.csv(write_path, mode='overwrite')

job.commit()
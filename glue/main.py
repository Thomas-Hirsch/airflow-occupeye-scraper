import logging
import sys
import os


import boto3

from etl_manager.meta import DatabaseMeta
from etl_manager.etl import GlueJob

s3_client = boto3.client('s3')

BUCKET = "alpha-dag-occupeye"
ROLE = "alpha_user_robinl"


if __name__ == '__main__':

    db_job = GlueJob('glue_jobs/job_partition/', bucket = BUCKET, job_role = ROLE)
    db_job.allocated_capacity = 2
    # Now let's run the job on AWS Glue
    db_job.run_job()


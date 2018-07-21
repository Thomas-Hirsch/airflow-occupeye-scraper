FROM python:3.7



RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libatlas-base-dev gfortran

RUN mkdir -p /opt/pandas/build/

COPY requirements.txt /opt/pandas/build/requirements.txt

RUN pip install -r /opt/pandas/build/requirements.txt


RUN pip install git+git://github.com/moj-analytical-services/dataengineeringutils.git@48cc711a53f053609960756c52461aaa61e01f8e
# RUN pip install git+https://github.com/apache/incubator-airflow.git
RUN mkdir -p /airflow/xcom
COPY python_scripts /

CMD ["bash"]
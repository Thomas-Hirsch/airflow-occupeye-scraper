FROM python:3.7



RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libatlas-base-dev gfortran


RUN mkdir -p /opt/pandas/build/

COPY requirements.txt /opt/pandas/build/requirements.txt

RUN pip install -r /opt/pandas/build/requirements.txt



RUN mkdir -p /airflow/xcom
COPY python_scripts /
COPY glue glue/
COPY column_renames column_renames/

CMD ["bash"]

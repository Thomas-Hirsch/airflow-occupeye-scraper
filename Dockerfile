FROM civisanalytics/datascience-python@sha256:2be577c124895ac6c6c9028d656fcba7f2cb6afa31bedee6661775662af8a2c2
RUN pip install git+git://github.com/moj-analytical-services/dataengineeringutils.git@48cc711a53f053609960756c52461aaa61e01f8e
RUN mkdir -p /airflow/xcom
COPY python_scripts /

CMD ["bash"]
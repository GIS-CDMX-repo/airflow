FROM python:3.9-buster
RUN wget https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-3.9.txt
RUN pip install "apache-airflow[celery]==2.5.3" --constraint constraints-3.9.txt
RUN rm constraints-3.9.txt
RUN pip install psycopg2 pandas pyamqp 'apache-airflow-providers-docker'
ENV AIRFLOW_HOME /opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES False
ENTRYPOINT ["airflow"]

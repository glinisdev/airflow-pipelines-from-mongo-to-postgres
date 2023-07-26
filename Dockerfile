FROM apache/airflow:2.4.3
COPY ./dags/ /opt/airflow/dags
RUN mkdir -p plugins
RUN mkdir -p logs
COPY ./requirements.txt /opt/airflow/requirements.txt
RUN pip3 install --user --upgrade pip
RUN pip3 install --no-cache-dir --user -r /opt/airflow/requirements.txt
RUN pip install boto3
USER root
RUN chown -R 50000:root dags/
RUN mkdir -p /opt/airflow/dags/data/daily_updates
RUN chown -R 50000:root /opt/airflow/dags/data/daily_updates/
RUN mkdir -p /opt/airflow/dags/data/daily_archieve
RUN chown -R 50000:root /opt/airflow/dags/data/daily_archieve/
USER airflow

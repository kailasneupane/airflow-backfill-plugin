FROM apache/airflow:2.10.2-python3.11

USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt

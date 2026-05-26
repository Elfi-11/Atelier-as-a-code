FROM apache/airflow:3.0.1-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY scripts/ /opt/scripts/
COPY patients_*.csv /opt/data/

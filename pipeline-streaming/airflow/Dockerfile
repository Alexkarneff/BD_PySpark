# Airflow 3.3.2 + the producer's dependency.
# requests is already in the base image; confluent-kafka is installed with
# Airflow's official constraints so it can't break Airflow's own dependencies.

FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "confluent-kafka==2.15.1" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

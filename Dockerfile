#imagenes de airflow y python3
FROM apache/airflow:3.0.0-python3.9 

#Cambiamos al usuario Root para las instalaciones basicas de sistema
USER root

RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    python3-dev \
    libpq-dev \ 
    build-essential \
    curl \
    gnupg \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean

USER airflow


COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY ./dags /opt/airflow/dags
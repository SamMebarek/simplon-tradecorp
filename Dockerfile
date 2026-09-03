FROM quay.io/jupyter/pyspark-notebook:latest

USER root

RUN python -c "import urllib.request; urllib.request.urlretrieve('https://jdbc.postgresql.org/download/postgresql-42.7.0.jar', '/usr/local/spark/jars/postgresql-42.7.0.jar')"

USER ${NB_UID}

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /home/jovyan
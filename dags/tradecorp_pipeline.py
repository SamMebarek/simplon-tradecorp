from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os
from datetime import datetime, timedelta

PROJECT_PATH = "//c/Users/samir/Desktop/tradecorp"


MOUNTS = [
    Mount(
        source=f"{PROJECT_PATH}/src",
        target="/home/jovyan/src",
        type="bind",
    ),
    Mount(
        source=f"{PROJECT_PATH}/data",
        target="/home/jovyan/data",
        type="bind",
    ),
    Mount(
        source=f"{PROJECT_PATH}/.env",
        target="/home/jovyan/.env",
        type="bind",
    ),
]


default_args = {
    "owner": "tradecorp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_docker_task(task_id, command):
    return DockerOperator(
        task_id=task_id,
        image="tradecorp_spark:latest",
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
        environment={
            "AZURE_STORAGE_ACCOUNT_NAME": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
            "AZURE_STORAGE_ACCOUNT_KEY": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
        },
    )


with DAG(
    dag_id="tradecorp_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["tradecorp", "etl", "spark"],
) as dag:

    fetch_exchange_rates = create_docker_task(
        "fetch_exchange_rates",
        "python /home/jovyan/src/fetch_exchange_rates.py",
    )

    reader = create_docker_task(
        "reader",
        "spark-submit /home/jovyan/src/reader.py",
    )

    transformer = create_docker_task(
        "transformer",
        "spark-submit /home/jovyan/src/transformer.py",
    )

    writer = create_docker_task(
        "writer",
        "spark-submit /home/jovyan/src/writer.py",
    )

    fetch_exchange_rates >> reader >> transformer >> writer

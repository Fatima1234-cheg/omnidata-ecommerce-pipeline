from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_PATH = "C:/Users/lessa/omnidata-ecommerce-pipeline"

default_args = {
    'owner': 'omnidata',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='omnidata_ecommerce_pipeline',
    default_args=default_args,
    description='Pipeline complet de traitement Olist',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # On prépare le dossier partagé (le volume) avec la méthode officielle
    mount_app = Mount(source=PROJECT_PATH, target="/app", type="bind")

    clean_orders = DockerOperator(
        task_id='clean_orders',
        image='apache/spark:3.5.1-python3',
        command='/opt/spark/bin/spark-submit /app/src/transformations/clean_orders_spark.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[mount_app]
    )

    load_postgres = DockerOperator(
        task_id='load_to_postgres',
        image='python:3.11-slim',
        command='bash -c "pip install pandas sqlalchemy psycopg[binary] pyarrow && python /app/src/load_all_gold_to_postgres.py"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[mount_app],
        environment={'DB_HOST': 'host.docker.internal'}
    )

    clean_orders >> load_postgres
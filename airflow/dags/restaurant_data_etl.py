from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="restaurant_data_etl",
    start_date=datetime(2026, 9, 30),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
)
def restaurant_data_etl():

    @task
    def extract_produce_data():
        import subprocess

        subprocess.run(
            [
                "python3",
                "/opt/airflow/producers/restaurant_producer.py"
            ],
            cwd="/opt/airflow",
            check=True
        )

    transform_load_data = SparkSubmitOperator(
        task_id="transform_load_data",
        application="/opt/airflow/scripts/restaurant_data.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077"
        }
    )

    @task
    def build_and_fill_star_schema():
        import subprocess

        subprocess.run(
            [
                "docker",
                "exec",
                "dbt",
                "dbt",
                "build",
                "--project-dir",
                "/usr/app/restaurant_analytics"
            ],
            check=True
        )

    extract_produce_data() >> transform_load_data >> build_and_fill_star_schema()


restaurant_data_etl()
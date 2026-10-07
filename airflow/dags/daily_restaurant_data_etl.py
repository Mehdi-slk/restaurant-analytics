from datetime import datetime

from airflow.sdk import dag, task, get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="restaurant_daily_data_etl",
    start_date=datetime(2026, 9, 30),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
)
def restaurant_daily_data_etl():

    @task
    def extract_produce_daily_data():
        import subprocess

        context = get_current_context()
        extraction_date = context["logical_date"].strftime("%Y-%m-%d")

        subprocess.run(
            [
                "python3",
                "/opt/airflow/producers/daily_restaurant_producer.py",
                extraction_date
            ],
            cwd="/opt/airflow",
            check=True
        )

    transform_load_daily_data = SparkSubmitOperator(
        task_id="transform_load_daily_data",
        application="/opt/airflow/scripts/daily_restaurant_data.py",
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

    (
        extract_produce_daily_data()
        >> transform_load_daily_data
        >> build_and_fill_star_schema()
    )


restaurant_daily_data_etl()
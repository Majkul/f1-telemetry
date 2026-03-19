from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

RAW_BASE  = "/tmp/f1_data/raw"
SPARK_APP = "/home/michal/praca/pipelines/dim_tables_ETL.py"

def download_f1_data(**ctx):
    import fastf1, pandas as pd, os

    os.makedirs(f"{RAW_BASE}/dims", exist_ok=True)

    drivers, teams = [], []
    for year in range(2019, 2026):
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        for _, event in schedule.iterrows():
            try:
                s = fastf1.get_session(year, event["RoundNumber"], "R")
                s.load(laps=False, telemetry=False, weather=False, messages=False)
                for _, r in s.results.iterrows():
                    drivers.append({
                        "code": r["Abbreviation"],
                        "full_name": r["FullName"],
                    })
                    teams.append({
                        "name": r["TeamName"],
                    })
            except Exception as e:
                print(f"Pominięto {year}: {e}")

    pd.DataFrame(drivers).drop_duplicates("code") \
      .to_parquet(f"{RAW_BASE}/dims/drivers_raw.parquet", index=False)

    pd.DataFrame(teams).drop_duplicates("name") \
      .to_parquet(f"{RAW_BASE}/dims/teams_raw.parquet", index=False)

    pd.DataFrame([
        {"name": "SOFT", "category": "DRY"},
        {"name": "MEDIUM", "category": "DRY"},
        {"name": "HARD", "category": "DRY"},
        {"name": "INTER", "category": "INTERMEDIATE"},
        {"name": "WET", "category": "WET"},
        {"name": "UNKNOWN", "category": "UNKNOWN"},
    ]).to_parquet(f"{RAW_BASE}/dims/compounds_raw.parquet", index=False)

    print("Extracted: drivers, teams, compounds")


def run_spark_job(**ctx):
    import os
    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

    os.environ["PYSPARK_PYTHON"] = "/home/michal/anaconda3/envs/pyspark_env/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/michal/anaconda3/envs/pyspark_env/bin/python"

    db_url = Variable.get("DB_URL")
    db_user = Variable.get("DB_USER")
    db_password = Variable.get("DB_PASSWORD")

    hook = SparkSubmitHook(
        conn_id="spark_default",
        packages="org.postgresql:postgresql:42.7.2",
        application_args=[
            "--db-url", db_url,
            "--db-user", db_user,
            "--db-password", db_password,
            "--raw-path", f"{RAW_BASE}/dims",
        ],
    )
    hook.submit(SPARK_APP)


with DAG(
    "base_dims_pipeline",
    default_args={
        "owner":   "michal",
        "retries": 2,
        'start_date': datetime(2024, 1, 1)
    },
    schedule=None,
    catchup=False,
    tags=["f1", "dims"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_drivers_teams",
        python_callable=download_f1_data,
    )

    spark_task = PythonOperator(
        task_id="spark_load",
        python_callable=run_spark_job,
    )

    cleanup_task = BashOperator(
        task_id="cleanup_all_data",
        bash_command=f"rm -rf {RAW_BASE}/dims",
        trigger_rule="all_success",
    )

    extract_task >> spark_task >> cleanup_task
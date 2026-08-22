import datetime as dt
from pendulum import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="incremental_load_Ex",
    start_date=datetime(year=2026, month=8, day=1, hour=22, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=8, day=25, hour=22, tz="Asia/Kolkata"),
    schedule=dt.timedelta(days=3),
    catchup=True,
)
def incremental_load_Ex():

    @task.python
    def incremental_data_load(**kwargs):
        date_interval_date = kwargs["data_interval_start"]
        date_interval_end = kwargs["data_interval_end"]
        print(f"Fetching data from {date_interval_date} to {date_interval_end}")

    @task.bash
    def process_data():
        return "echo 'Processing data from {{ data_interval_start }} to {{ data_interval_end }}'"

    fetch_data = incremental_data_load()
    process_data = process_data()

    fetch_data >> process_data


# Registering the DAG
incremental_load_Ex()

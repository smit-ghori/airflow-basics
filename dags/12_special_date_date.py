from airflow.sdk import EventsTimetable, dag, task
from pendulum import datetime

special_dates = EventsTimetable(
    event_dates=[
        datetime(2026, 8, 1, 22, 0, tz="Asia/Kolkata"),
        datetime(2026, 8, 15, 22, 0, tz="Asia/Kolkata"),
        datetime(2026, 8, 22, 22, 0, tz="Asia/Kolkata"),
        datetime(2026, 10, 10, 22, 0, tz="Asia/Kolkata"),
    ],
    description="Special dates for DAG execution",
)


@dag(
    dag_id="special_Date_dag_Ex",
    start_date=datetime(2026, 8, 1, 0, 0, tz="Asia/Kolkata"),
    end_date=datetime(2026, 10, 10, 23, 59, tz="Asia/Kolkata"),
    schedule=special_dates,
    catchup=True,
    is_paused_upon_creation=False,
)
def special_Date_dag_Ex():

    @task
    def special_event_task(**kwargs):
        logical_date = kwargs["logical_date"]

        print(f"Special event task executed on {logical_date}")

    special_event_task()


special_Date_dag_Ex()

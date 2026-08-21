from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="schedule_dag",
    start_date=datetime(year=2026, month=8, day=18, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=8, day=25, tz="Asia/Kolkata"),
    schedule="@daily",
    is_paused_upon_creation=False,
    catchup=True,  # True: Run all the missed schedules from start_date to current date
    # False: Run only the latest schedule
)
def schedule_dag():

    @task
    def first_task():
        print("This is first task...")

    @task
    def second_task():
        print("This is second task...")

    @task
    def third_task():
        print("This is third task...")

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


schedule_dag()

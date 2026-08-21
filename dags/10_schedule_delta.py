from airflow.sdk import dag, task
from pendulum import datetime
from datetime import timedelta


@dag(
    dag_id="schedule_delta_Ex",
    start_date=datetime(year=2026, month=8, day=18, hour=22, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=8, day=25, hour=22, tz="Asia/Kolkata"),
    schedule=timedelta(days=3),  # Run every 3 days at 10PM
    is_paused_upon_creation=False,  # True: DAG will be paused when created, False: DAG will be active when created
    catchup=True,  # True: Run all the missed schedules from start_date to current
)
def schedule_delta_Ex():

    @task.python
    def first_task():
        print("This is first task")

    @task.python
    def second_task():
        print("This is second task")

    @task.python
    def third_task():
        print("This is third task, Smit")

    # Defining task dependency
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


# Registering the DAG
schedule_delta_Ex()

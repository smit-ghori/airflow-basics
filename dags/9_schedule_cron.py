from airflow.sdk import dag, task
from pendulum import datetime

# Cron Syntax
# minute hour day month weekday
#   0     22    *    *      *


@dag(
    dag_id="cron_dag_example",
    start_date=datetime(year=2026, month=8, day=18, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=8, day=25, tz="Asia/Kolkata"),
    schedule="0 22 * * MON-FRI",  # Run 10PM Monday to Friday
    is_paused_upon_creation=False,  # True: DAG will be paused when created, False: DAG will be active when created
    catchup=True,  # True: Run all the missed schedules from start_date to current
)
def cron_dag_example():

    @task.python
    def first_task():
        print("This is first task")

    @task.python
    def second_task():
        print("This is first task")

    @task.python
    def third_task():
        print("This is first task, Smit")

    # Defining task dependency
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


# Registering the DAG
cron_dag_example()

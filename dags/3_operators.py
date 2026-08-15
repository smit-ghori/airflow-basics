from airflow.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(dag_id="operator_dag")
def operator_dag():

    @task.python
    def first_task():
        print("This is first task")

    @task.python
    def second_task():
        print("This is first task")

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org/"

    bash_task_school = BashOperator(
        task_id="bash_task_school",
        bash_command='echo "https://airflow.apache.org/"',
    )
    
    first = first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    bash_school_op = bash_task_school
    
    first >> second >> bash_modern >> bash_school_op
    
operator_dag()

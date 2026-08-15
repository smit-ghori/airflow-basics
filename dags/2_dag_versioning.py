from airflow.sdk import dag, task


@dag(dag_id="versioning_dag")
def versioning_dag():

    @task.python
    def first_task():
        print("This is first task")

    @task.python
    def second_task():
        print("This is first task")

    @task.python
    def third_task():
        print("This is first task, Smit")

    @task.python
    def versioning():
        print("This is version task!!!")

    # Defining task dependency
    first = first_task()
    second = second_task()
    third = third_task()
    version = versioning()

    first >> second >> third >> version


# Registering the DAG
versioning_dag()

# use xcom automatic method not manual because airflow is managing it automatically

from airflow.sdk import dag, task


@dag(dag_id="xcom_kwargs")
def xcom_kwargs():

    @task
    def pushing_args(**kwargs):
        ti = kwargs["ti"]

        print("Extracting data...")
        data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key="return_result", value=data)

    @task
    def pull_and_transform(**kwargs):
        ti = kwargs["ti"]

        pulled_data = ti.xcom_pull(task_ids="pushing_args", key="return_result")["data"]
        transformed_data = {"trans_data": pulled_data * 2}

        ti.xcom_push(key="transformed_data", value=transformed_data)

    @task
    def final_result(**kwargs):
        ti = kwargs["ti"]
        t_data = ti.xcom_pull(task_ids="pull_and_transform", key="transformed_data")

        return t_data

    pushing_args() >> pull_and_transform() >> final_result()


xcom_kwargs()

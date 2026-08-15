from airflow.sdk import dag, task


@dag(dag_id="xcom_dag_prec")
def xcom_dag_prec():

    @task
    def initial_setup():
        print("Fetching the data....")
        d1 = {"data": [1, 2, 3, 4, 5]}
        return d1

    @task
    def process_data(data: dict):
        transformed = data["data"] * 2
        transformed_dict = {"transformed": transformed}
        return transformed_dict

    @task
    def load_data(data: dict):
        return data

    # Sharing the output
    i_set = initial_setup()
    p_data = process_data(i_set)
    l_data = load_data(p_data)


xcom_dag_prec()

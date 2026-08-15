from airflow.sdk import dag, task


@dag(dag_id="conditional_dag_demo")
def conditional_dag_demo():  # sourcery skip: assign-if-exp

    @task
    def extract_data():
        d1 = {
            "postgre": [1, 2, 3, 4, 5],
            "s2": [4, 5, 6, 7, 8, 9],
            "api": [45, 66, 78, 89],
            "flag": "false",
        }
        return d1

    @task
    def process_postgresql(data: dict):
        postgresql_data = data["postgre"]
        print("Processing post gre data......")
        trans_postgresql_data = [i * 2 for i in postgresql_data]
        return trans_postgresql_data

    @task
    def process_s3(data: dict):
        s3_data = data["s2"]
        print("Processing s3 data......")
        trans_s3 = [i * 2 for i in s3_data]
        return trans_s3

    @task
    def process_api(data: dict):
        api_data = data["api"]
        print("Processing s3 data......")
        trans_api = [i * 2 for i in api_data]
        return trans_api

    # --------------Condition----------------------
    @task.branch
    def pro_condition(data: dict):
        if data["flag"] == "true":
            return "load_transformed_data"  # Returns that task id
        else:
            return "no_load"  # Returns that task id

    @task.bash
    def load_transformed_data(postgre, s3, api):
        return f"echo 'Loaded data: {postgre}, {s3}, {api}'"

    # run this task if flag == false
    @task.bash
    def no_load():
        print("No loading task...")
        return "echo 'No tasks...'"

    initial_data = extract_data()
    t_postgresql = process_postgresql(initial_data)
    t_s3 = process_s3(initial_data)
    t_api = process_api(initial_data)
    load = load_transformed_data(t_postgresql, t_s3, t_api)
    no_load_task = no_load()
    condition = pro_condition(initial_data)

    # In square bracket tasks will run parallel
    initial_data >> [t_postgresql, t_s3, t_api] >> condition >> [load, no_load_task]


conditional_dag_demo()

from airflow.sdk import dag, task


@dag
def parallel_demo():

    @task
    def extract_data():
        d1 = {
            "postgre": [1, 2, 3, 4, 5],
            "s2": [4, 5, 6, 7, 8, 9],
            "api": [45, 66, 78, 89],
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

    @task.bash
    def load_transformed_data(postgre, s3, api):
        return f"echo 'Loaded data: {postgre}, {s3}, {api}'"

    initial_data = extract_data()
    t_postgresql = process_postgresql(initial_data)
    t_s3 = process_s3(initial_data)
    t_api = process_api(initial_data)
    load = load_transformed_data(t_postgresql, t_s3, t_api)

    # In square bracket tasks will run parallel
    initial_data >> [t_postgresql, t_s3, t_api] >> load


parallel_demo()

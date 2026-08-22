from airflow.sdk import asset
from pendulum import datetime
import os


@asset(
    schedule="@daily",
    uri="/opt/airflow/logs/data/extracted_data.txt",
    name="asset_example",
)
def asset_example(self):

    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as file:
        file.write("Data fetched on successfully")

    print(f"Data written successfully {self.uri}")

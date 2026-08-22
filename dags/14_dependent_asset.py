from airflow.sdk import asset
from asset_13 import asset_example
import os


@asset(
    name="dependent_asset_example",
    uri="/opt/airflow/logs/data/preprocessed_data.txt",  # optional
    schedule=asset_example,  # this will run after the asset_example finish
)
def dependent_asset_example(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as file:
        file.write("This is processed file")

    print("Data processed successfully!!")

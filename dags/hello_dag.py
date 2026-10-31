from datetime import datetime
from airflow.sdk import dag,task

from airflow.providers.standard.operators.bash import BashOperator

@dag(
    start_date=datetime(2026,10,1),
    schedule="@daily",
    catchup=False,
    tags=["nauka"]
)
def hello_dag():
    say_date= BashOperator(
        task_id="say_date",
        bash_command = "echo 'Przetwarzam dane za dzien: {{ds}}'",
    )
    @task 
    def say_hello(ds=None):
        print(f"Hi python! Przetwarzam dane za dzien {ds}")

    @task 
    def say_bye():
        print("Koniec")

    say_date >> say_hello() >> say_bye()

hello_dag()
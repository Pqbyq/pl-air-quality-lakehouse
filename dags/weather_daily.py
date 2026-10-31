from datetime import datetime, timedelta 

import requests
from airflow.sdk import dag, task 
import json
from pathlib import Path
import pandas as pd

API_URL = "https://api.open-meteo.com/v1/forecast"



@dag(
    start_date = datetime(2026,10,1),
    schedule = "@daily",
    catchup=False,
    tags=["nauka"],
    default_args={
        "retries":3,
        "retry_delay":timedelta(minutes=2)
    }
 
)

def weather_daily():

    @task
    def fetch_weather(ds=None):
        params= {
            "latitude": 52.23,
            "longitude": 21.01,
            "hourly": "temperature_2m",
            "start_date": ds,
            "end_date": ds
        }

        r = requests.get(API_URL,params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        temps = data["hourly"]["temperature_2m"] 
        print(f"Dzien {ds}: liczba pomiarów = {len(temps)}")
        print(f"Min {min(temps)}, Max {max(temps)}")
        path = Path(f"/usr/local/airflow/include/data/raw/weather/dt={ds}/weather.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f)
        print(f"Zapisano: {path}")
        return str(path)

    @task
    def transform_weather(raw_path,ds=None):
        with open(raw_path) as f:
            data = json.load(f)

        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"]
        })
        df["time"] = pd.to_datetime(df["time"])
        df["dt"] = ds

        path = Path(f"/usr/local/airflow/include/data/processed/weather/dt={ds}/weather.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        print(f"Zapisano: {len(df)} wierszy do {path}")
        return str(path)

    transform_weather(fetch_weather())  


weather_daily()
from datetime import datetime, timedelta 

import requests
from airflow.sdk import dag, task 
import json
from pathlib import Path

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

    fetch_weather()

weather_daily()
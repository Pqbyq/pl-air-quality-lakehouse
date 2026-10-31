from datetime import datetime, timedelta
import json
from pathlib import Path

import requests
from airflow.sdk import dag, task

STATIONS_URL = "https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll"
PAGE_SIZE = 100


@dag(
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["projekt", "gios"],
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
)
def gios_stations():

    @task
    def fetch_stations(ds=None):
        all_stations = []                       # tu zbieramy stacje ze wszystkich stron

        # --- strona 0: pobierz ją i odczytaj, ile jest wszystkich stron ---
        # TODO: requests.get z params={"page": 0, "size": PAGE_SIZE}, timeout=30
        reponse = requests.get(STATIONS_URL, params={"page": 0, "size": PAGE_SIZE}, timeout=30)

        # TODO: raise_for_status, potem .json()
        reponse.raise_for_status()
        # TODO: total_pages = ... (pole "totalPages")
        data = reponse.json()
        total_pages = data["totalPages"]

        # TODO: dołóż stacje z tej strony do all_stations
        #       (klucz z listą stacji: "Lista stacji pomiarowych"; użyj .extend(...))
        all_stations.extend(data["Lista stacji pomiarowych"])
        # --- pozostałe strony: od 1 do total_pages - 1 ---
        # TODO: pętla for po numerach stron; w każdej: get, raise_for_status,
        for page in range(1, total_pages):
            reponse = requests.get(STATIONS_URL, params={"page": page, "size": PAGE_SIZE}, timeout=30)
            reponse.raise_for_status()
            data = reponse.json()
            all_stations.extend(data["Lista stacji pomiarowych"])
        #       json, dołóż stacje do all_stations
        


        print(f"Pobrano {len(all_stations)} stacji z {total_pages} stron")

        # --- zapis do raw ---
        # TODO: ścieżka z dt={ds}, mkdir, zapis all_stations do JSON (tryb "w")
        raw_path = Path(f"/usr/local/airflow/include/data/raw/gios/stations/dt={ds}")
        raw_path.mkdir(parents=True, exist_ok=True)
        file_path = raw_path / "stations.json"
        with open(file_path, "w") as f:
            json.dump(all_stations, f)
        return str(file_path)

    fetch_stations()


gios_stations()
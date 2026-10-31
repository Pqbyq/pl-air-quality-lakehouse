# =============================================================================
# DAG weather_daily – codziennie pobiera pogodę dla Warszawy
#
# Przepływ danych:
#   API Open-Meteo
#      │  krok 1: fetch_weather – pobierz i zapisz NA SUROWO
#      ▼
#   include/data/raw/weather/dt=YYYY-MM-DD/weather.json
#      │  krok 2: transform_weather – zrób z tego tabelkę
#      ▼
#   include/data/processed/weather/dt=YYYY-MM-DD/weather.parquet
#
# Zasada: jeden run = jeden dzień (ds). Ponowny run nadpisuje pliki tego dnia,
# więc nie powstają duplikaty (idempotencja), a historię pobiera się backfillem.
# =============================================================================

# --- Importy: "biorę narzędzia z szuflady" ------------------------------------
from datetime import datetime, timedelta   # punkt w czasie i odstęp czasu

import requests                            # wysyłanie zapytań do API przez internet
from airflow.sdk import dag, task          # klocki Airflow: @dag i @task
import json                                # zapis/odczyt plików JSON
from pathlib import Path                   # wygodna praca ze ścieżkami do plików
import pandas as pd                        # tabelki (DataFrame); "pd" to skrót

# Adres API – stała, bo używamy go w kilku miejscach
API_URL = "https://api.open-meteo.com/v1/forecast"



# --- Ustawienia DAG-a: "kiedy i jak uruchamiać" -------------------------------
@dag(
    start_date = datetime(2026,10,1),      # od kiedy DAG istnieje
    schedule = "@daily",                   # uruchamiaj raz dziennie
    catchup=False,                         # po włączeniu NIE nadrabiaj zaległych dni
    tags=["nauka"],                        # etykieta do filtrowania w UI
    default_args={                         # ustawienia dla WSZYSTKICH tasków
        "retries":3,                       # przy błędzie spróbuj jeszcze 3 razy
        "retry_delay":timedelta(minutes=2) # odczekaj 2 minuty między próbami
    }

)

def weather_daily():

    # =========================================================================
    # KROK 1: pobierz dane z API i zapisz je bez zmian (warstwa raw)
    # ds = dzień, za który przetwarzamy dane – Airflow wstawia go sam
    # =========================================================================
    @task
    def fetch_weather(ds=None):
        # Parametry zapytania: gdzie, co i za jaki dzień
        params= {
            "latitude": 52.23,             # Warszawa – szerokość geograficzna
            "longitude": 21.01,            # Warszawa – długość geograficzna
            "hourly": "temperature_2m",    # chcemy temperaturę co godzinę
            "start_date": ds,              # od dnia ds...
            "end_date": ds                 # ...do dnia ds = dokładnie jeden dzień
        }

        # Wyślij zapytanie; nie czekaj dłużej niż 30 sekund
        r = requests.get(API_URL,params=params, timeout=30)
        # Jeśli API zwróciło błąd (np. 500) – przerwij, Airflow zrobi retry
        r.raise_for_status()
        # Odpowiedź (tekst JSON) -> słownik Pythona
        data = r.json()

        # Wyciągnij listę temperatur: słownik "hourly" -> lista "temperature_2m"
        temps = data["hourly"]["temperature_2m"]
        print(f"Dzien {ds}: liczba pomiarów = {len(temps)}")   # print = linijka w logu
        print(f"Min {min(temps)}, Max {max(temps)}")

        # Adres pliku z partycją dt=<dzień> (jak partycje ds w Hive)
        path = Path(f"/usr/local/airflow/include/data/raw/weather/dt={ds}/weather.json")
        # Utwórz brakujące foldery; exist_ok=True = nie zgłaszaj błędu, jeśli już są
        path.parent.mkdir(parents=True, exist_ok=True)
        # "w" = write: NADPISZ plik (ponowny run nie dubluje danych)
        with open(path, "w") as f:
            json.dump(data, f)             # słownik -> plik JSON
        print(f"Zapisano: {path}")
        # Oddaj ŚCIEŻKĘ (nie same dane) – trafi przez XCom do kroku 2
        return str(path)

    # =========================================================================
    # KROK 2: wczytaj surowy JSON, zrób z niego tabelkę i zapisz jako Parquet
    # raw_path = ścieżka, którą oddał krok 1
    # =========================================================================
    @task
    def transform_weather(raw_path,ds=None):
        # Otwórz plik z kroku 1 i wczytaj go z powrotem do słownika
        # (json.load to odwrotność json.dump)
        with open(raw_path) as f:
            data = json.load(f)

        # Zbuduj tabelkę: nazwa kolumny -> lista wartości
        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"]
        })
        # Kolumna time: tekst "2026-10-31T13:00" -> prawdziwa data z godziną
        df["time"] = pd.to_datetime(df["time"])
        # Nowa kolumna dt: data dnia w każdym wierszu (wiadomo, z jakiej partycji jest wiersz)
        df["dt"] = ds

        # Adres pliku w warstwie processed, też z partycją dt=<dzień>
        path = Path(f"/usr/local/airflow/include/data/processed/weather/dt={ds}/weather.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        # Zapisz tabelkę do Parqueta (index=False = bez numerów wierszy pandas)
        df.to_parquet(path, index=False)
        print(f"Zapisano: {len(df)} wierszy do {path}")
        return str(path)

    # =========================================================================
    # ZŁOŻENIE PIPELINE'U
    # Najpierw fetch_weather(); jego wynik (ścieżka) wchodzi jako raw_path
    # do transform_weather. Airflow sam ustawia kolejność: krok 1 -> krok 2.
    # =========================================================================
    transform_weather(fetch_weather())


# Zbuduj DAG, żeby Airflow go zobaczył
weather_daily()

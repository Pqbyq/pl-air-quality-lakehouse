# Roadmap – PL Air Quality Lakehouse

Pytanie biznesowe: **gdzie i kiedy w Polsce jest smog, jak zależy od pogody i ilu mieszkańców dotyczy?**

Wszystko działa lokalnie (Docker + Astro). Przeniesienie do GCP (GCS + BigQuery) – opcjonalnie na końcu.

## Architektura

```
 GIOŚ (jakość powietrza) ─┐
 Open-Meteo (pogoda)      ├─► Airflow ──► raw (JSON) ──► processed (Parquet)
 GUS BDL (mieszkańcy)     ┘                                   │
                                                              ▼
                                            hurtownia DuckDB + dbt
                                     staging → marts (model gwiazdy, SCD2, testy)
                                                              │
                                                              ▼
                                         dashboard: smog vs pogoda w Polsce
```

## Źródła

| Źródło | Dane | Częstotliwość |
|---|---|---|
| GIOŚ | stacje, czujniki, pomiary PM10/PM2.5/NO₂, indeks jakości powietrza | co godzinę |
| Open-Meteo | pogoda dla lokalizacji stacji + archiwum | co godzinę / backfill |
| GUS BDL | liczba mieszkańców miast | raz w roku |

## Pipeline'y (DAG-i)

| DAG | Harmonogram | Co robi | Status |
|---|---|---|---|
| `weather_daily` | daily | pogoda dla Warszawy: raw → processed | ✔ (M0) |
| `gios_stations` | daily | lista stacji i czujników | M1 |
| `gios_measurements` | hourly | pomiary z każdego czujnika (dynamic task mapping) | M1 |
| `gios_aqi` | hourly | indeks jakości powietrza | M1 |
| `weather_hourly` | hourly | pogoda dla lokalizacji wszystkich stacji | M2 |
| `weather_archive_backfill` | ręcznie | historia pogody | M2 |
| `gus_population` | yearly | mieszkańcy miast | M2 |
| `dbt_warehouse` | po nowych danych (Assets) | budowa hurtowni | M3–M4 |
| `maintenance` | monthly | łączenie małych plików, retencja | M4 |

## Hurtownia

```
staging   stg_gios_stations · stg_gios_sensors · stg_gios_measurements · stg_gios_aqi
          stg_weather · stg_gus_population
marts     wymiary:  dim_station (SCD2) · dim_sensor · dim_pollutant · dim_city · dim_date · dim_hour
          fakty:    fct_air_measurements  (czujnik × godzina)
                    fct_weather_hourly    (lokalizacja × godzina)
                    fct_aqi_hourly        (stacja × godzina)
          agregaty: agg_city_daily · agg_smog_episodes
```

## Etapy

- [x] **M0** – pogoda dla jednego miasta: raw → processed, retry, backfill
- [ ] **M1** – GIOŚ: stacje, czujniki, pomiary, indeks (3 DAG-i)
- [ ] **M2** – pogoda dla wszystkich stacji, archiwum, GUS (3 DAG-i)
- [ ] **M3** – hurtownia dbt: staging, wymiary, fakty, SCD2, testy
- [ ] **M4** – orkiestracja całości (Assets), maintenance, monitoring świeżości
- [ ] **M5** – dashboard, testy pytest, CI (GitHub Actions), README z decyzjami projektowymi

## Zasady

- jeden run = jedna partycja (`ds` / godzina) → idempotencja, backfill bez duplikatów
- raw zawsze bez zmian – transformacje można przeliczyć od nowa
- każdy krok kończy się commitem

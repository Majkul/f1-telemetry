# F1 Race Strategy Platform

## Overview
An end-to-end data engineering and machine learning project built around Formula 1 telemetry data.
The goal is to collect, process, and store granular race data from the 2019–2025 seasons,
and use it to build a model capable of recommending optimal race strategies in real time.

## Data Pipeline
Raw data is sourced from the FastF1 API and processed through an Apache Airflow-orchestrated pipeline:

- **Extract** — FastF1 pulls session data (races, qualifying, sprints, practice) for every round across 7 seasons
- **Transform** — PySpark jobs clean and reshape raw parquets into a dimensional model
- **Load** — transformed data is written to PostgreSQL/Google BigQuery

## Data Model
The schema follows a star schema design with the following layers:

**Dimension tables** — slowly changing reference data: `dim_driver`, `dim_team`, `dim_circuit`,
`dim_session`, `dim_compound`, `dim_event`, `dim_season`, `dim_driver_season`

**Fact tables** — granular event data: `fact_lap`, `fact_stint`, `fact_pit_stop`,
`fact_result`, `fact_weather`, `fact_telemetry`

## Tech Stack
- **Orchestration** — Apache Airflow
- **Processing** — Apache Spark (PySpark)
- **Storage** — PostgreSQL, Google BigQuery
- **Data source** — FastF1 Python API

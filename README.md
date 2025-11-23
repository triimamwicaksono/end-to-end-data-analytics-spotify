# Spotify Throwback: End-to-End Modern Data Pipeline

This project is a hands-on exploration of the modern data stack, built to analyze my personal Spotify Top 20 tracks. It implements real-world data engineering patterns including ingestion, orchestration, warehousing, transformation, CI/CD, and dashboarding.

The goal was to use my Spotify Throwback data and convert it into a complete production-style pipeline.

---

## Architecture Overview

```
Spotify API → Kafka → MinIO (S3) → Airflow → Snowflake (RAW)
        → dbt (Bronze/Silver/Gold) → Looker Studio
```

Key practices included:

* Containerized components using Docker
* Scheduled ingestion with Airflow
* Medallion architecture using dbt
* Data quality tests and documentation
* CI/CD automation using GitHub Actions
* Visualization using Looker Studio

---

## 1. Data Ingestion

### Spotify API

Fetched personal Top 20 track data.

### Kafka

Sent track messages to a Kafka topic (`spotify_top_tracks`).

### MinIO

A Kafka consumer wrote each batch of track data into MinIO as JSON files.

### Docker

All ingestion components were containerized for portability and reproducibility.

---

## 2. Orchestration with Apache Airflow

Airflow handled:

* Checking for new files in MinIO
* Loading JSON files into Snowflake RAW tables
* Ensuring idempotent data loading
* Scheduling ingestion workflows

DAG used: `minio_to_snowflake_raw_data`

---

## 3. Data Modeling in Snowflake with dbt

The medallion architecture was implemented:

### Bronze Layer

Raw track-level data including metadata such as file names and load timestamps.

### Silver Layer

Cleaned dimension and fact models:

* `dim_artist`
* `dim_track`
* `fact_top_tracks`

### Gold Layer

Aggregated analytical models:

* Artist summary
* Album summary
* Track leaderboard

### dbt Features Used

* Sources and schema definitions
* Tests (unique, not_null, relationships)
* Documentation
* Centralized materializations using `dbt_project.yml`
* CI-friendly structure

To run transformations:

```
dbt deps
dbt run
dbt test
```

---

## 4. CI/CD with GitHub Actions

A GitHub Actions workflow automates:

* Installing dependencies
* Running `dbt deps`, `dbt run`, and `dbt test`
* Validating Snowflake models for every pull request

Snowflake credentials are stored as GitHub Secrets:

* SNOWFLAKE_ACCOUNT
* SNOWFLAKE_USER
* SNOWFLAKE_PASSWORD
* SNOWFLAKE_ROLE
* SNOWFLAKE_WAREHOUSE
* SNOWFLAKE_DATABASE
* SNOWFLAKE_SCHEMA

This ensures reliability and repeatability of all dbt operations.

---

## 5. Visualization in Looker Studio

The Gold layer was connected to Looker Studio to create simple analytical views, such as:

* Top tracks by popularity
* Top artists
* Track duration distribution
* Personal leaderboard

The results highlighted several interesting personal patterns from my Spotify Throwback.

---


## Running the Project Locally

### Clone the repository

```
git clone <repo-url>
cd <repo-folder>
```

### Start Docker environment

```
docker-compose up -d
```

### Access services

* Airflow: [http://localhost:8080](http://localhost:8080)
* MinIO: [http://localhost:9001](http://localhost:9001)

### Run dbt transformations

```
cd dbt
dbt deps
dbt run
dbt test
```

---

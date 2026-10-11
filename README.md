# NYC Restaurant Inspections & Compliance Analytics

An end-to-end data engineering portfolio project that turns New York City restaurant inspection data into an analytics-ready dimensional model and dashboards. The platform combines API ingestion, Avro serialization, Kafka, batch PySpark processing, PostgreSQL, dbt, and Apache Superset in a Docker Compose environment.

**Repository:** https://github.com/Mehdi-slk/restaurant-analytics

## Project goals

- Ingest public inspection data from a real operational source.
- Move records through a schema-aware Kafka pipeline.
- Process Kafka data in batch with PySpark and preserve Kafka partition/offset lineage.
- Load a PostgreSQL staging layer and transform it with dbt.
- Model inspections and violations separately so the different business grains remain explicit.
- Test data quality and expose inspection trends, violation patterns, and pipeline/model health through Superset.

## Architecture

**NYC Open Data API → Python producer → Avro / Schema Registry → Kafka (3 KRaft brokers) → PySpark batch job → PostgreSQL staging → dbt dimensional marts → Apache Superset dashboards**

The Spark job reads Kafka using Spark's batch Kafka source (`spark.read.format("kafka")`); it is not a continuously running Structured Streaming job. Kafka offsets are tracked in a JSON file so batches can be resumed, and the staging table stores the Kafka partition and offset for record lineage and deduplication.

## Technology stack

| Area | Technologies |
|---|---|
| Source | NYC Open Data, Socrata API |
| Ingestion and serialization | Python, Avro, Confluent Schema Registry |
| Message transport | Apache Kafka 4.x, KRaft, three brokers |
| Processing | Apache Spark / PySpark 4.1.x, batch processing |
| Warehouse | PostgreSQL 16 |
| Transformation and modeling | dbt Core, dbt-postgres, dbt-utils |
| Orchestration | Apache Airflow 3.0.x |
| BI and data-quality reporting | Apache Superset 6.0.x |
| Local environment | Docker Compose |

## Source dataset

The project uses the **DOHMH New York City Restaurant Inspection Results** dataset, maintained by NYC Open Data and the New York City Department of Health and Mental Hygiene.

- [Dataset page](https://data.cityofnewyork.us/d/43nn-pn8j)
- [Socrata API documentation](https://dev.socrata.com/foundry/data.cityofnewyork.us/43nn-pn8j)
- API endpoint used by the project: `https://data.cityofnewyork.us/resource/43nn-pn8j.json`

The source is intentionally not treated as perfectly clean. Its rows can repeat inspection-level attributes when an inspection has multiple violation records, contain missing or inconsistent attributes, and include establishments that have not yet received an inspection. The transformations preserve those distinctions instead of blindly treating every source row as a separate inspection.

## Data model

The dbt marts use a star-schema approach with separate grains for inspection events and violations.

### Fact tables

- **`fact_inspection`** — one row per inspection. Contains inspection score, inspection date and grade date keys, and links to restaurant, location, and inspection-profile dimensions.
- **`fact_violation`** — one row per inspection/violation combination. Contains a violation count and links to the relevant restaurant, location, date, and violation dimensions. Inspections without a recorded violation do not receive fabricated violation rows.

### Dimensions

- **`dim_date`** — calendar attributes for date-based analysis, including an explicit unknown-date member.
- **`dim_restaurant`** — restaurant business identity and descriptive attributes. The dbt snapshot tracks changes to restaurant name and cuisine as **SCD Type 2** versions; phone number is handled as a Type 1 attribute.
- **`dim_location`** — restaurant address and NYC geographic attributes, including borough, ZIP code, community board, council district, census tract, BIN, BBL, and NTA.
- **`dim_violation`** — violation code, description, and criticality.
- **`dim_inspection_profile`** — inspection type, action, and grade.

The model uses explicit unknown/not-graded handling where appropriate, keeps a missing score as missing, and does not interpret a score of `0` as a null. Rows whose source inspection date is the `1900-01-01` placeholder are treated as establishments not yet inspected, rather than as real dated inspection events.

## Data quality and engineering practices

- **Schema-aware ingestion:** records are serialized with Avro and validated through Schema Registry.
- **Replay-aware batch processing:** Kafka partition/offset lineage is retained in PostgreSQL staging; an offset file tracks batch progress.
- **Duplicate protection:** Kafka partition and offset identify a source message for staging deduplication.
- **Layered transformation:** dbt separates staging cleanup, intermediate recovery/business logic, and dimensional marts.
- **SCD Type 2 history:** restaurant name and cuisine changes can be analyzed over time without overwriting older versions.
- **Data-quality tests and warnings:** dbt checks model constraints and selected formatting/source anomalies, including suspicious identifiers and ZIP codes outside expected NYC ranges.
- **Integrity dashboard:** unknown keys, null scores, location completeness, and other model-quality indicators are exposed for investigation.

## Dashboards

The Superset setup contains two dashboards:

### NYC Restaurant Inspections

Includes inspection volume, average inspection score, score trends by borough, borough comparisons, grade distribution, inspection-grade breakdowns, score distribution, and restaurants with repeated inspections and high average scores.

### Data Quality and Pipeline Health

Includes location completeness, unknown geographic attributes, non-NYC ZIP-code source anomalies, restaurant dimension version counts, and key integrity checks for the fact model.

A separate violation analysis uses the violation dataset to explore critical violations, the most frequent violation descriptions, and violation patterns by cuisine and borough.

## Run locally

### Requirements

- Docker Desktop or Docker Engine with the Docker Compose plugin
- Git
- Sufficient memory and disk for Kafka, Spark, Airflow, PostgreSQL, dbt, and Superset to run together

### Start the services

```bash
git clone https://github.com/Mehdi-slk/restaurant-analytics.git
cd restaurant-analytics
docker compose up -d --build
docker compose ps
```

Inspect logs if a service does not become healthy:

```bash
docker compose logs --tail=200 <service-name>
```

Use the ports configured in `docker-compose.yml` for the web interfaces. In the current local setup, the intended interfaces include Airflow on `http://localhost:8081`, Kafka UI on `http://localhost:8084`, pgAdmin on `http://localhost:8082`, Superset on `http://localhost:8088`, and the Spark master UI on `http://localhost:8080`.

### Run the Spark batch job

After the Kafka topic, Schema Registry, PostgreSQL, and Spark services are running and the required source data is available in Kafka, run the batch job from the Spark driver container:

```bash
docker exec -it spark-driver \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/scripts/restaurant_data.py
```

The job reads `restaurant_topic`, uses the Avro schema mounted at `/opt/spark/schemas/restaurants.avsc`, writes to the PostgreSQL staging table, and updates its offset checkpoint file at `/opt/spark/scripts/offsets.json`.

### Run dbt

The dbt project is located in `dbt/restaurant_analytics` inside the repository and is mounted in the dbt container at `/usr/app/restaurant_analytics`.

```bash
docker compose run --rm dbt dbt deps --project-dir /usr/app/restaurant_analytics
docker compose run --rm dbt dbt build --project-dir /usr/app/restaurant_analytics
```

`dbt build` runs the configured models and tests. A source anomaly warning (such as an unexpected ZIP code) is not automatically proof that a source row should be deleted; investigate it against the published source data first.

## Configuration and credentials

This repository is designed for local development. Review `docker-compose.yml` and any mounted configuration files before running it in another environment. Replace development/default credentials and secret values before exposing any service beyond your local machine. Do not commit real API tokens, passwords, `.env` files, generated data, or local logs.

## Limitations

- The source dataset is maintained externally and can change over time; counts and dashboard values will therefore change between runs.
- The source contains missing and occasionally inconsistent values. Data-quality warnings deliberately surface anomalies for review instead of claiming that every source value can be recovered.
- The pipeline and dashboards are a portfolio demonstration of batch ingestion and analytics. They are not a production deployment with managed secrets, monitoring/alerting, high-availability storage, or a formally defined data refresh SLA.

## License

The project code is released under the MIT License. See [`LICENSE`](LICENSE). The source dataset remains subject to the terms and conditions published by NYC Open Data; see the [dataset page](https://data.cityofnewyork.us/d/43nn-pn8j).

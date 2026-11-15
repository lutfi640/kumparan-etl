# kumparan-etl

## Overview

An ETL pipeline that moves article data from a Postgres source database into a data
warehouse, orchestrated by Airflow and run every hour.

The warehouse follows the medallion architecture. Data moves through three layers, and
each layer is its own schema with one job:

| Layer            | Schema     | Contents                                                                                       |
| ---------------- | ---------- | ---------------------------------------------------------------------------------------------- |
| **Bronze** | `bronze` | Raw copy of the source tables, plus the delete events read from the source's replication slot. |
| **Silver** | `silver` | Cleaned dimension and fact tables.                                                             |
| **Gold**   | `gold`   | Materialized views for analytics.                                                              |

The warehouse is a Postgres database, not a dedicated data warehouse such as BigQuery
or Snowflake. Postgres was chosen to keep development simple: the whole stack runs
locally with Docker Compose, with no cloud account or credentials to set up.

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project structure](#project-structure)
- [How to run](#how-to-run)
- [Pipeline](#pipeline)
- [Bonus answers](#bonus-answers)
- [Limitations](#limitations)
- [AI usage](#ai-usage)

## Architecture

Airflow runs every step below. The numbers on the arrows match the table under the
diagram.

```mermaid
flowchart LR
    subgraph SRC["pg-source · kumparan_source"]
        T[("<b style='color:#000000'>public</b><br/>5 source tables")]
        SLOT{{"<b style='color:#000000'>kumparan_cdc</b><br/>replication slot"}}
    end

    subgraph DWH["pg-dwh · kumparan_dwh"]
        B[("<b style='color:#000000'>bronze</b><br/>raw tables")]
        C[("<b style='color:#000000'>bronze</b><br/>cdc_articles")]
        S[("<b style='color:#000000'>silver</b><br/>4 dims, 2 facts")]
        G[("<b style='color:#000000'>gold</b><br/>2 materialized views")]
    end

    T -->|"1 · load"| B
    SLOT -->|"2 · capture deletes"| C
    B -->|"3 · merge"| S
    C -->|"4 · mark deleted"| S
    S -->|"5 · refresh"| G

    classDef source fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#000000
    classDef bronze fill:#f3d5b5,stroke:#b45309,stroke-width:2px,color:#000000
    classDef silver fill:#e5e7eb,stroke:#6b7280,stroke-width:2px,color:#000000
    classDef gold fill:#fde68a,stroke:#ca8a04,stroke-width:2px,color:#000000

    class T,SLOT source
    class B,C bronze
    class S silver
    class G gold

    style SRC fill:transparent,stroke:#2563eb,stroke-dasharray:4 4
    style DWH fill:transparent,stroke:#6b7280,stroke-dasharray:4 4
```

| No  | Step            | From → to                                  | How                                                                              |
| --- | --------------- | ------------------------------------------- | -------------------------------------------------------------------------------- |
| 1   | Load            | source tables → `bronze`                 | Delete-insert on the last 2 days. Full load on the initial run.                  |
| 2   | Capture deletes | replication slot → `bronze.cdc_articles` | Peek the slot (`test_decoding`), save DELETE events, commit, advance the slot. |
| 3   | Merge           | `bronze` → `silver`                    | `MERGE` (upsert) into the dimension and fact tables.                           |
| 4   | Mark deleted    | `bronze.cdc_articles` → `silver`       | Set `is_deleted = true` on the deleted articles.                               |
| 5   | Refresh         | `silver` → `gold`                      | `REFRESH MATERIALIZED VIEW`.                                                   |

| Layer  | Tables                                                                                                              |
| ------ | ------------------------------------------------------------------------------------------------------------------- |
| Source | `articles`, `authors`, `categories`, `article_categories`, `article_views`                                |
| Silver | `dim_author`, `dim_category`, `dim_article`, `dim_date`, `fact_article_lifecycle`, `fact_article_views` |
| Gold   | `mv_monthly_publishing`, `mv_author_productivity`                                                               |

| Container          | Image                                | Host port   | Role                                                                      |
| ------------------ | ------------------------------------ | ----------- | ------------------------------------------------------------------------- |
| `pg-source`      | `postgres:16`                      | 5433        | Source database `kumparan_source`, runs with `wal_level=logical`.     |
| `pg-dwh`         | `postgres:16`                      | 5434        | Warehouse `kumparan_dwh` with schemas `bronze`, `silver`, `gold`. |
| `airflow`        | `apache/airflow:2.10.5-python3.11` | 8080        | Airflow in `standalone` mode with `LocalExecutor`.                    |
| `airflow-metadb` | `postgres:16`                      | not exposed | Airflow metadata database.                                                |

## Project structure

```
kumparan-etl/
├── docker-compose.yaml
├── dags/                                  # mounted into the Airflow container
│   ├── dwh_initial_load.py                # full load, triggered manually
│   ├── dwh_hourly.py                      # incremental load + CDC, every hour
│   ├── source_simulator.py                # writes test changes to the source
│   └── scripts/
│       ├── bronze/                        # Python loaders, one per table
│       └── silver/                        # MERGE statements, one per table
└── scripts/                               # run manually, once, during setup
    ├── create_fake_source.sql             # DDL: source tables
    ├── create_table_dwh_bronzesilver.sql  # DDL: warehouse schemas, bronze and silver tables
    ├── fill_dim_date.sql                  # fills silver.dim_date (2016 to 2030)
    ├── create_gold_mv.sql                 # DDL: gold materialized views
    ├── create_replication_slot_cdc.sql    # notes used to create and test the CDC slot
    ├── source_data_faker.ipynb            # seeds the source with fake data
    └── simulate_changes.py                # same logic as source_simulator, run from the host
```

## How to run

Requirements: Docker with Docker Compose. Python with `faker` and `psycopg2` is needed
only to seed the source data from the notebook.

The commands below use `<` to send a SQL file to `psql`. On PowerShell, use
`Get-Content <file> | docker exec -i ...` instead.

1. Start the containers.

   ```bash
   docker compose up -d
   ```
2. Create the source tables.

   ```bash
   docker exec -i pg-source psql -U source_user -d kumparan_source < scripts/create_fake_source.sql
   ```
3. Create the warehouse schemas and the bronze and silver tables.

   ```bash
   docker exec -i pg-dwh psql -U dwh_user -d kumparan_dwh < scripts/create_table_dwh_bronzesilver.sql
   ```
4. Fill `silver.dim_date` and create the gold materialized views.

   ```bash
   docker exec -i pg-dwh psql -U dwh_user -d kumparan_dwh < scripts/fill_dim_date.sql
   docker exec -i pg-dwh psql -U dwh_user -d kumparan_dwh < scripts/create_gold_mv.sql
   ```
5. Seed the source by running all cells in `scripts/source_data_faker.ipynb`
   (1,000 articles dated 2016 to 2023).
6. Create the replication slot on the source.

   ```bash
   docker exec pg-source psql -U source_user -d kumparan_source -c "SELECT pg_create_logical_replication_slot('kumparan_cdc', 'test_decoding');"
   ```

   `scripts/create_replication_slot_cdc.sql` holds the same statement together with the
   queries used to test the slot by hand. Run those one at a time, not as a whole file.
7. Create the two Airflow connections.

   ```bash
   docker exec airflow airflow connections add postgres_source --conn-uri "postgres://source_user:source_pass@pg-source:5432/kumparan_source"
   docker exec airflow airflow connections add postgres_dwh --conn-uri "postgres://dwh_user:dwh_pass@pg-dwh:5432/kumparan_dwh"
   ```
8. Run the initial load once, then turn on the hourly DAG. The Airflow UI is at
   http://localhost:8080.

   ```bash
   docker exec airflow airflow dags unpause dwh_initial_load
   docker exec airflow airflow dags trigger dwh_initial_load
   docker exec airflow airflow dags unpause dwh_hourly
   ```
9. Optional: generate test changes in the source (inserts, updates, soft deletes,
   hard deletes, views).

   ```bash
   docker exec airflow airflow dags unpause source_simulator
   ```

   The same logic can be run from the host with `python scripts/simulate_changes.py`.

## Pipeline

| DAG                  | Schedule              | Purpose                                                                          |
| -------------------- | --------------------- | -------------------------------------------------------------------------------- |
| `dwh_initial_load` | none (manual trigger) | Full load of all source tables into bronze, then silver merges and gold refresh. |
| `dwh_hourly`       | `@hourly`           | Incremental bronze load, CDC read, silver merges, gold refresh.                  |
| `source_simulator` | `30 * * * *`        | Writes random changes to the source so the pipeline can be tested.               |

In both warehouse DAGs the tasks are grouped into three task groups, which run in this
order:

| Task group       | Tasks                                                                                                                                                      |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bronze_layer` | `authors`, `categories`, `article_categories` (in parallel), then `articles`, `cdc_articles`, `article_views`                                  |
| `silver_layer` | `merge_dim_author`, `merge_dim_category`, `merge_dim_article`, `merge_fact_article_lifecycle`, `merge_fact_article_views`, `apply_cdc_deletes` |
| `gold_layer`   | `refresh_mv_monthly_publishing`, `refresh_mv_author_productivity` (in parallel)                                                                        |

`dwh_initial_load` has the same tasks without `cdc_articles`.

### Bronze

Code: `dags/scripts/bronze/`.

- Each loader takes a `days` argument. With the default `days=2` it deletes the bronze
  rows from the last 2 days and inserts the same range from the source. The range is
  based on `updated_at` (plus `deleted_at` for articles) or `viewed_at` for views.
- With `days=None` (initial load) it truncates the table and loads everything.
- `article_categories` has no timestamp, so it is fully reloaded on every run.

### CDC

Code: `dags/scripts/bronze/load_cdc_articles.py`.

1. Read the `kumparan_cdc` slot with `pg_logical_slot_peek_changes`, which does not
   consume the changes.
2. Keep only the `DELETE` rows of `public.articles`.
3. Insert them into `bronze.cdc_articles` with `ON CONFLICT (lsn) DO NOTHING` and commit.
4. Only then call `pg_replication_slot_advance`. A failed run leaves the slot unchanged,
   so the next run reads the same changes again.

### Silver

Code: `dags/scripts/silver/`.

- The dimension tables and `fact_article_lifecycle` use `MERGE` keyed on the source id,
  taking the latest bronze version of each row, so reruns give the same result.
- `fact_article_views` uses `INSERT ... ON CONFLICT (view_id) DO NOTHING`.
- `apply_cdc_deletes.sql` sets `is_deleted = true` on `dim_article` and
  `fact_article_lifecycle` for hard-deleted articles. The merges never set it back to
  false.

### Gold

`gold.mv_monthly_publishing` and `gold.mv_author_productivity` are refreshed with
`REFRESH MATERIALIZED VIEW` as the last tasks of the DAG.

| View                       | Answers                                                                                                       |
| -------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `mv_monthly_publishing`  | How many articles were published per month and category, with average time to publish and average word count. |
| `mv_author_productivity` | Which authors publish the most and are read the most.                                                         |

## Bonus answers

### Data since 2016

Done in this project:

- History is loaded once by a separate DAG (`dwh_initial_load`), so the hourly DAG only
  reads the last 2 days of changes.
- Loads are delete-insert in bronze and upsert in silver, so both DAGs can be rerun
  without creating duplicates.
- `silver.dim_date` covers 2016-01-01 to 2030-12-31.

### Hard delete

- A hard-deleted row no longer exists in the source, so a query on `updated_at` cannot
  see it. Only the database log still records the delete.
- The source runs with `wal_level=logical`. A logical replication slot (`kumparan_cdc`,
  plugin `test_decoding`) keeps the changes until the pipeline reads them.
- Each hourly run stores the `DELETE` events in `bronze.cdc_articles`, then flags the
  matching silver rows with `is_deleted = true`. Rows are flagged, not removed, and the
  gold views exclude them.

## Limitations

- Setup is manual: the DDL files, the Airflow connections and the replication slot are
  applied by hand (steps 2 to 7). There are no init scripts that run on container start.
- The incremental range is a fixed `now() - 2 days`, not the DAG run interval. If the
  pipeline is down for more than 2 days, `dwh_initial_load` must be run again.
- Hard deletes are captured only for `articles`. Deletes in other tables are not detected.
- The CDC task reads all pending slot changes into memory and parses the
  `test_decoding` text output with a regex. If the DAG stops, WAL grows on the source.
- `dim_article` keeps one category per article, and each silver merge scans the whole
  bronze table.

## AI usage

- Google Gemini was used to predict what kumparan's business data looks like and to
  decide the table structure.
- Google Gemini was used to understand logical replication slots, a built-in Postgres
  feature that I use for CDC, so I could write the SQL and the Python script. Gemini
  also checked the code that I wrote.
- Claude Code was used to create `simulate_changes.py` and the `source_simulator` DAG,
  so I could create test data changes quickly.
- Claude Code was also used to create the date dimension (`silver.dim_date`). This is a
  standard table in most companies and makes dates easier to maintain.
- Claude Code was also used to create `docker-compose.yaml` to start the containers.
- Claude Code was also used to generate this Readme.md
- Microsoft Copilot was used to write code faster because it gives code suggestions.
  Sometimes the suggestions were wrong, so I had to fix them myself.

# YouTube Channel Analytics ELT Pipeline — Full Project Description

**Author:** Safwen Cherif  
**Repository:** [ETL-Data-engineering-and-warehousing](https://github.com/SafwenCherif/ETL-Data-engineering-and-warehousing)  
**Document purpose:** Complete technical report of the project architecture, every file and function, end-to-end data flow, technology choices, and a reproducible setup guide for anyone cloning the repository.

---

## Table of contents

1. [Executive summary](#1-executive-summary)
2. [Problem statement and goals](#2-problem-statement-and-goals)
3. [Technology stack and why each tool exists](#3-technology-stack-and-why-each-tool-exists)
4. [High-level architecture](#4-high-level-architecture)
5. [Repository structure](#5-repository-structure)
6. [Journey from zero to production-ready pipeline](#6-journey-from-zero-to-production-ready-pipeline)
7. [YouTube Data API design (Russian-doll extraction)](#7-youtube-data-api-design-russian-doll-extraction)
8. [Configuration and secrets (.env)](#8-configuration-and-secrets-env)
9. [Containerization](#9-containerization)
10. [PostgreSQL data warehouse design](#10-postgresql-data-warehouse-design)
11. [Airflow orchestration](#11-airflow-orchestration)
12. [File-by-file and function-by-function reference](#12-file-by-file-and-function-by-function-reference)
13. [Data quality with Soda Core](#13-data-quality-with-soda-core)
14. [Testing strategy](#14-testing-strategy)
15. [CI/CD with GitHub Actions](#15-cicd-with-github-actions)
16. [End-to-end runtime flow](#16-end-to-end-runtime-flow)
17. [How to clone and run this project](#17-how-to-clone-and-run-this-project)
18. [Day-2 operations cheat sheet](#18-day-2-operations-cheat-sheet)
19. [Troubleshooting](#19-troubleshooting)
20. [Security notes](#20-security-notes)
21. [Possible extensions](#21-possible-extensions)
22. [Glossary](#22-glossary)
- [Appendix A — Default ports](#appendix-a--default-ports)
- [Appendix B — Image and runtime versions](#appendix-b--image-and-runtime-versions-used-in-this-project)
- [Appendix C — Minimal success checklist](#appendix-c--minimal-success-checklist-for-newcomers)
- [Appendix D — Design rationale summary](#appendix-d--design-rationale-summary-what--why-compressed)
- [Appendix E — docker-compose.yaml deep dive](#appendix-e--docker-composeyaml-deep-dive-service-by-service)
- [Appendix F — Airflow Variable and Connection contract](#appendix-f--airflow-variable-and-connection-contract)
- [Appendix G — Data dictionary and mapping](#appendix-g--data-dictionary-and-mapping-from-api-to-warehouse)
- [Appendix H — Example analytical SQL](#appendix-h--example-analytical-sql-after-a-successful-run)
- [Appendix I — Exact first-day command script](#appendix-i--exact-first-day-command-script-copypaste)
- [Appendix J — Channel portability](#appendix-j--channel-portability)
- [Appendix K — Quota and operational cost awareness](#appendix-k--quota-and-operational-cost-awareness)
- [Appendix L — What each major dependency contributes at runtime](#appendix-l--what-each-major-dependency-contributes-at-runtime)
- [Appendix M — Integrity guarantees provided by tests](#appendix-m--integrity-guarantees-provided-by-tests)
- [Appendix N — CI change-detection philosophy](#appendix-n--ci-change-detection-philosophy)
- [Appendix O — Local vs CI differences](#appendix-o--local-vs-ci-differences)
- [Appendix P — Recommended demo script for portfolio walkthrough](#appendix-p--recommended-demo-script-for-portfolio-walkthrough)
- [Appendix Q — File responsibility matrix](#appendix-q--file-responsibility-matrix)
- [Appendix R — Known simplifications](#appendix-r--known-simplifications-honest-scope-boundary)
- [Appendix S — Recovery playbooks](#appendix-s--recovery-playbooks)
- [Appendix T — Final mental model](#appendix-t--final-mental-model)
- [Appendix U — Contributor onboarding FAQ](#appendix-u--contributor-onboarding-faq)
- [Appendix V — Document maintenance](#appendix-v--document-maintenance)
- [Appendix W — Quick reference card](#appendix-w--quick-reference-card)

---

## 1. Executive summary

This project is a full **ELT (Extract → Load → Transform)** data engineering pipeline that:

- **Extracts** public video metadata and engagement statistics for a YouTube channel (default: **MrBeast**) via the **YouTube Data API v3**
- **Lands** raw/near-raw records as dated JSON files under `./data/`
- **Loads** them into a **PostgreSQL** warehouse with two layers:
  - **`staging`**: close to source shape
  - **`core`**: cleaned types + business attribute `Video_Type` (**Shorts** vs **Normal**)
- **Orchestrates** everything with **Apache Airflow 2.9.2** running under **CeleryExecutor** inside **Docker Compose**
- **Validates** warehouse data with **Soda Core** quality checks
- **Protects** quality with **pytest** unit + integration tests
- **Automates** build/test via **GitHub Actions** and publishes a custom Airflow image to **Docker Hub**

The pipeline is designed so a successful run of **`produce_json`** automatically triggers **`update_db`**, which then triggers **`data_quality`**. That chaining turns a manual analytics pull into a repeatable, testable data product.

### Key metrics at a glance

| Attribute | Value |
|-----------|-------|
| Airflow version | 2.9.2-python3.10 |
| Executor | CeleryExecutor |
| Postgres version | 13 |
| Redis | Celery broker |
| DAG count | 3 |
| Warehouse DB | `elt_db` |
| Connection ID | `postgres_db_yt_elt` |
| Default schedule | `0 14 * * *` (14:00 daily) |
| Image reference | `${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:latest` |

---

## 2. Problem statement and goals

### 2.1 What problem does this solve?

YouTube channel owners, analysts, and data teams often need historical and recurring snapshots of:

- Which videos exist on a channel
- When they were published
- How long they are
- How performance metrics evolve (views, likes, comments)

Doing this manually in the browser does not scale. Calling the API ad hoc without orchestration, storage design, quality checks, and CI leads to brittle notebooks.

### 2.2 Project goals

| Goal | How this project addresses it |
|------|-------------------------------|
| Reliable extraction | Paginated + batched YouTube API calls |
| Durable landing zone | Dated JSON under `./data/` |
| Warehouse modeling | Staging (raw-ish) + Core (enriched) |
| Upserts over time | Insert / update / soft-delete reconciliation |
| Orchestration | Airflow DAGs with explicit dependencies |
| Data quality | Soda scans on staging and core |
| Developer confidence | Unit + integration tests |
| Reproducibility | Docker image + Compose + `.env` |
| Automation | GitHub Actions CI/CD |

### 2.3 Non-goals

- **Not** a real-time streaming system
- **Not** a multi-channel product UI
- **Not** a production-hardened multi-tenant SaaS
- Local Compose setup is for **development / portfolio / learning-grade** operations

---

## 3. Technology stack and why each tool exists

| Technology | Role in this project | Why chosen |
|------------|---------------------|------------|
| Python 3.10 / 3.12 (local) | Extract scripts, DAGs, tests | Ecosystem for APIs + Airflow + data tooling |
| YouTube Data API v3 | Source system | Official public metadata/stats API |
| Google Cloud API key | Auth for public data | Simple for read-only public resources |
| Docker | Package runtime | Same environment everywhere |
| Docker Compose | Multi-service local/CI runtime | Postgres + Redis + Airflow services together |
| Apache Airflow 2.9.2 | Orchestration | Scheduling, retries, UI, task graph, triggers |
| CeleryExecutor + Redis | Distributed task execution | Matches real Airflow deployments better than SequentialExecutor |
| PostgreSQL 13 | Metadata DB + Celery backend + ELT warehouse | One engine, three logical databases |
| Soda Core (postgres) | Data quality scans | Declarative checks against warehouse tables |
| pytest | Automated tests | Unit mocks + live integration checks |
| GitHub Actions | CI/CD | Build image, run Compose tests on push |
| Docker Hub | Image registry | Share/reuse custom Airflow image |
| DBeaver (optional) | SQL exploration | Inspect staging / core interactively |
| Postman (optional early) | API smoke tests | Validate key + endpoints before coding |

---

## 4. High-level architecture

```mermaid
flowchart TB
    subgraph Source["Source"]
        YT[YouTube Data API v3]
    end

    subgraph Orchestration["Docker Compose / Airflow"]
        Web[airflow-webserver :8080]
        Sch[airflow-scheduler]
        Wkr[airflow-worker Celery]
        Init[airflow-init]
        Redis[redis broker]
    end

    subgraph Storage["PostgreSQL container"]
        Meta[(airflow_metadata_db)]
        CeleryDB[(celery_results_db)]
        ELT[(elt_db)]
        Staging[(schema staging.yt_api)]
        Core[(schema core.yt_api)]
        ELT --> Staging
        ELT --> Core
    end

    subgraph Landing["Host volume"]
        JSON[./data/YT_data_YYYY-MM-DD.json]
    end

    subgraph Quality["Data quality"]
        Soda[Soda Core checks.yml]
    end

    YT -->|produce_json tasks| Wkr
    Wkr --> JSON
    JSON -->|update_db staging| Staging
    Staging -->|transform + load| Core
    Core --> Soda
    Staging --> Soda
    Sch --> Redis
    Redis --> Wkr
    Wkr --> Meta
    Wkr --> CeleryDB
    Web --> Meta
```

### Architectural principles used

- **Separate concerns:** extract code, warehouse code, DQ code, orchestration wiring
- **Medallion-style layers:** staging then core (not full bronze/silver/gold naming, same idea)
- **Idempotent-ish loads:** insert new IDs, update known IDs, delete IDs removed from source snapshot
- **Config outside code:** credentials and handles via environment / Airflow Variables
- **Test before trust:** local pytest + CI e2e DAG tests

---

## 5. Repository structure

```
.
├── .github/workflows/ci-cd_yt-elt.yaml   # GitHub Actions pipeline
├── .gitignore
├── dockerfile                           # Custom Airflow image
├── docker-compose.yaml                  # Full local/CI stack
├── requirements.txt                     # Extra Python deps baked into image
├── .env                                 # Local secrets (NOT committed)
├── docker/postgres/init-multiple-databases.sh
├── dags/
│   ├── main.py                          # Defines 3 DAGs + chaining
│   ├── api/
│   │   └── video_stats.py               # Extract + land JSON
│   ├── datawarehouse/
│   │   ├── data_utils.py                # Connections, DDL helpers
│   │   ├── data_loading.py              # Read today's JSON
│   │   ├── data_modification.py         # INSERT/UPDATE/DELETE
│   │   ├── data_transformation.py       # Duration parse + Video_Type
│   │   └── dwh.py                       # Airflow tasks staging/core
│   └── dataquality/
│       └── soda.py                      # BashOperator wrappers for Soda
├── include/soda/
│   ├── configuration.yml                # Soda datasource connection
│   └── checks.yml                       # Quality rules for yt_api
├── tests/
│   ├── conftest.py                      # Shared fixtures
│   ├── unit_test.py                     # Mocked Airflow var/conn + DAG integrity
│   └── integration_test.py              # Live YouTube + live Postgres
├── data/                                # JSON landing zone (gitignored)
├── logs/                                # Airflow logs (gitignored)
├── config/ plugins/                     # Airflow mount dirs
├── video_stats.md                       # Optional deep-dive notes
└── README.md                            # This document
```

### What is intentionally not committed

| Path | Reason |
|------|--------|
| `.env` | Secrets |
| `venv/` | Local virtualenv |
| `logs/` | Runtime noise |
| `data/` | Large generated snapshots |
| `.bin/` | Optional local tooling binaries |

---

## 6. Journey from zero to production-ready pipeline

This section documents the build order that produced the final system. Understanding the order helps newcomers recreate not just files, but decisions.

### Phase A — Access the source system

1. Create a Google Cloud project
2. Enable **YouTube Data API v3**
3. Create an API key restricted to that API (public data)
4. Smoke-test with an HTTP client (e.g. Postman):

```http
GET https://youtube.googleapis.com/youtube/v3/videos?part=statistics&id=<VIDEO_ID>&key=<API_KEY>
```

**Why:** Fail fast on credentials before writing orchestration code.

### Phase B — Standalone extraction prototype

1. Create a Python virtualenv
2. Write extract logic that walks:
   - **Channels** → uploads playlist ID
   - **PlaylistItems** → all video IDs (paginated)
   - **Videos** → metadata + statistics (batched)
3. Save `./data/YT_data_<today>.json`

**Why:** Validate API nesting and data shape before Docker/Airflow complexity.

### Phase C — Containerize Airflow

1. Create `requirements.txt` (later filled with Soda + pytest)
2. Create `dockerfile` extending `apache/airflow:2.9.2-python3.10`
3. Build custom image (locally tagged for your Docker Hub namespace)
4. Create `.env` with all Compose/Airflow/Postgres variables (exact names matter)
5. Add `docker-compose.yaml` + Postgres init script for 3 databases
6. `docker compose up -d`

**Why:** Local Airflow matching a multi-service deployment model.

### Phase D — Airflow-native extract DAG

1. Move extract code under `dags/api/video_stats.py`
2. Convert functions to `@task`
3. Read secrets via `Variable.get` (`AIRFLOW_VAR_*` from Compose env)
4. Wire DAG `produce_json` in `dags/main.py`
5. Trigger in UI / CLI and confirm JSON lands in `./data`

### Phase E — Warehouse load + transform

1. Implement `dags/datawarehouse/*`
2. Add DAG `update_db` (`staging_table` → `core_table`)
3. Verify with SQL client (DBeaver) against `elt_db`

### Phase F — Data quality

1. Add Soda packages to image, rebuild
2. Add `include/soda/configuration.yml` + `checks.yml`
3. Validate CLI connectivity and scans
4. Wrap scans in Airflow via `dataquality/soda.py`
5. Add DAG `data_quality` and chain it from `update_db`

### Phase G — Automated testing + CI/CD

1. Add pytest suite under `tests/`
2. Run tests inside `airflow-worker`
3. Add GitHub Actions workflow
4. Configure GitHub Secrets/Variables
5. Confirm green pipeline: build/push image + unit/integration/e2e tests

---

## 7. YouTube Data API design (Russian-doll extraction)

YouTube does not expose "give me all stats for a channel handle" in one call. The API forces a nested discovery pattern:

```mermaid
flowchart LR
    A["Channels.list\nforHandle=MrBeast"] -->|uploads playlistId| B["PlaylistItems.list\npaginated"]
    B -->|video IDs| C["Videos.list\nbatches of 50"]
    C -->|nested JSON| D["Flatten to 7 fields"]
```

### Fields collected per video

| Output field | Source path | Meaning |
|--------------|-------------|---------|
| `video_id` | `item.id` | Stable primary key |
| `title` | `snippet.title` | Video title |
| `publishedAt` | `snippet.publishedAt` | Publish timestamp (ISO-8601) |
| `duration` | `contentDetails.duration` | ISO-8601 duration (`PT14M35S`) |
| `viewCount` | `statistics.viewCount` | Views (string from API; stored as int later) |
| `likeCount` | `statistics.likeCount` | Likes |
| `commentCount` | `statistics.commentCount` | Comments |

### Why pagination and batching

- **PlaylistItems** returns at most **50** items per page → loop on `nextPageToken`
- **Videos** accepts up to **50** IDs per request → chunk ID list
- This reduces quota usage and runtime versus one-request-per-video

### Example flattened record

```json
{
    "video_id": "f7y2XikE7sY",
    "title": "Paying For Food With My Car",
    "publishedAt": "2026-07-31T16:00:11Z",
    "duration": "PT50S",
    "viewCount": "14010118",
    "likeCount": "687391",
    "commentCount": "8250"
}
```

---

## 8. Configuration and secrets (.env)

Compose interpolates `${VAR}` from `.env` automatically. Variable names are a contract between `.env`, `docker-compose.yaml`, Soda config, tests, and CI.

### 8.1 Docker image identity

```bash
DOCKERHUB_NAMESPACE=<your_dockerhub_username>
DOCKERHUB_REPOSITORY=ytb_etl
IMAGE_TAG=1.0.1
```

`docker-compose.yaml` uses:

```yaml
image: ${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:latest
```

### 8.2 Postgres bootstrap user

```bash
POSTGRES_CONN_USERNAME=postgres
POSTGRES_CONN_PASSWORD=...
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432
```

> **Note:** `POSTGRES_CONN_HOST=postgres` is the Docker network DNS name of the Postgres service, **not** `localhost`, from inside containers.

### 8.3 Three application databases

| Logical DB env prefix | Purpose |
|---------------------|---------|
| `METADATA_DATABASE_*` | Airflow metadata |
| `CELERY_BACKEND_*` | Celery result backend |
| `ELT_DATABASE_*` | Analytics warehouse (`elt_db`) |

### 8.4 Airflow runtime

```bash
AIRFLOW_UID=1000   # use `id -u` on Linux so mounted files are writable
AIRFLOW_WWW_USER_USERNAME=...
AIRFLOW_WWW_USER_PASSWORD=...
FERNET_KEY=...     # encrypts Airflow connections/variables at rest
```

Generate a Fernet key if needed:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 8.5 YouTube parameters

```bash
API_KEY=...
CHANNEL_HANDLE=MrBeast
```

Mapped into Airflow as:

- `AIRFLOW_VAR_API_KEY`
- `AIRFLOW_VAR_CHANNEL_HANDLE`

so DAG code can call `Variable.get("API_KEY")`.

---

## 9. Containerization

### 9.1 dockerfile — why a custom image

Base Airflow images are intentionally minimal. This project needs:

- `soda-core-postgres`
- `pytest`

Baked into the image so workers do not reinstall packages on every start.

```dockerfile
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ENV AIRFLOW_HOME=/opt/airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
```

**Why re-pin Airflow in pip install?** Keeps constraints consistent when adding requirements.

### 9.2 requirements.txt

```text
soda-core-postgres==3.3.14
pytest==8.3.3
```

Version bumps of this file should trigger image rebuilds (local and CI).

### 9.3 docker-compose.yaml services

| Service | Image / role |
|---------|--------------|
| `postgres` | `postgres:13`, hosts 3 DBs, runs init script |
| `redis` | Celery broker |
| `airflow-init` | DB migrate + admin user create |
| `airflow-webserver` | UI on `:8080` |
| `airflow-scheduler` | Parses DAGs, schedules tasks |
| `airflow-worker` | Executes tasks via Celery |

**Common Airflow settings** (shared via YAML anchors):

- Executor: **CeleryExecutor**
- Metadata SQLAlchemy URI → metadata DB
- Celery result backend → celery DB
- Broker → Redis
- Connection `postgres_db_yt_elt` → `elt_db`
- Mounts: `dags`, `data`, `include`, `logs`, `tests`, etc.

### 9.4 docker/postgres/init-multiple-databases.sh

Runs only on **first** Postgres volume initialization.

Function `create_user_and_database(db, user, password)`:

- `CREATE USER`
- `CREATE DATABASE`
- `GRANT ALL PRIVILEGES ON DATABASE ...`

Creates:

- metadata DB/user
- celery DB/user
- ELT DB/user

**Why three DBs?** Isolation of Airflow internal state from analytics data and Celery results.

### 9.5 Linux ownership note

On Linux, set:

```bash
AIRFLOW_UID=$(id -u)
```

Otherwise `dags/` may be created as UID **50000** and your IDE cannot create files (permission denied).

---

## 10. PostgreSQL data warehouse design

- **Database:** `elt_db`
- **User used by pipeline:** `yt_api_user` (from `.env`)
- **Table name in both schemas:** `yt_api`

### 10.1 Staging schema (`staging.yt_api`)

**Purpose:** land API-adjacent data quickly.

| Column | Type | Notes |
|--------|------|-------|
| `Video_ID` | `VARCHAR(11)` PK | YouTube IDs are 11 chars |
| `Video_Title` | `TEXT` | |
| `Upload_Date` | `TIMESTAMP` | from `publishedAt` |
| `Duration` | `VARCHAR(20)` | keep ISO duration string |
| `Video_Views` | `INT` | |
| `Likes_Count` | `INT` | |
| `Comments_Count` | `INT` | |

### 10.2 Core schema (`core.yt_api`)

**Purpose:** analytics-ready shape.

| Column | Type | Notes |
|--------|------|-------|
| `Video_ID` | `VARCHAR(11)` PK | |
| `Video_Title` | `TEXT` | |
| `Upload_Date` | `TIMESTAMP` | |
| `Duration` | `TIME` | parsed from ISO duration |
| `Video_Type` | `VARCHAR(10)` | **Shorts** if ≤ 60 seconds else **Normal** |
| `Video_Views` | `INT` | |
| `Likes_Count` | `INT` | |
| `Comments_Count` | `INT` | |

### 10.3 Load strategy (staging)

For each JSON row:

1. If table empty → insert all
2. Else if `video_id` exists → update mutable metrics/title
3. Else → insert
4. After loop: **delete** warehouse IDs absent from latest JSON snapshot (soft-delete reconciliation)

### 10.4 Load strategy (core)

1. Read all rows from `staging.yt_api`
2. Transform each row (`Duration`, `Video_Type`)
3. Same insert/update/delete reconciliation against `core.yt_api`

```mermaid
flowchart TD
    J[Today JSON] --> S[staging.yt_api]
    S --> T[transform_data]
    T --> C[core.yt_api]
    C --> Q[Soda checks]
    S --> Q
```

---

## 11. Airflow orchestration

### 11.1 DAG inventory

| DAG ID | Schedule | Purpose |
|--------|----------|---------|
| `produce_json` | `0 14 * * *` (14:00 daily) | Extract API → JSON, then trigger next DAG |
| `update_db` | none (trigger-only) | JSON → staging → core, then trigger DQ |
| `data_quality` | none (trigger-only) | Soda staging then Soda core |

### 11.2 Task graphs

**`produce_json`** (5 tasks):

```
get_playlist_id
  -> get_video_ids
    -> extract_video_data
      -> save_to_json
        -> trigger_update_db
```

**`update_db`** (3 tasks):

```
staging_table -> core_table -> trigger_data_quality
```

**`data_quality`** (2 tasks):

```
soda_test_staging -> soda_test_core
```

### 11.3 Why TriggerDagRunOperator

Keeps DAGs modular:

- Extract failures do not partially mix with transform code ownership
- DQ can be re-run independently
- Clear operational boundaries in the Airflow UI

### 11.4 Default args (shared)

| Setting | Value |
|---------|-------|
| `owner` | `dataengineers` |
| `depends_on_past` | `false` |
| `catchup` | `false` (no backfill storms) |
| `dagrun_timeout` | 1 hour |
| `start_date` | 2025-01-01 Europe/Malta |
| `max_active_runs` | 1 |

---

## 12. File-by-file and function-by-function reference

### 12.1 `dags/api/video_stats.py`

**Responsibility:** Extract YouTube data and land JSON. Airflow TaskFlow API module.

#### Module-level configuration

| Name | Why |
|------|-----|
| `API_KEY = Variable.get("API_KEY")` | Secret from env (`AIRFLOW_VAR_API_KEY`) |
| `CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")` | Target channel handle |
| `maxResults = 50` | API max page/batch size |

Commented `dotenv` block remains as historical reminder that local prototype used `.env` directly; Airflow path uses Variables instead.

#### `get_playlist_id()` — `@task`

- **What:** Calls Channels API with `forHandle`, returns uploads playlist ID.
- **Why:** Outer doll; all channel uploads are addressed via that playlist.

**Steps:**

1. Build URL with `part=contentDetails`
2. `requests.get`
3. `raise_for_status()`
4. Navigate `items[0].contentDetails.relatedPlaylists.uploads`
5. Return string playlist ID

#### `get_video_ids(playlistId)` — `@task`

- **What:** Collects every video ID from the uploads playlist.
- **Why:** Middle doll; Videos endpoint needs IDs.

**Steps:**

1. Initialize empty list + `pageToken=None`
2. Loop:
   - Request PlaylistItems
   - Append each `contentDetails.videoId`
   - Read `nextPageToken` or break
3. Return `list[str]`

#### `extract_video_data(video_ids)` — `@task`

- **What:** Fetch snippet + contentDetails + statistics for all IDs.
- **Why:** Inner doll; actual metrics live here.

**Nested helper `batch_list(lst, batch_size)`:**

- Yields slices `[0:50]`, `[50:100]`, ...
- Generator avoids building all batches eagerly

For each batch:

1. Join IDs with commas
2. Call Videos API with three parts
3. Flatten nested objects into the 7-field dict
4. Use `.get(..., None)` for optional stats

#### `save_to_json(extracted_data)` — `@task`

- **What:** Write pretty JSON to `./data/YT_data_<today>.json`
- **Why:** Durable landing zone for load stage; dated files enable daily snapshots.

Requires `data/` directory to exist (Compose/init creates it; host `mkdir` also fine).

#### `__main__` block

Allows conceptual standalone execution, but TaskFlow-decorated callables are intended to run inside Airflow task execution context.

---

### 12.2 `dags/datawarehouse/data_utils.py`

**Responsibility:** Shared DB access and DDL.

- `table = "yt_api"` — central table name constant.

#### `get_conn_cursor()`

- Builds `PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")`
- Opens connection
- Creates `RealDictCursor` so rows behave like dictionaries (`row["Video_ID"]`)

**Why RealDictCursor?** Staging/core code paths are clearer with named fields than tuple indexes.

#### `close_conn_cursor(conn, cur)`

Closes cursor then connection to avoid leaks in long DAG runs.

#### `create_schema(schema)`

`CREATE SCHEMA IF NOT EXISTS {schema}` + commit.

#### `create_table(schema)`

Branches DDL:

- **staging** → duration as `VARCHAR`, no `Video_Type`
- **else (core)** → duration as `TIME` + `Video_Type`

#### `get_video_ids(cur, schema)`

Selects existing IDs for reconciliation logic (insert vs update vs delete).

---

### 12.3 `dags/datawarehouse/data_loading.py`

#### `load_data()`

Reads `./data/YT_data_{date.today()}.json`.

**Error handling:**

- `FileNotFoundError` → log + raise (forces operator awareness that extract didn't produce today's file)
- `JSONDecodeError` → log + raise

**Why today-only filename?** Aligns staging load with the extract cadence; each successful daily extract produces the input for same-day load.

---

### 12.4 `dags/datawarehouse/data_modification.py`

#### `insert_rows(cur, conn, schema, row)`

- Staging uses JSON keys (`video_id`, `title`, ...)
- Core uses warehouse keys (`Video_ID`, `Video_Title`, ..., `Video_Type`)
- Commits per row (simple and debuggable; not maximal throughput)

#### `update_rows(cur, conn, schema, row)`

Updates mutable analytical fields:

- title
- views
- likes
- comments

Match predicate: `Video_ID` + `Upload_Date` (stable identity/time pair).

Does **not** overwrite core `Duration`/`Video_Type` in update set (those are set on insert via transform). Metrics refresh is the recurring need.

#### `delete_rows(cur, conn, schema, ids_to_delete)`

Deletes IDs present in warehouse but missing from latest source snapshot.

**Why delete?** Snapshot reconciliation keeps warehouse aligned with the channel's current uploads playlist contents.

---

### 12.5 `dags/datawarehouse/data_transformation.py`

#### `parse_duration(duration_str)`

Converts ISO-8601 durations like `PT1H2M3S` / `PT50S` into `timedelta`.

**Algorithm:**

1. Strip `P` and `T`
2. Split out `D`/`H`/`M`/`S` components if present
3. Build `timedelta`

#### `transform_data(row)`

1. Parse staging `Duration` string
2. Replace with `datetime.time`
3. Set `Video_Type`:
   - **Shorts** if total seconds ≤ 60
   - else **Normal**

**Why this rule?** Practical Shorts heuristic for analytics segmentation without extra API calls.

---

### 12.6 `dags/datawarehouse/dwh.py`

Airflow task wrappers that compose the modules above.

#### `staging_table()` — `@task`

1. Connect
2. `load_data()`
3. Ensure schema/table
4. Reconcile rows (insert/update)
5. Delete stale IDs
6. Always close connection in `finally`

#### `core_table()` — `@task`

1. Connect
2. Ensure schema/table
3. `SELECT * FROM staging.yt_api`
4. Transform each row
5. Reconcile into core
6. Delete stale IDs
7. Close in `finally`

---

### 12.7 `dags/dataquality/soda.py`

#### Constants

- `SODA_PATH = "/opt/airflow/include/soda"` (container path of mounted `include/soda`)
- `DATASOURCE = "pg_datasource"` (must match `configuration.yml`)

#### `yt_elt_data_quality(schema)`

Returns a `BashOperator` that runs:

```bash
soda scan -d pg_datasource \
  -c /opt/airflow/include/soda/configuration.yml \
  -v SCHEMA=<schema> \
  /opt/airflow/include/soda/checks.yml
```

**Why BashOperator?** Soda's CLI is the supported scan interface; this keeps checks declarative in YAML while Airflow handles sequencing/logging.

---

### 12.8 `dags/main.py`

Wiring file only: imports task factories/operators and declares the three DAGs + dependencies/triggers.

No business logic should live here beyond orchestration structure.

```python
# DAG 1: produce_json — 5 tasks, schedule 0 14 * * *
# DAG 2: update_db — 3 tasks, trigger-only
# DAG 3: data_quality — 2 tasks, trigger-only
```

---

### 12.9 Soda YAML files

#### `include/soda/configuration.yml`

Declares postgres datasource `pg_datasource` with env var placeholders:

- username/password/host/port/database from ELT + POSTGRES env
- `schema: ${SCHEMA}` injected at scan time (staging or core)

#### `include/soda/checks.yml`

Checks for dataset `yt_api`:

- `missing_count("Video_ID") = 0`
- `duplicate_count("Video_ID") = 0`
- Custom SQL: count of rows where likes > views must be 0
- Custom SQL: count of rows where comments > views must be 0

**Why these checks?** They catch broken primary keys and impossible engagement inconsistencies.

---

### 12.10 Tests

#### `tests/conftest.py` fixtures

| Fixture | Purpose |
|---------|---------|
| `api_key` | Mock `AIRFLOW_VAR_API_KEY` and read via `Variable.get` |
| `channel_handle` | Mock channel variable |
| `mock_postgres_conn_vars` | Mock Airflow Connection URI env |
| `dagbag` | Load all DAGs for integrity assertions |
| `airflow_variable` | Helper to read `AIRFLOW_VAR_*` from real env |
| `real_postgres_connection` | Live psycopg2 connection for integration test |

#### `tests/unit_test.py`

- Assert mocked variable values
- Assert mocked connection fields
- Assert DAG import errors empty
- Assert expected DAG IDs present
- Assert exactly **3** DAGs
- Assert task counts: `produce_json=5`, `update_db=3`, `data_quality=2`

#### `tests/integration_test.py`

- Live HTTP 200 from YouTube Channels endpoint using real env key/handle
- Live `SELECT 1` against warehouse DB

---

### 12.11 CI workflow `.github/workflows/ci-cd_yt-elt.yaml`

#### Triggers

- Push to `main` or `feature/*`
- PR to `main`
- Manual `workflow_dispatch`

#### Job 1: `build-and-push-image`

Runs when `dockerfile`/`Dockerfile`/`requirements.txt` changed or manual dispatch:

1. Checkout
2. Setup Buildx
3. Docker Hub login
4. Build & push `:latest` and `:<gitsha>`

#### Job 2: `unit-and-integration-and-e2e-tests`

Needs job 1. Runs when DAG/include/compose changed or manual dispatch:

1. `docker compose up -d`
2. `pytest tests/ -v` inside worker
3. `airflow dags test` for all three DAGs
4. `docker compose down`

Environment is injected from GitHub Secrets/Variables (mirrors local `.env` contract).

---

## 13. Data quality with Soda Core

### Why Soda in this architecture

Warehouse loads can succeed technically while still storing bad data. Soda adds an explicit quality gate after load/transform.

### Manual CLI commands (inside Airflow container)

```bash
# connectivity
soda test-connection -d pg_datasource \
  -c /opt/airflow/include/soda/configuration.yml -V

# scan core
soda scan -d pg_datasource \
  -c /opt/airflow/include/soda/configuration.yml \
  -v SCHEMA=core \
  /opt/airflow/include/soda/checks.yml

# scan staging
soda scan -d pg_datasource \
  -c /opt/airflow/include/soda/configuration.yml \
  -v SCHEMA=staging \
  /opt/airflow/include/soda/checks.yml
```

Via Compose:

```bash
docker compose exec -e SCHEMA=core airflow-scheduler bash -lc \
  'soda test-connection -d pg_datasource -c /opt/airflow/include/soda/configuration.yml -V'
```

---

## 14. Testing strategy

### Layers

| Layer | What it validates | External systems |
|-------|-------------------|------------------|
| Unit | Airflow Variable/Connection plumbing + DAG graph integrity | No |
| Integration | Real API key works; real DB accepts connections | Yes |
| E2E (`airflow dags test`) | Tasks execute in runtime ordering | Yes |

### Local test command

```bash
docker exec -t airflow-worker sh -c "pytest tests/ -v"
```

### Local e2e DAG commands

```bash
docker exec -t airflow-worker sh -c "airflow dags test produce_json"
docker exec -t airflow-worker sh -c "airflow dags test update_db"
docker exec -t airflow-worker sh -c "airflow dags test data_quality"
```

---

## 15. CI/CD with GitHub Actions

### Why CI/CD here

Code that only works on one laptop is not a pipeline product. CI proves:

- Image still builds with dependencies
- Tests pass in a clean environment
- DAGs still parse and run

### Required GitHub configuration

#### Secrets

| Secret | Purpose |
|--------|---------|
| `API_KEY` | YouTube Data API key |
| `FERNET_KEY` | Airflow encryption key |
| `POSTGRES_CONN_USERNAME` | Postgres bootstrap user |
| `POSTGRES_CONN_PASSWORD` | Postgres bootstrap password |
| `POSTGRES_CONN_HOST` | Use `postgres` for Compose network |
| `POSTGRES_CONN_PORT` | Typically `5432` |
| `METADATA_DATABASE_NAME` | Airflow metadata DB name |
| `METADATA_DATABASE_USERNAME` | Metadata DB user |
| `METADATA_DATABASE_PASSWORD` | Metadata DB password |
| `CELERY_BACKEND_NAME` | Celery results DB name |
| `CELERY_BACKEND_USERNAME` | Celery DB user |
| `CELERY_BACKEND_PASSWORD` | Celery DB password |
| `ELT_DATABASE_NAME` | Warehouse DB name |
| `ELT_DATABASE_USERNAME` | Warehouse DB user |
| `ELT_DATABASE_PASSWORD` | Warehouse DB password |
| `AIRFLOW_WWW_USER_USERNAME` | Airflow UI username |
| `AIRFLOW_WWW_USER_PASSWORD` | Airflow UI password |
| `DOCKERHUB_PASSWORD` | Docker Hub access token |

#### Variables

| Variable | Purpose |
|----------|---------|
| `DOCKERHUB_USERNAME` | Docker Hub login |
| `DOCKERHUB_NAMESPACE` | Image namespace |
| `DOCKERHUB_REPOSITORY` | Image repository name |
| `CHANNEL_HANDLE` | YouTube channel handle |
| `AIRFLOW_UID` | Linux UID for file ownership |

### Manual run

**GitHub → Actions → CI-CD Pipeline → Run workflow**

---

## 16. End-to-end runtime flow

```mermaid
sequenceDiagram
    participant U as Operator / Schedule
    participant P as produce_json
    participant API as YouTube API
    participant FS as ./data JSON
    participant D as update_db
    participant DB as elt_db
    participant Q as data_quality
    participant S as Soda

    U->>P: trigger / schedule 14:00
    P->>API: channels → playlistItems → videos
    API-->>P: nested payloads
    P->>FS: write YT_data_today.json
    P->>D: TriggerDagRunOperator
    D->>FS: load_data()
    D->>DB: upsert staging.yt_api
    D->>DB: transform + upsert core.yt_api
    D->>Q: TriggerDagRunOperator
    Q->>S: scan SCHEMA=staging
    Q->>S: scan SCHEMA=core
    S-->>Q: pass/fail
```

### Expected healthy outcomes

- New/updated JSON file in `data/`
- Row counts in staging and core roughly equal to channel upload count
- Core has both **Shorts** and **Normal**
- Soda: **4/4 checks passed** per schema scan
- Airflow UI: green task squares across the chain

---

## 17. How to clone and run this project

These instructions assume **Ubuntu/Linux** (Windows users should use WSL2 or adjust paths/UID guidance).

### 17.1 Prerequisites

Install:

- Git
- Docker Engine
- Docker Compose plugin (`docker compose version`)
- (Optional) DBeaver
- (Optional) Python 3.12+ if experimenting outside Docker

Verify:

```bash
git --version
docker --version
docker compose version
```

### 17.2 Clone

```bash
git clone https://github.com/SafwenCherif/ETL-Data-engineering-and-warehousing.git
cd ETL-Data-engineering-and-warehousing
```

### 17.3 Create YouTube API key

1. Create/select a Google Cloud project
2. Enable **YouTube Data API v3**
3. Create an API key
4. Restrict key to YouTube Data API v3
5. Keep the key private

### 17.4 Create `.env`

Create a `.env` file in the repo root (never commit it). Use this template and replace placeholders:

```bash
# Docker image identity (must match an image you build or pull)
DOCKERHUB_NAMESPACE=YOUR_DOCKERHUB_USERNAME
DOCKERHUB_REPOSITORY=ytb_etl
IMAGE_TAG=1.0.1

# Postgres bootstrap
POSTGRES_CONN_USERNAME=postgres
POSTGRES_CONN_PASSWORD=CHANGE_ME_STRONG
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432

# Airflow metadata DB
METADATA_DATABASE_NAME=airflow_metadata_db
METADATA_DATABASE_USERNAME=airflow_meta_user
METADATA_DATABASE_PASSWORD=CHANGE_ME_STRONG

# Celery results DB
CELERY_BACKEND_NAME=celery_results_db
CELERY_BACKEND_USERNAME=celery_user
CELERY_BACKEND_PASSWORD=CHANGE_ME_STRONG

# ELT warehouse DB
ELT_DATABASE_NAME=elt_db
ELT_DATABASE_USERNAME=yt_api_user
ELT_DATABASE_PASSWORD=CHANGE_ME_STRONG

# Airflow
AIRFLOW_UID=1000
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=CHANGE_ME_STRONG
FERNET_KEY=GENERATE_A_FERNET_KEY

# YouTube
API_KEY=YOUR_YOUTUBE_API_KEY
CHANNEL_HANDLE=MrBeast
```

**Linux recommendation:**

```bash
echo "AIRFLOW_UID=$(id -u)"
# put that numeric value into AIRFLOW_UID in .env
```

### 17.5 Make Postgres init script executable

```bash
chmod +x docker/postgres/init-multiple-databases.sh
```

### 17.6 Build the custom Airflow image

```bash
docker build -f dockerfile -t YOUR_DOCKERHUB_USERNAME/ytb_etl:1.0.1 .
docker tag YOUR_DOCKERHUB_USERNAME/ytb_etl:1.0.1 YOUR_DOCKERHUB_USERNAME/ytb_etl:latest
```

If dependencies change later:

```bash
docker build --no-cache -f dockerfile -t YOUR_DOCKERHUB_USERNAME/ytb_etl:1.0.1 .
docker tag YOUR_DOCKERHUB_USERNAME/ytb_etl:1.0.1 YOUR_DOCKERHUB_USERNAME/ytb_etl:latest
```

Optional push (needed for clean CI/other machines pulling from Hub):

```bash
docker login
docker push YOUR_DOCKERHUB_USERNAME/ytb_etl:latest
docker push YOUR_DOCKERHUB_USERNAME/ytb_etl:1.0.1
```

### 17.7 Start the stack

```bash
mkdir -p data dags logs plugins config include tests
docker compose up -d
docker compose ps
```

Wait until webserver is healthy, then open:

- **UI:** http://localhost:8080
- **Login:** values from `AIRFLOW_WWW_USER_USERNAME` / `AIRFLOW_WWW_USER_PASSWORD`

### 17.8 Unpause DAGs and run

**In UI:**

1. Unpause `produce_json`, `update_db`, `data_quality`
2. Trigger `produce_json`

**Or CLI:**

```bash
docker compose exec airflow-scheduler airflow dags unpause produce_json
docker compose exec airflow-scheduler airflow dags unpause update_db
docker compose exec airflow-scheduler airflow dags unpause data_quality
docker compose exec airflow-scheduler airflow dags trigger produce_json
```

### 17.9 Verify outputs

**JSON:**

```bash
ls -la data/
```

**Database (example):**

```bash
docker exec -it postgres psql -U postgres -d elt_db -c 'SELECT count(*) FROM staging.yt_api;'
docker exec -it postgres psql -U postgres -d elt_db -c 'SELECT count(*) FROM core.yt_api;'
docker exec -it postgres psql -U postgres -d elt_db -c 'SELECT "Video_Type", count(*) FROM core.yt_api GROUP BY 1;'
```

**DBeaver connection:**

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `elt_db` |
| User | `yt_api_user` |
| Password | your `ELT_DATABASE_PASSWORD` |

### 17.10 Run tests locally

```bash
docker exec -t airflow-worker sh -c "pytest tests/ -v"
```

### 17.11 Stop / reset

Stop containers:

```bash
docker compose down
```

Stop and delete Postgres volume (destructive reset of DBs):

```bash
docker compose down -v
```

---

## 18. Day-2 operations cheat sheet

```bash
# status
docker compose ps

# logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# trigger chain head
docker compose exec airflow-scheduler airflow dags trigger produce_json

# test one DAG in isolation
docker compose exec airflow-worker airflow dags test update_db

# pytest
docker exec -t airflow-worker sh -c "pytest tests/ -v"

# soda scan core
docker compose exec -e SCHEMA=core airflow-scheduler bash -lc \
  'soda scan -d pg_datasource -c /opt/airflow/include/soda/configuration.yml -v SCHEMA=core /opt/airflow/include/soda/checks.yml'

# rebuild image after requirements change
docker build --no-cache -f dockerfile -t $DOCKERHUB_NAMESPACE/$DOCKERHUB_REPOSITORY:latest .
docker compose up -d --force-recreate
```

---

## 19. Troubleshooting

### FileNotFoundError: `./data/YT_data_....json` during extract save

Create folder:

```bash
mkdir -p data
```

### DAG import error: No module named `dataquality` / `datawarehouse`

Ensure packages exist under `dags/` and Airflow has reloaded. Check:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```

### Permission denied creating files under `dags/`

Folder owned by UID 50000. Fix:

```bash
# set AIRFLOW_UID=$(id -u) in .env, then:
docker run --rm -v "$PWD:/work" alpine chown -R $(id -u):$(id -g) /work/dags /work/include /work/logs /work/tests /work/data
docker compose up -d --force-recreate
```

### `update_db` fails with file not found

`load_data()` expects today's filename. Run `produce_json` first the same day, or temporarily copy an existing snapshot:

```bash
cp data/YT_data_YYYY-MM-DD.json data/YT_data_$(date +%Y-%m-%d).json
```

### Soda not found in container

Image was built before requirements were added. Rebuild with `--no-cache` and recreate containers.

### CI cannot pull image

Ensure Docker Hub repo exists, secrets/vars are correct, and build job pushed `:latest`.

### Postgres init script did not create DBs

Init runs only on empty volume. Reset with `docker compose down -v` (destructive) then `up -d`.

### API quota / 403 from YouTube

Check key restrictions, API enablement, and quota in Google Cloud Console.

---

## 20. Security notes

- **Never commit** `.env`
- **Never paste** API keys or Docker Hub tokens into public issues/chats
- Restrict YouTube API keys to needed APIs
- Rotate any credential that was exposed
- Use Docker Hub **access tokens** (not account password) for CI
- Prefer least-privilege DB users in real deployments (this project grants broad privileges for simplicity)
- Fernet key loss makes encrypted Airflow secrets unreadable — **back it up securely**

---

## 21. Possible extensions

- Add partitioning / incremental extract windows for very large channels
- Replace per-row commits with bulk upsert (`COPY` / `ON CONFLICT`)
- Add Great Expectations or additional Soda metrics (freshness, volume anomalies)
- Emit warehouse data to a BI layer (Metabase/Superset)
- Multi-channel config table instead of single `CHANNEL_HANDLE`
- Alerting on DAG failure (email/Slack)
- Infrastructure as code for cloud Airflow (MWAA/Composer/Astro)
- Schema migrations tool (Alembic) for warehouse DDL evolution

---

## 22. Glossary

| Term | Meaning |
|------|---------|
| ELT | Extract, Load, then Transform in the warehouse |
| DAG | Directed Acyclic Graph of tasks in Airflow |
| TaskFlow | Airflow `@task` decorator style |
| Staging | Early landing schema close to source |
| Core | Cleaned/enriched analytics schema |
| Upsert pattern | Insert new + update existing records |
| CeleryExecutor | Airflow executor using Celery workers |
| Fernet key | Symmetric key used by Airflow for secret encryption |
| Soda scan | Execution of declarative data quality checks |
| DagBag | Airflow object that loads/parses DAG files |
| Russian-doll API pattern | Nested calls: channel → playlist items → videos |

---

## Closing

This repository is a complete, runnable demonstration of modern batch data engineering practices: API extraction, warehouse modeling, orchestration, data quality, automated testing, and CI/CD.

If you follow **Section 17** carefully—especially `.env` naming, image tagging, and `AIRFLOW_UID` on Linux—you can reproduce the full pipeline on a fresh machine and observe green DAGs, populated Postgres schemas, passing Soda checks, and a passing pytest suite.

**Project owner:** Safwen Cherif  
**GitHub:** [ETL-Data-engineering-and-warehousing](https://github.com/SafwenCherif/ETL-Data-engineering-and-warehousing)

---

## Appendix A — Default ports

| Service | Host port |
|---------|-----------|
| Airflow Web UI | 8080 |
| PostgreSQL | 5432 |
| Redis | internal 6379 (exposed in Compose network) |

---

## Appendix B — Image and runtime versions used in this project

| Component | Version / pin |
|-----------|---------------|
| Airflow base image | 2.9.2-python3.10 |
| Postgres | 13 |
| Redis | 7.2-bookworm |
| soda-core-postgres | 3.3.14 |
| pytest | 8.3.3 |

---

## Appendix C — Minimal success checklist for newcomers

- [ ] Repo cloned
- [ ] `.env` created with real secrets
- [ ] `AIRFLOW_UID` matches Linux user
- [ ] Image built and tagged as `${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:latest`
- [ ] `docker compose up -d` healthy
- [ ] Can log into Airflow UI
- [ ] `produce_json` success creates today's JSON
- [ ] `update_db` success populates staging + core
- [ ] `data_quality` success (Soda pass)
- [ ] `pytest tests/ -v` → all passed
- [ ] (Optional) GitHub Actions workflow green

---

## Appendix D — Design rationale summary (what + why, compressed)

| Decision | What | Why |
|----------|------|-----|
| JSON landing | Write dated files before DB | Debuggable intermediate artifact; decouple extract from load |
| Staging then core | Two schemas | Preserve source-like data and publish curated model |
| TriggerDagRun chaining | 3 DAGs | Modular operations and clearer failure domains |
| CeleryExecutor | Redis + worker | Closer to production Airflow topology |
| Env-based Variables/Connections | `AIRFLOW_VAR_*` / `AIRFLOW_CONN_*` | 12-factor config; CI-friendly |
| Soda YAML checks | Declarative DQ | Non-developers can read rules; scans are portable |
| Custom image | Bake pytest/Soda | Deterministic worker environment |
| GitHub Actions | Build + test automation | Prevent silent breakage on change |

---

## Appendix E — docker-compose.yaml deep dive (service by service)

This appendix explains the Compose file the way an operator reads it: what each block does and what breaks if it is wrong.

### E.1 YAML anchors (`x-airflow-common`)

Compose supports extension fields starting with `x-`. This project defines `x-airflow-common` once and reuses it for webserver/scheduler/worker/init via:

```yaml
<<: *airflow-common
```

**Why:** Prevents drifting environment variables across Airflow containers. If `AIRFLOW_VAR_API_KEY` were present on the worker but missing on a future service copy-paste, tests or tasks would fail intermittently.

**Included shared concerns:**

- custom image reference
- executor/broker/metadata URIs
- Fernet key
- example DAG loading disabled
- health-check enablement for scheduler
- warehouse connection URI
- YouTube Variables
- ELT env vars exposed for Soda/tests
- volume mounts
- service dependency on healthy Redis + Postgres
- runtime user `${AIRFLOW_UID}:0`

### E.2 Postgres service

**Critical mounts:**

- Named volume `postgres-db-volume` → persistent data files
- Init script → `/docker-entrypoint-initdb.d/init-multiple-databases.sh`

**Healthcheck:**

```bash
pg_isready -U <postgres user> -d <metadata db>
```

**Why metadata DB in healthcheck?** Airflow cannot start usefully until metadata connectivity works; that DB is created by the init script, so readiness implies init completed on first boot.

Published port `5432:5432` allows host tools (DBeaver, psql) to connect to `localhost:5432`.

### E.3 Redis service

Used only as Celery broker URL:

```text
redis://:@redis:6379/0
```

No password in this local design for simplicity. Healthcheck pings with `redis-cli ping`.

### E.4 airflow-init

Runs as root (`user: "0:0"`) to:

- Warn if `AIRFLOW_UID` missing
- Warn on low CPU/RAM/disk
- `mkdir` source folders and `chown` to Airflow UID
- Execute entrypoint with DB migrate + admin user creation flags:
  - `_AIRFLOW_DB_MIGRATE=true`
  - `_AIRFLOW_WWW_USER_CREATE=true`
  - username/password from env

Other Airflow services `depends_on` init with `service_completed_successfully`.

### E.5 Webserver / scheduler / worker

| Service | Command | Host access |
|---------|---------|-------------|
| webserver | webserver | localhost:8080 |
| scheduler | scheduler | internal health on 8974 |
| worker | celery worker | no host port required |

Worker sets `DUMB_INIT_SETSID=0` for proper Celery warm shutdown signal handling (Airflow Docker guidance).

### E.6 Profiles not used by default

Compose includes optional debug CLI profile and commented Flower. They are intentionally inactive to keep the default stack lean.

---

## Appendix F — Airflow Variable and Connection contract

### F.1 Variables (env → code)

**Compose:**

```yaml
AIRFLOW_VAR_API_KEY: ${API_KEY}
AIRFLOW_VAR_CHANNEL_HANDLE: ${CHANNEL_HANDLE}
```

**Code:**

```python
Variable.get("API_KEY")
Variable.get("CHANNEL_HANDLE")
```

**Important behavior:**

- These Variables work via environment injection
- They may **not** appear in Admin → Variables UI
- `airflow variables list` may also be empty
- This is expected and documented by Airflow

### F.2 Connection

**Compose:**

```yaml
AIRFLOW_CONN_POSTGRES_DB_YT_ELT: 'postgresql://user:pass@postgres:5432/elt_db'
```

**Code / Hook:**

```python
PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
```

**Naming rule:**

- Env form: `AIRFLOW_CONN_<CONN_ID_IN_UPPERCASE>`
- Runtime conn id: lowercase/as defined (`postgres_db_yt_elt`)

---

## Appendix G — Data dictionary and mapping from API to warehouse

### G.1 Extract JSON (landing)

| JSON key | Example | Semantic type |
|----------|---------|-----------------|
| `video_id` | `abcDEF12345` | string PK |
| `title` | `$1 vs $10,000 ...` | string |
| `publishedAt` | `2026-07-31T16:00:11Z` | timestamptz string |
| `duration` | `PT14M35S` | ISO duration string |
| `viewCount` | `"38645202"` | numeric string |
| `likeCount` | `"1351510"` | numeric string |
| `commentCount` | `"70351"` | numeric string |

### G.2 Staging mapping

| Staging column | Source JSON key |
|----------------|-----------------|
| `Video_ID` | `video_id` |
| `Video_Title` | `title` |
| `Upload_Date` | `publishedAt` |
| `Duration` | `duration` (unchanged string) |
| `Video_Views` | `viewCount` |
| `Likes_Count` | `likeCount` |
| `Comments_Count` | `commentCount` |

### G.3 Core mapping

| Core column | Source |
|-------------|--------|
| `Video_ID` | `staging.Video_ID` |
| `Video_Title` | `staging.Video_Title` |
| `Upload_Date` | `staging.Upload_Date` |
| `Duration` | parsed `TIME` from `staging.Duration` |
| `Video_Type` | derived from duration seconds |
| `Video_Views` | `staging.Video_Views` |
| `Likes_Count` | `staging.Likes_Count` |
| `Comments_Count` | `staging.Comments_Count` |

---

## Appendix H — Example analytical SQL after a successful run

```sql
-- Volume overview
SELECT
  (SELECT count(*) FROM staging.yt_api) AS staging_rows,
  (SELECT count(*) FROM core.yt_api) AS core_rows;

-- Shorts vs Normal
SELECT "Video_Type", count(*) AS n
FROM core.yt_api
GROUP BY 1
ORDER BY 2 DESC;

-- Top 20 videos by views
SELECT "Video_Title", "Video_Views", "Likes_Count", "Comments_Count", "Video_Type"
FROM core.yt_api
ORDER BY "Video_Views" DESC
LIMIT 20;

-- Engagement rate proxy (likes / views)
SELECT
  "Video_Title",
  "Video_Views",
  "Likes_Count",
  ROUND(("Likes_Count"::numeric / NULLIF("Video_Views", 0)) * 100, 2) AS like_rate_pct
FROM core.yt_api
WHERE "Video_Views" > 0
ORDER BY like_rate_pct DESC
LIMIT 20;

-- Upload activity by year
SELECT date_trunc('year', "Upload_Date") AS year, count(*) AS uploads
FROM core.yt_api
GROUP BY 1
ORDER BY 1;

-- Recent Shorts
SELECT "Video_Title", "Upload_Date", "Duration", "Video_Views"
FROM core.yt_api
WHERE "Video_Type" = 'Shorts'
ORDER BY "Upload_Date" DESC
LIMIT 20;
```

---

## Appendix I — Exact first-day command script (copy/paste)

Use this as a single path from clone to green pipeline on a Linux machine with Docker installed.

```bash
# 0) clone
git clone https://github.com/SafwenCherif/ETL-Data-engineering-and-warehousing.git
cd ETL-Data-engineering-and-warehousing

# 1) env file (edit values afterward)
cat > .env << 'EOF'
DOCKERHUB_NAMESPACE=YOUR_DOCKERHUB_USERNAME
DOCKERHUB_REPOSITORY=ytb_etl
IMAGE_TAG=1.0.1
POSTGRES_CONN_USERNAME=postgres
POSTGRES_CONN_PASSWORD=replace_me
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432
METADATA_DATABASE_NAME=airflow_metadata_db
METADATA_DATABASE_USERNAME=airflow_meta_user
METADATA_DATABASE_PASSWORD=replace_me
CELERY_BACKEND_NAME=celery_results_db
CELERY_BACKEND_USERNAME=celery_user
CELERY_BACKEND_PASSWORD=replace_me
ELT_DATABASE_NAME=elt_db
ELT_DATABASE_USERNAME=yt_api_user
ELT_DATABASE_PASSWORD=replace_me
AIRFLOW_UID=1000
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=replace_me
FERNET_KEY=replace_with_generated_fernet
API_KEY=replace_with_youtube_key
CHANNEL_HANDLE=MrBeast
EOF

# set UID automatically on Linux
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env

# generate fernet key helper (optional)
python3 - << 'PY'
from pathlib import Path
try:
    from cryptography.fernet import Fernet
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'cryptography'])
    from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
PY
# paste printed key into FERNET_KEY in .env

# 2) permissions + dirs
chmod +x docker/postgres/init-multiple-databases.sh
mkdir -p data dags logs plugins config include tests

# 3) build image (namespace must match .env)
source .env
docker build -f dockerfile -t ${DOCKERHUB_NAMESPACE}/${DOCKERHUB_REPOSITORY}:latest .

# 4) start
docker compose up -d
docker compose ps

# 5) wait for UI, then unpause + trigger
docker compose exec airflow-scheduler airflow dags unpause produce_json
docker compose exec airflow-scheduler airflow dags unpause update_db
docker compose exec airflow-scheduler airflow dags unpause data_quality
docker compose exec airflow-scheduler airflow dags trigger produce_json

# 6) validate
ls -la data/
docker exec postgres psql -U postgres -d elt_db -c 'SELECT count(*) FROM core.yt_api;'
docker exec -t airflow-worker sh -c "pytest tests/ -v"
```

---

## Appendix J — Channel portability

To run this pipeline for another public channel:

1. Change `.env`:
   ```bash
   CHANNEL_HANDLE=AnotherChannelHandle
   ```
2. Recreate Airflow containers so env is re-read:
   ```bash
   docker compose up -d --force-recreate
   ```
3. Trigger `produce_json` again

No code change is required if the handle is valid for `forHandle` lookups.

If a channel lacks a handle and only has a channel ID, the extract module would need an alternate Channels query (`id=` / `forUsername=` depending on channel), which is an intentional extension point.

---

## Appendix K — Quota and operational cost awareness

YouTube Data API quota is finite. This project's extract pattern is efficient but not free:

- 1 Channels call
- N PlaylistItems pages (`ceil(video_count / 50)`)
- M Videos calls (`ceil(video_count / 50)`)

For ~1000 videos, expect roughly:

- 1 channel call
- ~20 playlist pages
- ~20 video batch calls

Running extracts too frequently can exhaust daily quota. The default schedule (`0 14 * * *`) is once per day, which is appropriate for portfolio/analytics snapshotting.

---

## Appendix L — What each major dependency contributes at runtime

### Inside Airflow worker during `produce_json`

- Scheduler creates task instances
- Celery broker (Redis) queues work
- Worker process executes Python task callables
- `requests` performs HTTPS calls to Google
- XCom transports return values between TaskFlow tasks (playlist id → ids → records)
- Filesystem write creates JSON on mounted `./data`

### Inside worker during `update_db`

- Task reads JSON from mounted volume
- `PostgresHook` opens DB session to `elt_db`
- SQL DDL/DML mutates staging/core
- Logs stream to Airflow task logs

### Inside worker during `data_quality`

- `BashOperator` spawns shell
- `soda` CLI reads YAML
- Soda connects with same ELT credentials
- Non-zero exit fails the Airflow task if checks fail

---

## Appendix M — Integrity guarantees provided by tests

| Failure mode | Caught by |
|--------------|-----------|
| Broken DAG import / missing module | `test_dags_integrity` |
| Accidental removal of a DAG | expected DAG ID assertions |
| Task graph accidentally shortened/lengthened | task count assertions |
| Variable env contract regression | unit variable tests |
| Connection env contract regression | mock connection test |
| Invalid YouTube key / API disabled | integration YouTube test |
| Warehouse DB unreachable / bad creds | integration Postgres test |
| Extract/load/DQ runtime regressions | `airflow dags test ...` in CI |

---

## Appendix N — CI change-detection philosophy

The workflow uses path filters so not every commit rebuilds/pushes an image:

- Image rebuild when packaging inputs change (`dockerfile`, `requirements.txt`)
- Test stack when pipeline logic/config changes (`dags/**`, `include/**`, `docker-compose.yaml`)
- `workflow_dispatch` forces full path regardless of diff

This reduces CI minutes while preserving safety on meaningful changes.

**Caveat:** if someone changes Python business logic but forgets that image deps also changed, they must update `requirements.txt` or manually dispatch. In practice dependency changes are rare compared with DAG edits.

---

## Appendix O — Local vs CI differences

| Topic | Local | GitHub Actions |
|-------|-------|----------------|
| Secrets source | `.env` file | GitHub Secrets/Variables |
| Image source | Local build/tag often enough | Usually pulled from Docker Hub after push job |
| Persistence | Named docker volume survives restarts | Ephemeral runner; torn down each job |
| UID | Must match host user on Linux | Uses configured `AIRFLOW_UID` var |
| Debugging | Airflow UI + DBeaver + logs | Job logs only |

---

## Appendix P — Recommended demo script for portfolio walkthrough

If presenting this project:

1. Show architecture diagram (Section 4)
2. Open Airflow UI Graph for `produce_json`
3. Trigger run and show task progression
4. Open generated JSON sample record
5. In DBeaver, show staging vs core (Duration type + `Video_Type`)
6. Show Soda scan logs (4/4 passed)
7. Show pytest output
8. Show green GitHub Actions run
9. Explain how `TriggerDagRunOperator` connects the product stages

This sequence communicates engineering maturity beyond "I wrote a script that calls an API."

---

## Appendix Q — File responsibility matrix

| File | Create schema | Call API | Write JSON | Write SQL rows | Transform | DQ | Orchestrate | Test |
|------|:-------------:|:--------:|:----------:|:--------------:|:---------:|:--:|:-----------:|:----:|
| `api/video_stats.py` | | ✓ | ✓ | | | | task defs | |
| `data_loading.py` | | | read | | | | | |
| `data_utils.py` | ✓ | | | helper | | | | |
| `data_modification.py` | | | | ✓ | | | | |
| `data_transformation.py` | | | | | ✓ | | | |
| `dwh.py` | via utils | | | via modification | via transform | | tasks | |
| `soda.py` | | | | | | ✓ | operator factory | |
| `main.py` | | | | | | | ✓ | |
| `checks.yml` | | | | | | rules | | |
| `unit_test.py` | | | | | | | | ✓ |
| `integration_test.py` | | ✓ | | connect | | | | ✓ |
| `ci-cd_yt-elt.yaml` | | | | | | | automate | automate |

---

## Appendix R — Known simplifications (honest scope boundary)

This project intentionally chooses clarity over absolute production maximalism:

- Per-row commit SQL is easier to reason about than bulk upsert performance tuning
- No retry exponential backoff around YouTube HTTP calls beyond Airflow task retries (optional)
- No separate object storage (S3) for landing JSON; local/mounted filesystem is sufficient for this architecture
- Broad DB grants simplify onboarding
- Single-channel configuration via env var
- Compose warning in file header: not a hardened cloud production deployment by itself

These are conscious trade-offs, not accidents.

---

## Appendix S — Recovery playbooks

### S.1 Recreate only Airflow containers, keep warehouse data

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler airflow-worker airflow-init
```

### S.2 Recreate everything including empty databases

```bash
docker compose down -v
docker compose up -d
# then rerun produce_json → update_db → data_quality
```

### S.3 Re-run DQ only

```bash
docker compose exec airflow-scheduler airflow dags trigger data_quality
```

### S.4 Re-run load only (same day's JSON already exists)

```bash
docker compose exec airflow-scheduler airflow dags trigger update_db
```

---

## Appendix T — Final mental model

Think of the system as four concentric contracts:

1. **Source contract:** YouTube API nested resources and quotas
2. **Landing contract:** dated JSON with seven fields
3. **Warehouse contract:** staging/core tables and reconciliation semantics
4. **Quality/ops contract:** Soda + pytest + Airflow + CI must stay green

If you change one contract, update the downstream contract tests the same day.

That discipline is the difference between a demo script and an engineered data platform component.

---

## Appendix U — Contributor onboarding FAQ

**Q: Do I need Docker Hub to run locally?**  
A: No. Locally you can build and tag the image with the same name Compose expects. Docker Hub matters for CI runners and other machines pulling prebuilt images.

**Q: Why are there three Postgres databases instead of three containers?**  
A: One Postgres process is enough; logical separation gives isolation without the RAM cost of three database servers.

**Q: Why TaskFlow (`@task`) instead of classic operators for extract/load?**  
A: TaskFlow reduces boilerplate for Python-native steps and passes data through XCom naturally between extract stages.

**Q: Why keep JSON if Postgres is the warehouse?**  
A: JSON is an audit-friendly landing zone and decouples extract success from load success. You can reload without re-hitting the API.

**Q: Can I schedule `update_db` independently?**  
A: Yes, but the intended product path is trigger-based after extract. Independent schedules risk loading a stale/missing "today" file.

**Q: What happens if Soda fails?**  
A: The `data_quality` DAG run fails. Upstream data remains in the warehouse for investigation; fix data/rules then re-trigger DQ.

**Q: Is `Video_Type = Shorts` for duration ≤ 60s always accurate?**  
A: It is a practical heuristic aligned with common Shorts length expectations, not a guarantee of YouTube's internal Shorts designation.

---

## Appendix V — Document maintenance

When evolving the project, update this report in the same PR/commit when you:

- Add/remove DAGs or tasks (also update unit task-count assertions)
- Change table schemas or JSON fields
- Add dependencies in `requirements.txt`
- Change CI secret/variable contracts
- Change default schedules or trigger topology

Keeping code and this report synchronized preserves the value of the repository as both a runnable system and a teaching artifact for your future self and collaborators.

---

## Appendix W — Quick reference card

| Need | Command / location |
|------|-------------------|
| Start stack | `docker compose up -d` |
| Stop stack | `docker compose down` |
| Airflow UI | http://localhost:8080 |
| Trigger pipeline | Trigger DAG `produce_json` |
| Pytest | `docker exec -t airflow-worker sh -c "pytest tests/ -v"` |
| Core row count | `docker exec postgres psql -U postgres -d elt_db -c 'SELECT count(*) FROM core.yt_api;'` |
| Rebuild image | `docker build -f dockerfile -t $DOCKERHUB_NAMESPACE/$DOCKERHUB_REPOSITORY:latest .` |
| CI | GitHub → Actions → CI-CD Pipeline |
| Secrets file | `.env` (local, gitignored) |
| Extract code | `dags/api/video_stats.py` |
| Warehouse code | `dags/datawarehouse/` |
| DQ code | `dags/dataquality/soda.py` + `include/soda/` |
| Orchestration | `dags/main.py` |

---

*Document generated for the Safwen Cherif YouTube ELT Data Engineering project.*

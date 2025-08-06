# Project Status Over Time ETL

This repository contains an ETL pipeline that transforms project milestone data into a daily project status table, suitable for time-series analytics in BI tools like Tableau. It leverages PostgreSQL for storage and transformation, Python for orchestration, and Docker for environment consistency.

---

## Overview

Typical project databases track only milestone dates like `initial_date`, `active_date`, and `end_date`. These do not provide visibility into the project's status on any given day.

This ETL generates a **daily-grain fact table** where each row reflects a project's status (`initial`, `active`, or `completed`) on a specific date. This structure enables precise trend analyses and timeline visualizations.

---

## Use Cases

- Visualizing project status over time in Tableau or Power BI
- Comparing durations across projects or teams
- Tracking active projects on any day
- Supporting operational and performance dashboards

---

## Project Logic Rules

- `store_date` is always populated
- `initial_date` can be null
- `active_date` must be after `initial_date` (if present), and can be null
- `end_date` must be after `active_date` (if present), and can be null

### Dependencies Between Dates:

| Condition | Requirements |
|----------|--------------|
| `end_date` is present | `initial_date` and `active_date` must also be present |
| `active_date` is present | `initial_date` must also be present |
| Only `initial_date` is present | `active_date` and `end_date` can be null |

---

## Input Schema (`projects` table)

| Column        | Type    | Description                                      |
|---------------|---------|--------------------------------------------------|
| project_id    | UUID    | Unique identifier                                |
| store_date    | DATE    | When the record was stored in the database       |
| initial_date  | DATE    | When the project was first created               |
| active_date   | DATE    | When the project moved into execution            |
| end_date      | DATE    | When the project was completed                   |
| project_name  | TEXT    | Synthetic name for the project                   |
| owner         | TEXT    | Department responsible for the project           |
| region        | TEXT    | Region of operation                              |
| budget        | NUMERIC | Budget amount in USD                             |

---

## Output Schema (`daily_project_status` table)

| Column       | Type    | Description                              |
|--------------|---------|------------------------------------------|
| project_id   | UUID    | Project identifier                       |
| project_date | DATE    | Specific date between start and end      |
| status       | TEXT    | One of: `initial`, `active`, `completed` |

---

## Status Assignment Logic

For each date in the range between start and end:

- If `active_date` is not null and `date` < `active_date` → `initial`
- If `active_date` is not null and `active_date` ≤ `date` < `end_date` → `active`
- If `end_date` is not null and `date` = `end_date` → `completed`
- Fallback → `initial`

---

## Estimated End Dates

If `end_date` is missing:
- Use the latest available milestone (`initial_date`, `active_date`, or `store_date`)
- Add the **average project duration** (calculated from completed projects)
- Add a buffer of 15 days

---

## Audit Table (`etl_audit`)

The pipeline logs every run in an audit table:

| Column       | Type      | Description                       |
|--------------|-----------|-----------------------------------|
| run_id       | UUID      | Unique ID per ETL run             |
| script_name  | TEXT      | The Python or SQL script executed |
| status       | TEXT      | `success` or `failure`            |
| run_time     | TIMESTAMP | When the script was executed      |
| message      | TEXT      | Additional notes or errors        |

---

## ETL Architecture

```mermaid
flowchart TD
    subgraph Source
        A[projects table<br>PostgreSQL] 
    end

    subgraph ETL Pipeline [ETL Pipeline - SQL + Python + Docker]
        direction TB
        B[Extract Data<br>populate_projects.py]
        C[Transform Data<br>transform_projects_to_daily_status.sql]
        D[Load Result<br>Insert into daily_project_status]
    end

    subgraph Destination
        E[daily_project_status<br>PostgreSQL table]
    end

    subgraph BI Layer
        F[Tableau Dashboard]
    end

    subgraph Logs
        L[audit table<br>etl_audit]
    end

    A --> B --> C --> D --> E --> F
    C --> L
    D --> L
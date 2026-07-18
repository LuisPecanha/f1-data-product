# CLAUDE.md

Instructions for the coding agent working on the F1 Data Product.

## Project Overview

This project builds a set of analytical **data products** covering Formula 1 race
data, sourced from the public [OpenF1 API](https://openf1.org). It ingests raw
session, lap, stint, pit, and race-control data; transforms it through a
bronze/silver/gold layered pipeline; and publishes gold-layer tables as governed
data products backed by [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/)
contracts stored in `contracts/`.

The workflow is **contract-driven**: for every data product, the ODCS contract in
`contracts/` is written first and is the single source of truth for schema, types,
nullability, quality checks, and lineage. dbt models are then scaffolded and
implemented *to match the contract*. Code never dictates the contract — the
contract dictates the code. If dbt output and the contract ever disagree, dbt is
wrong and must be fixed; the contract is not adjusted to fit what the code happens
to produce.

## Working Principles

1. Don't assume. Don't hide confusion. Surface tradeoffs.
2. Minimum code that solves the problem. Nothing speculative.
3. Touch only what you must. Clean up only your own mess.
4. Define success criteria. Loop until verified.

## Stack

| Tool | Role | Notes |
|---|---|---|
| **DuckDB** | Local analytical database. Stores bronze/silver/gold schemas in a single file, `f1.duckdb`. | Contracts reference it via `type: duckdb`, `path: ./f1.duckdb`. |
| **dlt** (data load tool) | Ingestion. Pulls raw JSON from the OpenF1 REST API and loads it into the bronze schema. | One pipeline per source endpoint or a small set of related endpoints. |
| **dbt Core** with **dbt-duckdb** | Transformation. Builds bronze → silver → gold models entirely in SQL against the local DuckDB file. | Model layer and materialization must follow the [Layer Convention](#layer-convention) below. |
| **Data Contract CLI** (`datacontract`) | Contract validation. Runs `datacontract test` against each ODCS YAML file to check the live gold table matches the declared schema and quality rules. | Must pass before a data product is considered done. |

## Layer Convention

- **Bronze** — raw, unmodified data as loaded by dlt from the OpenF1 API.
  Materialized as a **view**. No renaming, no casting, no filtering, no
  business logic of any kind — bronze is a thin SQL pass-through over the
  raw ingested table so the pipeline never re-copies untransformed data.
- **Silver** — cleaned, typed, deduplicated data. Materialized as a **table**.
  Column names and types are normalized here, natural keys are deduplicated
  (see [SQL Conventions](#sql-conventions)), and invalid/malformed rows are
  handled explicitly. `select *` is forbidden — every column must be named
  and, where it clarifies intent, aliased.
- **Gold** — the contract-facing output. Materialized as a **table**. This is
  what the ODCS contract describes and what `datacontract test` validates
  against. Business aggregation, joins across silver models, and
  contract-mandated derived fields (e.g. enum normalization, safety-car
  flags) live here. `select *` is forbidden here too.

## Project Structure

```text
f1_data_product/
├── CLAUDE.md
├── README.md
├── f1.duckdb              # local DuckDB database file (bronze/silver/gold schemas)
├── contracts/              # ODCS contracts — source of truth, written before code
│   ├── sessions.odcs.yaml
│   ├── lap_times.odcs.yaml
│   └── driver_season_stats.odcs.yaml
├── ingestion/               # dlt pipelines that load raw OpenF1 API data into bronze
└── dbt_f1/
    └── models/
        ├── bronze/          # views, 1:1 with raw ingested sources
        ├── silver/          # cleaned, typed, deduplicated tables
        └── gold/            # contract-facing output tables
```

- `contracts/` holds one ODCS YAML file per data product. Never edit these to
  match code — code is fixed to match them.
- `ingestion/` holds the dlt source/pipeline code that populates bronze tables
  from OpenF1 endpoints.
- `dbt_f1/` is the dbt project. Its `models/` directory is split into the three
  layers described above; each dbt model's schema (columns, types, tests)
  must trace back to a contract field.

## Data Contract Rules

- The ODCS contract is the source of truth for schema and quality — not the
  dbt model, not the dlt pipeline, not this file.
- If dbt output and the contract diverge, **fix dbt, never the contract.**
  The contract only changes when the upstream OpenF1 API itself changes in a
  way that requires it.
- A breaking schema change in the OpenF1 API (a field removed, renamed, or
  retyped that a contract depends on) **must trigger a contract version bump**
  (SemVer: major for removed/renamed/retyped fields, minor for new optional
  fields), with the ingestion/dbt pipeline updated in the same change. No
  gold-layer promotion is allowed while the contract and pipeline are out of
  sync.
- `datacontract test` must pass against the relevant contract before any
  model tied to that contract is considered done.

## SQL Conventions

- Lowercase keywords (`select`, `from`, `where`, `join`, not `SELECT`, `FROM`, ...).
- Explicit column aliases — no ambiguous or implicit column names, especially
  across joins.
- No `select *` in silver or gold models. Bronze views may pass through all
  raw columns since they do no transformation.
- Deduplication uses `row_number()` (not `rank()` or `dense_rank()`) unless a
  specific requirement calls for preserving ties, since `row_number()` always
  yields exactly one row per partition.

## Data Products and Dependency Order

Three gold-layer data products, each backed by a contract in `contracts/`.
They must be built in this order because each depends on the gold tables of
the ones before it:

1. **Sessions** (`contracts/sessions.odcs.yaml`)
   - Input: OpenF1 `/v1/sessions`.
   - Output: `gold.sessions`, one row per race-weekend session.
   - No dependency on other data products — build first.

2. **Lap Times** (`contracts/lap_times.odcs.yaml`)
   - Input ports: `gold.sessions` (join key `session_key`), plus OpenF1
     `/v1/laps`, `/v1/stints`, `/v1/pit`, `/v1/race_control`.
   - Output: `gold.lap_times`, one row per driver per lap per session.
   - Requires `gold.sessions` to be fully loaded and validated first.

3. **Driver Season Stats** (`contracts/driver_season_stats.odcs.yaml`)
   - Input ports: `gold.sessions` and `gold.lap_times` (joined on
     `session_key`), plus OpenF1 `/v1/drivers` for name/team resolution.
   - Output: `gold.driver_season_stats`, one row per driver per season.
   - Requires both `gold.sessions` and `gold.lap_times` to be fully loaded
     and validated first. This is the primary output port most downstream
     consumers query.

Strict build order: **Sessions → Lap Times → Driver Season Stats.**

## Workflow Per Data Product

Follow this exact sequence for each data product, in dependency order:

1. Read the relevant contract in `contracts/` in full — fields, types,
   nullability, quality checks, and any custom extension sections
   (`inputPorts`, `sourceEndpoints`, `aggregationLogic`, `upstreamStability`).
2. Scaffold dbt models (bronze → silver → gold) under `dbt_f1/models/` that
   implement the contract's schema, materialized per the
   [Layer Convention](#layer-convention).
3. Write or extend the dlt ingestion pipeline in `ingestion/` for the OpenF1
   endpoints the contract requires.
4. Run `dbt run` and `dbt test` — fix until green.
5. Run `datacontract test` against the contract — fix until green.
6. Commit and push — CI must pass.
7. Only then move to the next data product in the build order.

## OpenF1 API Notes

- Base URL: `https://api.openf1.org`. No authentication is required.
- Key endpoints and join keys:
  - `/v1/sessions` — feeds `gold.sessions`. Primary key: `session_key`.
  - `/v1/laps` — spine of `gold.lap_times`. Join/composite key:
    `session_key`, `driver_number`, `lap_number`.
  - `/v1/stints` — tyre compound. Joined on `session_key` + `driver_number`,
    resolved to a lap via the range `lap_start <= lap_number <= lap_end`.
  - `/v1/pit` — pit stop duration. Joined on `session_key`, `driver_number`,
    and `lap_number` (the pit endpoint's `lap_number` is the lap the car
    entered the pits, i.e. the `is_pit_out_lap` lap). Use `lane_duration`,
    not the deprecated `pit_duration` alias.
  - `/v1/race_control` — safety car / VSC windows. Not joined on
    `lap_number`; matched by time window
    `[date_start_of_lap, date_start_of_next_lap)`, filtered to
    `category IN ('SafetyCar', 'VSC')`.
  - `/v1/drivers` — driver name and team resolution for
    `gold.driver_season_stats`, using the most recent record per
    `(driver_number, year)`.
- **Unknown tyre compounds may appear outside the accepted values list**
  (`SOFT`, `MEDIUM`, `HARD`, `INTERMEDIATE`, `WET`). The pipeline must map
  any unrecognized, null, or empty compound string to `UNKNOWN` rather than
  passing through a raw value or dropping the row, and should log unmapped
  strings for monitoring.

## CI

GitHub Actions runs `dbt test` and `datacontract test` on every push. Failing
CI blocks a data product from being considered complete — a data product is
not "done" until both checks pass in CI, not just locally.

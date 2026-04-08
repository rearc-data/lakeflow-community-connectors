# Databricks notebook source
# MAGIC %md
# MAGIC # Snyk — direct **bronze** via Lakeflow (recommended)
# MAGIC
# MAGIC Detection use case: use the **`detections_unified`** connector table so ingestion lands in **one** UC Delta table
# MAGIC **`cyber_prod.bronze.snyk_events`** (no separate `cyber_prod.snyk.*` staging tables).
# MAGIC
# MAGIC | Column | Type | Meaning |
# MAGIC |--------|------|--------|
# MAGIC | `lw_id` | string | SHA-256 dedupe key |
# MAGIC | `time` | timestamp | Event time from source |
# MAGIC | `team_id` | string | Org / tenant |
# MAGIC | `data` | variant | Full JSON payload (same as `_raw`) |
# MAGIC | `_raw` | variant | Full JSON payload for silver path expressions |
# MAGIC | `rawstr` | string | Same payload as JSON text (Lakewatch / `parse_json` fallback) |
# MAGIC | `_metadata` | struct | Synthetic file metadata (`file_*`, `file_modification_time` timestamp) |
# MAGIC | `ingest_time_utc` | timestamp | Pipeline ingest time |
# MAGIC
# MAGIC **Pipeline spec:** `demo/snyk_demo_pipeline_spec.json` — single object `detections_unified` → `cyber_prod.bronze.snyk_events`.
# MAGIC Set `org_id` and optional `streams` (`issues,events,vulnerabilities`) in `table_configuration`.
# MAGIC
# MAGIC **Lakewatch:** `demo/snyk_events_preset.yaml` — bronze preTransform uses VARIANT `_raw` / `parse_json(rawstr)` then silver/gold (OCSF). The bronze table must be a **Delta table** (not a Materialized View) for streaming ingestion.
# MAGIC
# MAGIC **Legacy:** If you still have staging tables `cyber_prod.snyk.events`, migrate with a one-off batch job or drop them after cutover; this notebook no longer runs a DLT transform.

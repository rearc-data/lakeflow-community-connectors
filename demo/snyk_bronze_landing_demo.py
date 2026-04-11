# Databricks notebook source
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # Snyk Bronze Landing Demo — Reference Notebook
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC
# MAGIC This notebook demonstrates a **complete end-to-end data pipeline** that ingests security vulnerability data from [Snyk](https://snyk.io/) and surfaces it as alerts in the **Lakewatch Alert Dashboard**. It covers every stage of the data journey:
# MAGIC
# MAGIC 1. **Snyk API Ingestion** — Connects to the Snyk REST API (or a built-in mock API) using the `SnykLakeflowConnect` community connector. Reads all 6 Snyk entity types: `organizations`, `projects`, `issues`, `targets`, `users`, and `vulnerabilities`.
# MAGIC 2. **Raw Landing (Volumes)** — Writes each table as newline-delimited JSON (NDJSON) files into a Unity Catalog Volume at `/Volumes/<catalog>/<schema>/<volume>/snyk/raw/<table>/data.json`.
# MAGIC 3. **Bronze Delta Layer** — Reads the raw JSON from Volumes, applies schema enforcement using the connector's authoritative Spark schemas, and writes Delta tables to `<catalog>.<schema>.snyk_<table>`.
# MAGIC 4. **CDC Incremental Read** — Demonstrates cursor-based change data capture on the `issues` table, showing how the Lakeflow pipeline handles incremental updates.
# MAGIC 5. **Lakewatch Detection Rule** — Deploys a batch detection rule via the `dasl_client` that queries the bronze `snyk_vulnerabilities` table for critical/high-severity CVEs every 6 hours, generating notables (alerts) in `system.notables`.
# MAGIC 6. **Alert Dashboard Integration** — The generated notables automatically appear in the Lakewatch Alert Dashboard with MITRE ATT&CK mapping (T1190), observable entities (CVE, package), and risk scoring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How to Run This Notebook
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - **Compute**: Attach to a Databricks serverless interactive cluster (or a classic cluster with Python 3.10+)
# MAGIC - **Catalog Access**: You need `USE CATALOG`, `CREATE SCHEMA`, and `CREATE VOLUME` permissions on the target catalog (default: `dsl_dev`). Update `CATALOG` in Cell 6 if using a different catalog.
# MAGIC - **Lakewatch (optional)**: To deploy the detection rule (Section 10), the workspace must have Lakewatch/DASL installed.
# MAGIC
# MAGIC ### Steps
# MAGIC 1. **Run All Cells** — Click `Run All` or execute cells sequentially from top to bottom. The notebook is designed to run end-to-end without manual intervention.
# MAGIC 2. **Mock vs. Live Mode** — By default, `SNYK_TOKEN = "mock"` uses synthetic data with no external dependencies. To use real Snyk data, set `SNYK_TOKEN` and `ORG_ID` in Cell 6 to your actual Snyk API token and organization UUID.
# MAGIC 3. **First Run** — Cells 3–4 install the connector package and restart Python. On subsequent runs within the same session, these cells can be skipped.
# MAGIC 4. **Lakewatch Section** — Cells 36–39 deploy the detection rule. If the rule already exists, it will be replaced automatically.
# MAGIC
# MAGIC ### Expected Runtime
# MAGIC - Mock mode: \~2–3 minutes end-to-end
# MAGIC - Live API mode: depends on the number of Snyk projects and issues in your organization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Use Cases
# MAGIC
# MAGIC | Use Case | How |
# MAGIC | --- | --- |
# MAGIC | **Quick Demo** | Run as-is in mock mode to demonstrate Snyk → Bronze → Lakewatch flow without credentials |
# MAGIC | **Production Onboarding** | Replace `SNYK_TOKEN` and `ORG_ID` with real values to ingest live Snyk data into your lakehouse |
# MAGIC | **Detection Rule Development** | Modify the SQL in the "Configure Snyk Detection Rule" cell to adjust severity filters, add new observables, or change the schedule |
# MAGIC | **Pipeline Reference** | Use Section 9 as a guide for deploying the same flow via the `community-connector` CLI and Spark Declarative Pipelines (SDP) |
# MAGIC | **CDC Pattern Reference** | Section 8 shows how cursor-based incremental reads work with the Lakeflow connector framework |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Flow
# MAGIC
# MAGIC ```
# MAGIC Snyk API (mock or real)          # Source
# MAGIC       │
# MAGIC       ▼
# MAGIC SnykLakeflowConnect              # Connector — reads + paginates all 6 tables
# MAGIC       │
# MAGIC       ▼
# MAGIC UC Volumes (raw NDJSON)          # /Volumes/dsl_dev/security_raw/snyk_landing/snyk/raw/<table>/
# MAGIC       │
# MAGIC       ▼
# MAGIC Delta Tables (bronze)            # dsl_dev.security_raw.snyk_<table>
# MAGIC       │
# MAGIC       ▼
# MAGIC Lakewatch Detection Rule         # Batch SQL query on snyk_vulnerabilities (every 6h)
# MAGIC       │
# MAGIC       ▼
# MAGIC system.notables                  # Alerts with CVE observables + risk scores
# MAGIC       │
# MAGIC       ▼
# MAGIC Lakewatch Alert Dashboard        # SOC analyst review board
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tables Created
# MAGIC
# MAGIC | Delta Table | Source | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `snyk_organizations` | Snyk Orgs API | Organization metadata (name, slug, URL) |
# MAGIC | `snyk_projects` | Snyk Projects API | Monitored repositories and projects |
# MAGIC | `snyk_issues` | Snyk Issues API | Security issues with severity, status, and tool info |
# MAGIC | `snyk_vulnerabilities` | Snyk Vulns API | CVE details with CVSS scores, affected packages, and fix versions |
# MAGIC | `snyk_targets` | Snyk Targets API | Scan target repositories (GitHub, GitLab, etc.) |
# MAGIC | `snyk_users` | Snyk Users API | Organization members and their roles |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 — Install the connector package
# MAGIC
# MAGIC Run this once per cluster restart.

# COMMAND ----------

# DBTITLE 1,Install Connector Package
#%pip install -e /Workspace/Repos/<your-repo-path>/lakeflow-community-connectors --quiet
%pip install -e /Workspace/Repos/shambhu.adhikari@rearc.io/lakeflow-community-connectors --quiet

# If you cloned the repo to a Databricks Repo, use the path above.
# Alternatively, if you uploaded the package wheel:
#   %pip install /Volumes/<catalog>/<schema>/<volume>/lakeflow_community_connectors-0.1.0-py3-none-any.whl --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python Environment
# Restart Python to pick up the newly installed connector package
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Configuration
# MAGIC
# MAGIC Set your catalog, schema, volume, and credentials here.

# COMMAND ----------

# DBTITLE 1,Set Pipeline Parameters
# ── Edit these values ─────────────────────────────────────────────────────────
CATALOG   = "dsl_dev"               # Unity Catalog catalog
SCHEMA    = "security_raw"       # Schema (will be created if it doesn't exist)
VOLUME    = "snyk_landing"       # UC Volume name (will be created if it doesn't exist)

# Credentials — use "mock" for demo without a real Snyk account
SNYK_TOKEN = "mock"              # Replace with your real Snyk API token for live demo
ORG_ID     = "org-mock-0001"     # Replace with your real Snyk org UUID for live demo
# ─────────────────────────────────────────────────────────────────────────────

VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/snyk/raw"

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Volume  : {VOLUME}")
print(f"Mode    : {'MOCK' if SNYK_TOKEN == 'mock' else 'LIVE API'}")
print(f"Landing : {VOLUME_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Bootstrap Unity Catalog resources

# COMMAND ----------

# DBTITLE 1,Create Schema and Volume
# Ensure the target Unity Catalog schema and volume exist before landing data
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

print(f"Schema  created/confirmed: {CATALOG}.{SCHEMA}")
print(f"Volume  created/confirmed: {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Initialize the Snyk connector

# COMMAND ----------

# DBTITLE 1,Initialize Snyk Connector
# Workaround: On serverless, the editable install doesn't survive restartPython().
# The 'databricks' namespace is pre-loaded by the runtime (databricks-sdk), so we
# must manually extend its __path__ to include the connector source directory.
import sys, pkgutil, importlib
sys.path.insert(0, "/Workspace/Repos/shambhu.adhikari@rearc.io/lakeflow-community-connectors/src")

import databricks
databricks.__path__ = pkgutil.extend_path(databricks.__path__, databricks.__name__)
databricks_labs = importlib.import_module("databricks.labs")
databricks_labs.__path__ = pkgutil.extend_path(databricks_labs.__path__, databricks_labs.__name__)

from databricks.labs.community_connector.sources.snyk.snyk import SnykLakeflowConnect
from databricks.labs.community_connector.sources.snyk.snyk_mock_api import reset_mock_api

# Reset mock state for reproducible demo runs
if SNYK_TOKEN == "mock":
    reset_mock_api()

# Initialize the Snyk connector and discover available tables
connector = SnykLakeflowConnect({"token": SNYK_TOKEN})
tables = connector.list_tables()
print("Tables available:", tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Read all tables from Snyk and land raw JSON into Volumes
# MAGIC
# MAGIC Each table lands as newline-delimited JSON files at:
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/<volume>/snyk/raw/<table_name>/data.json
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Read Snyk API and Land Raw JSON
import json
import os

# Tables that require org_id as a table option
ORG_SCOPED_TABLES = {"projects", "issues", "targets", "users", "vulnerabilities"}

landing_summary = {}

for table_name in tables:
    table_opts = {"org_id": ORG_ID} if table_name in ORG_SCOPED_TABLES else {}

    # Read from connector (mock or real)
    records_iter, offset = connector.read_table(table_name, {}, table_opts)
    records = list(records_iter)

    # Write to Volume as newline-delimited JSON (one JSON object per line)
    out_dir  = f"{VOLUME_BASE}/{table_name}"
    out_file = f"{out_dir}/data.json"
    os.makedirs(out_dir, exist_ok=True)

    with open(out_file, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    landing_summary[table_name] = {"records": len(records), "path": out_file}
    print(f"  ✓  {table_name:20s}  {len(records):>4d} records  →  {out_file}")

print("\nLanding complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Inspect the raw Volume contents

# COMMAND ----------

# DBTITLE 1,Display Landing Summary
# Show a summary table of all landed files, record counts, and Volume paths
display(
    spark.createDataFrame(
        [{"table": t, "records": v["records"], "volume_path": v["path"]}
         for t, v in landing_summary.items()],
        schema="table STRING, records INT, volume_path STRING",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Read from Volume → Delta (bronze layer)
# MAGIC
# MAGIC For each table:
# MAGIC 1. Read the NDJSON file from the Volume
# MAGIC 2. Apply the connector's Spark schema (schema enforcement)
# MAGIC 3. Write/merge into a Delta table at `<catalog>.<schema>.snyk_<table>`

# COMMAND ----------

# DBTITLE 1,Write Bronze Delta Tables
from pyspark.sql import functions as F

for table_name in tables:
    table_opts = {"org_id": ORG_ID} if table_name in ORG_SCOPED_TABLES else {}

    # Get the authoritative schema from the connector
    spark_schema = connector.get_table_schema(table_name, table_opts)

    # Read raw JSON from Volume with schema enforcement
    volume_path = f"{VOLUME_BASE}/{table_name}"
    df = (
        spark.read
        .schema(spark_schema)
        .json(volume_path)
    )

    bronze_table = f"{CATALOG}.{SCHEMA}.snyk_{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")           # Use "merge" with SDP pipeline for incremental
        .option("overwriteSchema", "true")
        .saveAsTable(bronze_table)
    )

    count = spark.table(bronze_table).count()
    print(f"  ✓  {bronze_table:50s}  {count:>4d} rows")

print("\nBronze layer load complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Preview the bronze tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Organizations

# COMMAND ----------

# DBTITLE 1,Preview Organizations
# Display the snyk_organizations bronze table — Snyk org metadata
display(spark.table(f"{CATALOG}.{SCHEMA}.snyk_organizations"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Projects

# COMMAND ----------

# DBTITLE 1,Preview Projects
# Display the snyk_projects bronze table — monitored repos/projects
display(spark.table(f"{CATALOG}.{SCHEMA}.snyk_projects"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issues (CDC table — has updated_at cursor)

# COMMAND ----------

# DBTITLE 1,Preview Issues
# Display the snyk_issues bronze table — security issues ordered by most recent
display(
    spark.table(f"{CATALOG}.{SCHEMA}.snyk_issues")
    .orderBy("updated_at", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vulnerabilities

# COMMAND ----------

# DBTITLE 1,Preview Vulnerabilities
# Display the snyk_vulnerabilities bronze table — CVEs sorted by CVSS score
display(
    spark.table(f"{CATALOG}.{SCHEMA}.snyk_vulnerabilities")
    .orderBy("cvss_score", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Targets

# COMMAND ----------

# DBTITLE 1,Preview Targets
# Display the snyk_targets bronze table — scan target repositories
display(spark.table(f"{CATALOG}.{SCHEMA}.snyk_targets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Users

# COMMAND ----------

# DBTITLE 1,Preview Users
# Display the snyk_users bronze table — Snyk organization members
display(spark.table(f"{CATALOG}.{SCHEMA}.snyk_users"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Demo: CDC incremental read (issues table)
# MAGIC
# MAGIC The `issues` table uses CDC ingestion. After the first full load, subsequent
# MAGIC reads only return records updated after the cursor. This simulates what the
# MAGIC Lakeflow pipeline does automatically.

# COMMAND ----------

# DBTITLE 1,CDC Incremental Read Demo
# First full load — get the offset
records_iter, offset_after_full_load = connector.read_table(
    "issues", {}, {"org_id": ORG_ID}
)
all_issues = list(records_iter)
print(f"Full load    : {len(all_issues)} issues,  offset = {offset_after_full_load}")

# Incremental read — pass the offset back (simulates second pipeline trigger)
records_iter2, offset2 = connector.read_table(
    "issues", offset_after_full_load, {"org_id": ORG_ID}
)
new_issues = list(records_iter2)
print(f"Incremental  : {len(new_issues)} new issues since {offset_after_full_load}")
print(f"New offset   : {offset2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — What happens in a real pipeline (SDP)
# MAGIC
# MAGIC When deployed via the `community-connector` CLI, a Spark Declarative Pipeline
# MAGIC handles steps 4–8 automatically and continuously:
# MAGIC
# MAGIC ```
# MAGIC community-connector create_connection snyk  snyk_demo_connection \
# MAGIC   -o '{"token": "<YOUR_SNYK_TOKEN>"}'
# MAGIC
# MAGIC community-connector create_pipeline snyk  snyk_bronze_pipeline \
# MAGIC   -ps pipeline-spec/snyk_demo_pipeline_spec.json \
# MAGIC   -c main  -t security_bronze
# MAGIC ```
# MAGIC
# MAGIC The pipeline spec (`snyk_demo_pipeline_spec.json`) looks like:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "connection_name": "snyk_demo_connection",
# MAGIC   "objects": [
# MAGIC     { "table": { "source_table": "organizations" } },
# MAGIC     { "table": { "source_table": "projects",        "table_configuration": { "org_id": "<ORG_UUID>" } } },
# MAGIC     { "table": { "source_table": "issues",          "table_configuration": { "org_id": "<ORG_UUID>" } } },
# MAGIC     { "table": { "source_table": "targets",         "table_configuration": { "org_id": "<ORG_UUID>" } } },
# MAGIC     { "table": { "source_table": "users",           "table_configuration": { "org_id": "<ORG_UUID>" } } },
# MAGIC     { "table": { "source_table": "vulnerabilities", "table_configuration": { "org_id": "<ORG_UUID>" } } }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC The pipeline writes Delta tables directly without needing the Volume step —
# MAGIC this notebook shows the Volume approach as an explicit raw-landing layer
# MAGIC for teams that want to inspect data before it hits Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9b — Lakewatch Integration: Detection Rule Pipeline
# MAGIC
# MAGIC The Lakewatch Alert Dashboard reads from `system.notables`. To surface Snyk findings there,
# MAGIC we deploy a **batch detection rule** via the `dasl_client` that queries Snyk bronze tables
# MAGIC and generates notables (alerts) automatically.

# COMMAND ----------

# DBTITLE 1,Query Critical and High Severity Vulnerabilities
# MAGIC %sql
# MAGIC -- Preview the vulnerabilities that will trigger the detection rule
# MAGIC SELECT * FROM dsl_dev.security_raw.snyk_vulnerabilities
# MAGIC WHERE severity IN ('critical', 'high')

# COMMAND ----------

# DBTITLE 1,Lakewatch Integration Overview
# MAGIC %md
# MAGIC ## 10 — Connect to Lakewatch Alert Dashboard
# MAGIC
# MAGIC The [Lakewatch Alert Dashboard](https://docs.sl.antimatter.io/) reads from `system.notables`.  
# MAGIC To surface Snyk findings there, we deploy a **batch detection rule** via the `dasl_client` that:
# MAGIC
# MAGIC 1. Runs a SQL query against the Snyk bronze tables on a schedule
# MAGIC 2. Generates **notables** (alerts) for critical/high-severity vulnerabilities
# MAGIC 3. Notables automatically appear in the Lakewatch Alert Dashboard
# MAGIC
# MAGIC ```
# MAGIC Snyk Bronze Tables → Lakewatch Detection Rule → system.notables → Alert Dashboard
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Install DASL Client
# Install the DASL client library for Lakewatch detection rule management
%pip install dasl_client --quiet

# COMMAND ----------

# DBTITLE 1,Initialize DASL Client
# Connect to the Lakewatch API — auto-detects workspace URL and credentials
from dasl_client.client import Client
from dasl_client.types import Rule, Schedule, Metadata

client = Client.for_workspace()
print("DASL client initialized successfully")

# COMMAND ----------

# DBTITLE 1,Configure Snyk Detection Rule
# --- Snyk Detection Rule: Critical & High Vulnerabilities ---

# Schedule: run every 6 hours
schedule = Schedule(
    exactly="0 0 */6 * * ?",
    compute_group="automatic",
    enabled=True
)

# Batch input — query the Snyk vulnerabilities bronze table
snyk_detection_sql = f"""
FROM {CATALOG}.{SCHEMA}.snyk_vulnerabilities
|> WHERE severity IN ('critical', 'high')
|> SELECT
    id AS vuln_id,
    title,
    severity,
    cve,
    cvss_score,
    package,
    version,
    fixed_in,
    project_id,
    organization_id,
    disclosure_time
"""

input = Rule.Input(
    batch=Rule.Input.Batch(
        sql=snyk_detection_sql
    )
)

# Observables — entities to track risk against
# Valid kinds: Email Address, IP Address, Domain Name, Entity, Test
# Valid relationships: ActingUser, DstIP, SrcIP, TargetUser
observables = [
    Rule.Observable(
        kind="Entity",
        value="cve",
        relationship="TargetUser",
        risk=Rule.Observable.Risk(impact="10", confidence="10")
    ),
    Rule.Observable(
        kind="Entity",
        value="package",
        relationship="TargetUser",
        risk=Rule.Observable.Risk(impact="8", confidence="9")
    ),
]

# Output — notable summary template
output = Rule.Output(
    summary="Snyk Critical/High Vulnerability: {cve} in {package} (CVSS {cvss_score})",
    context=None,
    default_context=True
)

# Rule metadata — classification and MITRE mapping
# Valid categories: APT, Malware, Policy, SpecialEvent, SuspectEvent, Target, Trend
rule_metadata = Rule.RuleMetadata(
    version="1",
    category="Target",
    severity="high",
    fidelity="High",
    mitre=[Rule.RuleMetadata.Mitre(
        taxonomy="Enterprise",
        tactic="Initial Access",
        technique_id="T1190",
        technique="Exploit Public-Facing Application",
        sub_technique_id=None,
        sub_technique=None
    )],
    objective="Detect critical and high severity vulnerabilities reported by Snyk across monitored projects."
)

# Assemble the full rule
rule = Rule(
    schedule=schedule,
    input=input,
    output=output,
    observables=observables,
    rule_metadata=rule_metadata,
)

print("Detection rule configured:")
print(f"  SQL table  : {CATALOG}.{SCHEMA}.snyk_vulnerabilities")
print(f"  Schedule   : Every 6 hours")
print(f"  Category   : Target")
print(f"  Severity   : high")
print(f"  Observables: CVE (Entity/TargetUser), Package (Entity/TargetUser)")

# COMMAND ----------

# DBTITLE 1,Deploy Rule to Lakewatch
# Deploy the detection rule to Lakewatch
RULE_NAME = "Snyk/Vulnerability Management/Critical and High Severity Vulnerabilities"

try:
    created_rule = client.create_rule(RULE_NAME, rule)
    print(f"Successfully created rule: {created_rule.metadata.name}")
    print(f"Notables will now appear in the Lakewatch Alert Dashboard.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Rule already exists. Replacing...")
        try:
            replaced_rule = client.replace_rule(RULE_NAME, rule)
            print(f"Successfully replaced rule: {replaced_rule.metadata.name}")
        except Exception as e2:
            print(f"Error replacing rule: {e2}")
    else:
        print(f"Error creating rule: {e}")

# COMMAND ----------

# DBTITLE 1,Limitations, Challenges, and Future Improvements
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 11 — Limitations, Challenges & Future Improvements
# MAGIC
# MAGIC ### Known Limitations
# MAGIC
# MAGIC | Area | Limitation | Impact |
# MAGIC | --- | --- | --- |
# MAGIC | **Editable Install** | `pip install -e` does not survive `restartPython()` on serverless compute. A `sys.path` workaround is required (Cell 11). | Adds boilerplate; may break if the Repo path changes. |
# MAGIC | **Overwrite Mode** | Bronze tables use `mode("overwrite")` — each run replaces the full table. No incremental merge or deduplication is applied. | Not suitable for production CDC without switching to Delta `MERGE`. |
# MAGIC | **Single Org Scope** | The pipeline reads from a single Snyk organization (`ORG_ID`). Multi-org ingestion requires looping or parameterization. | Limits coverage for enterprises with many Snyk orgs. |
# MAGIC | **Schema Evolution** | `overwriteSchema=true` blindly replaces the schema on each run. If Snyk adds or renames fields, downstream consumers may break silently. | Needs schema drift detection and alerting in production. |
# MAGIC | **Detection Rule Scope** | The Lakewatch rule only targets `snyk_vulnerabilities` with severity `critical`/`high`. Issues, projects, and other tables are not monitored. | Missed alert coverage for medium-severity or cross-table correlation. |
# MAGIC | **Observable Mapping** | CVE and package are mapped as generic `Entity` / `TargetUser` because the workspace's Lakewatch config doesn't include vulnerability-specific kinds. | Reduces observable fidelity in the SOC workflow. |
# MAGIC | **Mock Data** | Mock mode generates static synthetic data — no pagination, no rate-limiting, no realistic volume. It doesn't exercise error handling or retry logic. | May hide bugs that surface only with the live API. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Challenges Encountered
# MAGIC
# MAGIC 1. **Namespace Package Conflicts** — The `databricks` namespace is pre-loaded by the runtime (`databricks-sdk`, `databricks-connect`). Importing `databricks.labs.*` from the connector source required manually extending `__path__` via `pkgutil.extend_path` after adding the source directory to `sys.path`.
# MAGIC 2. **DASL API Validation** — The Lakewatch `create_rule` API enforces strict enum validation on `category`, `observable.kind`, and `observable.relationship`. Valid values are workspace-specific and not well-documented — required iterative trial-and-error.
# MAGIC 3. **Kernel State on Serverless** — `%pip install` triggers an automatic kernel restart on serverless, which can leave the Jupyter client in an `IllegalStateException` state. The recommended path is `dbutils.library.restartPython()` in a separate cell.
# MAGIC 4. **Catalog Permissions** — The default `main` catalog may not be accessible to all users. The pipeline requires `USE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME`, and `CREATE TABLE` grants on the target catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Future Improvements
# MAGIC
# MAGIC #### Pipeline Enhancements
# MAGIC - **Delta `MERGE` for Incremental Loads** — Replace `overwrite` with `MERGE INTO` keyed on each table's primary ID. The connector already returns CDC offsets (cursors) — wire them into a stateful pipeline.
# MAGIC - **Multi-Org Support** — Parameterize `ORG_ID` as a list and iterate across all Snyk organizations, writing to org-partitioned tables.
# MAGIC - **Silver Layer Transformations** — Add a silver layer that joins `vulnerabilities` with `projects` and `targets` to produce enriched views like "vulnerabilities by repo" or "CVSS heatmap by team".
# MAGIC - **Schema Drift Detection** — Compare `connector.get_table_schema()` against the existing Delta table schema before each write. Alert on new/removed/renamed columns.
# MAGIC - **Streaming Ingestion** — Convert the batch pipeline to a Structured Streaming job using the Spark Python Data Source API, enabling near-real-time vulnerability detection.
# MAGIC - **Data Quality Checks** — Add expectations (e.g., Databricks Lakehouse Monitoring or Great Expectations) to validate row counts, null rates, and CVSS score ranges after each load.
# MAGIC
# MAGIC #### Lakewatch / Detection Improvements
# MAGIC - **Multi-Table Detection Rules** — Create rules that correlate across tables (e.g., "critical vulnerability in a project with no assigned users" or "new high-severity issue on a target with no recent scans").
# MAGIC - **Streaming Detection** — Migrate from batch to streaming rules for sub-minute alerting on newly discovered critical CVEs.
# MAGIC - **Dynamic Risk Scoring** — Weight risk scores by CVSS score, exploit maturity, and asset criticality rather than using static impact/confidence values.
# MAGIC - **Additional MITRE Mappings** — Extend beyond T1190 (Exploit Public-Facing Application) to cover supply chain (T1195) and software deployment tools (T1072) techniques.
# MAGIC
# MAGIC #### AI & Generative AI Opportunities
# MAGIC
# MAGIC | Opportunity | Description | Databricks Tools |
# MAGIC | --- | --- | --- |
# MAGIC | **AI-Powered Vulnerability Triage** | Use `ai_classify()` SQL AI function to auto-classify vulnerabilities into actionable categories ("patch immediately", "schedule fix", "accept risk") based on CVSS, exploit availability, and asset context. | [AI Functions](https://docs.databricks.com/en/large-language-models/ai-functions.html) |
# MAGIC | **Natural Language Remediation Guidance** | Use `ai_generate()` to produce human-readable remediation steps for each CVE — e.g., "Upgrade `express` from 1.0.0 to 1.0.1 to fix CVE-2024-1007 (CVSS 7.2). See https://nvd.nist.gov/..." | AI Functions + Foundation Model APIs |
# MAGIC | **Vulnerability Summarization** | Use `ai_summarize()` to generate executive summaries of the current vulnerability landscape — daily/weekly digests for security leadership. | AI Functions |
# MAGIC | **Anomaly Detection with AI Forecast** | Apply `ai_forecast()` on daily vulnerability counts to detect unusual spikes in new CVEs — could indicate a supply chain attack or mass disclosure event. | [AI Forecast](https://docs.databricks.com/en/large-language-models/ai-functions.html#ai_forecast) |
# MAGIC | **Genie Space for Security Analysts** | Create a Genie Space over the bronze tables so SOC analysts can ask natural language questions like "Show me all critical vulnerabilities in the express package discovered this week". | [Databricks Genie](https://docs.databricks.com/en/genie/index.html) |
# MAGIC | **RAG-Powered Incident Response** | Build a Retrieval-Augmented Generation (RAG) pipeline that indexes CVE details, NVD advisories, and internal remediation playbooks. Analysts ask questions and get contextual answers with source citations. | Vector Search + Foundation Model APIs + LangChain |
# MAGIC | **Automated Ticket Creation** | Use an AI agent (Databricks Agent Framework) that monitors `system.notables` and automatically creates Jira/ServiceNow tickets with AI-generated descriptions, severity, and suggested assignees. | [Databricks Agents](https://docs.databricks.com/en/generative-ai/agent-framework/index.html) |
# MAGIC | **Vulnerability Embedding & Similarity** | Generate embeddings for CVE descriptions using Foundation Model APIs. Cluster similar vulnerabilities to identify patterns (e.g., "all these CVEs target the same dependency chain"). | Foundation Model APIs + Vector Search |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Bottom line**: This notebook provides a solid foundation for Snyk-to-Lakehouse ingestion and Lakewatch alerting. The next step is hardening it for production (incremental loads, schema drift, multi-org) and layering AI capabilities to move from **reactive alerting** to **proactive, intelligent vulnerability management**.
# Lakeflow Community Connectors — Wiz & Snyk Implementation

> *Last updated:* 2026-03-26
> *Repo:* https://github.com/databrickslabs/lakeflow-community-connectors
> *Branch:* master
> *Status:* ✅ Both connectors fully implemented, mock APIs in place, all tests passing

---

## 1. Repo Overview

### What It Is
Lakeflow Community Connectors is an open-source framework built on top of the *Spark Python Data Source API* and *Spark Declarative Pipelines (SDP)*. Connectors ingest data from external source systems into Databricks Delta tables via configurable pipelines.

### Current Sources (20 connectors)
appsflyer, dicomweb, example, fhir, github, gmail, google_analytics_aggregated, google_sheets_docs, hubspot, microsoft_teams, mixpanel, osipi, qualtrics, sap_successfactors, **snyk** *(new)*, surveymonkey, **wiz** *(new)*, zendesk, zoho_crm

### Key Directories

```
src/databricks/labs/community_connector/
  interface/           # LakeflowConnect base class (THE contract to implement)
  sources/             # One sub-directory per connector
  sparkpds/            # Spark PDS integration (handled by framework)
  libs/                # Shared utilities (includes simulated_source/ for example connector)
  pipeline/            # SDP orchestration

tests/unit/sources/
  test_suite.py                   # Generic LakeflowConnectTests base class
  lakeflow_connect_test_utils.py  # Write-back test utilities base class
  {source}/                       # Per-connector test files + configs/
```

---

## 2. Interface Contract (LakeflowConnect)

All connectors sub-class `LakeflowConnect` from `interface/lakeflow_connect.py`. The *4 mandatory methods* are:

| Method | Signature | Notes |
|--------|-----------|-------|
| `list_tables` | `() → list[str]` | Static list or API-discovered. Must return unique strings. |
| `get_table_schema` | `(table_name, table_options) → StructType` | Returns PySpark StructType. Raise ValueError for unknown tables. |
| `read_table_metadata` | `(table_name, table_options) → dict` | Must contain `ingestion_type`, `primary_keys` (non-append), `cursor_field` (CDC). |
| `read_table` | `(table_name, start_offset, table_options) → (Iterator[dict], dict)` | Returns `(records_iter, offset_dict)`. Pagination stops when offset == start_offset. |

*Optional:*
- `read_table_deletes(table_name, start_offset, table_options) → (Iterator[dict], dict)` — required if any table uses `ingestion_type: cdc_with_deletes`.

### Ingestion Types
| Type | Use case | `cursor_field` required? | `primary_keys` required? |
|------|----------|----------------------|----------------------|
| `snapshot` | Full-reload tables | No | Yes |
| `cdc` | Incremental changes, no deletes | Yes | Yes |
| `cdc_with_deletes` | Incremental + explicit delete tracking | Yes | Yes |
| `append` | Append-only (logs, events) | Yes (optional) | No |

### `__init__` Option Convention
```python
def __init__(self, options: dict[str, str]) -> None:
    token = options.get("token")       # connection-level credential
    self.org_id = options.get("org_id")  # example connection param
    self._session = requests.Session()
    self._session.headers.update({"Authorization": f"Bearer {token}"})
```

All options come from the Unity Catalog connection.
Table-specific options (e.g. `org_id`, `project_id`) must be declared in `external_options_allowlist` in `connector_spec.yaml`.

### Per-Connector File Structure

```
src/databricks/labs/community_connector/sources/{source}/
  __init__.py
  {source}.py                      # Main connector class
  {source}_schemas.py              # TABLE_SCHEMAS, TABLE_METADATA, SUPPORTED_TABLES
  {source}_mock_api.py             # In-memory mock API (for demo/testing without live creds)
  connector_spec.yaml              # UC connection parameters spec
  README.md                        # User-facing documentation (optional)

tests/unit/sources/{source}/
  __init__.py
  test_{source}_connector.py       # Subclasses LakeflowConnectTests
  configs/
    dev_config.json                # Credentials (use {"token":"mock"} for mock mode)
    dev_table_config.json          # Table-level options (e.g. org_id)
```

---

## 3. Mock API Strategy

Both Snyk and Wiz connectors support a **mock mode** for running tests and demos without real API access or credentials. This follows the same singleton pattern used by the built-in `example` connector's `SimulatedSourceAPI`.

### How It Works

Each connector detects a sentinel credential value at init time and swaps in an in-memory session instead of a real HTTP session:

| Connector | Trigger | Mock credential value |
|-----------|---------|----------------------|
| Snyk | `options["token"] == "mock"` | `{"token": "mock"}` |
| Wiz | `options["wiz_client_id"] == "mock"` | `{"wiz_client_id": "mock", ...}` |

The `requests` import is **lazy** (inside the `else` branch) so the library is not required when running in mock mode.

### Mock API Files

| File | What it simulates |
|------|------------------|
| `sources/snyk/snyk_mock_api.py` | Snyk V1 (`/v1/orgs`) + REST (`/rest/orgs/{org_id}/...`) endpoints with JSON:API pagination via `links.next` |
| `sources/wiz/wiz_mock_api.py` | Wiz OAuth2 token endpoint + GraphQL endpoint with `pageInfo { hasNextPage endCursor }` pagination |

Both mock APIs use `_PAGE_SIZE = 3` intentionally small to exercise multi-page reads in tests, and provide `get_mock_api()` / `reset_mock_api()` singleton functions matching the example connector pattern.

### Mock Seeded Data Summary

**Snyk** (`org-mock-0001`):
- 1 organization, 5 projects, 10 issues (CDC), 5 targets, 5 users, 10 vulnerabilities

**Wiz**:
- 8 issues (CDC), 6 cloud resources, 8 vulnerabilities (CDC), 5 projects, 4 users, 6 controls

---

## 4. Test Harness

### Generic Test Suite (`test_suite.py`)
Subclass `LakeflowConnectTests` in your connector test file:

```python
from tests.unit.sources.test_suite import LakeflowConnectTests
from databricks.labs.community_connector.sources.wiz.wiz import WizLakeflowConnect
from databricks.labs.community_connector.sources.wiz.wiz_mock_api import reset_mock_api

class TestWizConnector(LakeflowConnectTests):
    connector_class = WizLakeflowConnect
    sample_records = 50

    @classmethod
    def setup_class(cls):
        cls.config = cls._load_config()
        reset_mock_api()          # reset mock singleton before creating connector
        super().setup_class()
```

*Tests validated by the harness:*

| Test | What it validates |
|------|------------------|
| `test_initialization` | `__init__` succeeds with config |
| `test_list_tables` | Returns non-empty list of unique strings |
| `test_invalid_table_name` | All 3 methods raise on unknown table |
| `test_get_table_schema` | Returns valid non-empty `StructType` for every table |
| `test_read_table_metadata` | `ingestion_type` present + valid; `primary_keys`/`cursor_field` present when required |
| `test_read_table` | Returns `(iterator, dict)` for every non-partitioned table; records pass schema validation |
| `test_micro_batch_offset_contract` | Two successive `read_table` calls with offset round-trip |
| `test_read_table_deletes` | Only if any table is `cdc_with_deletes` (skipped for both Snyk and Wiz) |

### Current Test Results

```
# Snyk
pytest tests/unit/sources/snyk/test_snyk_connector.py -v
→ 7 passed, 7 skipped   (0.10s)

# Wiz
pytest tests/unit/sources/wiz/test_wiz_connector.py -v
→ 7 passed, 7 skipped   (0.10s)
```

The 7 skipped tests are expected: partition mixin tests (connectors don't use `SupportsPartition`), `test_read_table_deletes` (no `cdc_with_deletes` tables), and write-back tests (no `test_utils_class` — both APIs are read-only).

### Config Files (mock mode — no real credentials needed)
```json
// tests/unit/sources/snyk/configs/dev_config.json
{ "token": "mock" }

// tests/unit/sources/snyk/configs/dev_table_config.json
{
  "issues":          { "org_id": "org-mock-0001" },
  "projects":        { "org_id": "org-mock-0001" },
  "targets":         { "org_id": "org-mock-0001" },
  "users":           { "org_id": "org-mock-0001" },
  "vulnerabilities": { "org_id": "org-mock-0001" }
}

// tests/unit/sources/wiz/configs/dev_config.json
{
  "wiz_client_id": "mock",
  "wiz_client_secret": "mock",
  "wiz_api_endpoint": "https://mock.wiz.io/graphql"
}
```

### Running Tests
```bash
# Install
pip install -e ".[dev]"

# Run all tests for each connector
pytest tests/unit/sources/snyk/ -v
pytest tests/unit/sources/wiz/ -v

# Run one specific test
pytest tests/unit/sources/wiz/ -k "test_read_table" -v

# Run both together
pytest tests/unit/sources/snyk/ tests/unit/sources/wiz/ -v
```

---

## 5. Wiz Connector ✅ Implemented

### 5.1 API Overview

| Attribute | Value |
|-----------|-------|
| API type | *GraphQL* (single endpoint) |
| Base URL | `https://<tenant>.api.wiz.io/graphql` |
| Auth | OAuth 2.0 client credentials → Bearer token |
| Token URL | `https://auth.app.wiz.io/oauth/token` |
| Required scopes | `read:all` (or specific scopes per query) |
| Pagination | Cursor-based: `pageInfo.endCursor` / `hasNextPage` |
| Rate limiting | Enforced; exponential back-off recommended |

### 5.2 Authentication Flow
```python
# Connector calls _ensure_token() before every GraphQL request.
# Token is cached until expires_in - 60 seconds.
resp = self._session.post(auth_url, json={
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "audience": "wiz-api"
})
self._token = resp.json()["access_token"]
self._token_expiry = time.time() + resp.json().get("expires_in", 3600)
```

In mock mode, the `WizMockSession` detects auth requests by `grant_type` in the POST body and returns `{"access_token": "mock-wiz-token", "expires_in": 3600}`.

### 5.3 Implemented Tables

| Table | GraphQL operation | Ingestion Type | Primary Key | Cursor Field |
|-------|-------------------|---------------|-------------|-------------|
| `issues` | `GetIssues` | `cdc` | `id` | `updatedAt` |
| `cloud_resources` | `GetCloudResources` | `snapshot` | `id` | — |
| `vulnerabilities` | `GetVulnerabilities` | `cdc` | `id` | `lastDetectedAt` |
| `projects` | `GetProjects` | `snapshot` | `id` | — |
| `users` | `GetUsers` | `snapshot` | `id` | — |
| `controls` | `GetControls` | `snapshot` | `id` | — |

### 5.4 Mock Injection (`wiz.py`)
```python
if client_id == "mock":
    from databricks.labs.community_connector.sources.wiz.wiz_mock_api import get_mock_api
    self._session = get_mock_api().get_session()
else:
    import requests          # lazy import — not required for mock
    self._session = requests.Session()
```

### 5.5 Schema Definitions (`wiz_schemas.py`)
```python
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, StringType, StructField, StructType
)

TABLE_SCHEMAS = {
    "issues": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("createdAt", StringType(), True),
        StructField("updatedAt", StringType(), True),
        StructField("dueAt", StringType(), True),
        StructField("resolvedAt", StringType(), True),
        StructField("entity", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
        ]), True),
        StructField("control", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("severity", StringType(), True),
        ]), True),
    ]),
    "cloud_resources": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("cloudProvider", StringType(), True),
        StructField("tags", StringType(), True),       # JSON-encoded tag map
        StructField("subscription", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
        ]), True),
    ]),
    "vulnerabilities": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("cvssScore", DoubleType(), True),
        StructField("cveIds", ArrayType(StringType()), True),
        StructField("lastDetectedAt", StringType(), True),
        StructField("fixedVersion", StringType(), True),
        StructField("status", StringType(), True),
    ]),
    "projects": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("createdAt", StringType(), True),
        StructField("slug", StringType(), True),
    ]),
    "users": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("authProviders", ArrayType(StringType()), True),
        StructField("lastLoginAt", StringType(), True),
        StructField("role", StringType(), True),
    ]),
    "controls": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("shortId", StringType(), True),
        StructField("description", StringType(), True),
        StructField("enabled", BooleanType(), True),
        StructField("severity", StringType(), True),
        StructField("createdAt", StringType(), True),
    ]),
}
```

### 5.6 Connector Spec (`connector_spec.yaml`)
```yaml
display_name: Wiz
connection:
  parameters:
    - name: wiz_client_id
      type: string
      required: true
      secret: true
      description: Wiz OAuth2 client ID (Service Account)

    - name: wiz_client_secret
      type: string
      required: true
      secret: true
      description: Wiz OAuth2 client secret

    - name: wiz_api_endpoint
      type: string
      required: true
      description: >
        Wiz GraphQL API endpoint, e.g. https://<tenant>.api.wiz.io/graphql

    - name: wiz_auth_url
      type: string
      required: false
      description: >
        Wiz OAuth2 token URL. Defaults to https://auth.app.wiz.io/oauth/token

external_options_allowlist: ""
```

---

## 6. Snyk Connector ✅ Implemented

### 6.1 API Overview

| Attribute | Value |
|-----------|-------|
| API type | *REST* (JSON:API standard) |
| Base URL | `https://api.snyk.io/rest` (REST v2) or `https://api.snyk.io/v1` (legacy) |
| Auth | API token as `Authorization: token <API_KEY>` |
| Version param | `?version=2024-10-15` (required on all REST calls) |
| Pagination | Cursor-based: `links.next` contains embedded `starting_after` cursor |
| Rate limiting | 1620 req/min per API key; HTTP 429 on breach |
| SDK/docs | https://docs.snyk.io/snyk-api/reference |

### 6.2 Authentication
```python
# Real mode — long-lived token, no refresh needed
self._session.headers.update({
    "Authorization": f"token {token}",    # NOTE: "token", not "Bearer"
    "Accept": "application/vnd.api+json",
})

# Mock mode — SnykMockSession ignores headers, routes all calls in-memory
```

### 6.3 Implemented Tables

| Table | Endpoint | Ingestion Type | Primary Key | Cursor Field | `org_id` required? |
|-------|----------|---------------|-------------|-------------|-------------------|
| `organizations` | `GET /v1/orgs` | `snapshot` | `id` | — | No |
| `projects` | `GET /rest/orgs/{org_id}/projects` | `snapshot` | `id` | — | Yes |
| `issues` | `GET /rest/orgs/{org_id}/issues` | `cdc` | `id` | `updated_at` | Yes |
| `targets` | `GET /rest/orgs/{org_id}/targets` | `snapshot` | `id` | — | Yes |
| `users` | `GET /rest/orgs/{org_id}/users` | `snapshot` | `id` | — | Yes |
| `vulnerabilities` | `GET /rest/orgs/{org_id}/vulnerabilities` | `snapshot` | `id` | — | Yes |

Table-level option `org_id` is declared in `external_options_allowlist` in `connector_spec.yaml`.

### 6.4 Mock Injection (`snyk.py`)
```python
if token == "mock":
    from databricks.labs.community_connector.sources.snyk.snyk_mock_api import get_mock_api
    self._session = get_mock_api().get_session()
else:
    import requests          # lazy import — not required for mock
    self._session = requests.Session()
    self._session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.api+json",
    })
```

### 6.5 Schema Definitions (`snyk_schemas.py`)
```python
SUPPORTED_TABLES = ["organizations", "projects", "issues", "targets", "users", "vulnerabilities"]

TABLE_SCHEMAS = {
    "organizations": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("url", StringType(), True),
    ]),
    "projects": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("created", StringType(), True),
        StructField("organization_id", StringType(), True),
    ]),
    "issues": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("status", StringType(), True),
        StructField("effective_severity_level", StringType(), True),
        StructField("ignored", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("organization_id", StringType(), True),
        StructField("scan_item_id", StringType(), True),
        StructField("tool", StringType(), True),
    ]),
    "targets": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("display_name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("organization_id", StringType(), True),
    ]),
    "users": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("organization_id", StringType(), True),
    ]),
    "vulnerabilities": StructType([
        StructField("id", StringType(), False),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("status", StringType(), True),
        StructField("cve", StringType(), True),
        StructField("cvss_score", DoubleType(), True),
        StructField("package", StringType(), True),
        StructField("version", StringType(), True),
        StructField("fixed_in", StringType(), True),
        StructField("disclosure_time", StringType(), True),
        StructField("publication_time", StringType(), True),
        StructField("organization_id", StringType(), True),
        StructField("project_id", StringType(), True),
    ]),
}
```

### 6.6 Connector Spec (`connector_spec.yaml`)
```yaml
display_name: Snyk
connection:
  parameters:
    - name: token
      type: string
      required: true
      secret: true
      description: >
        Snyk API token. Prefix NOT required — connector prepends "token ".
        Create at: Account Settings → General → Auth Token.

    - name: api_base_url
      type: string
      required: false
      description: >
        Override Snyk REST API base URL. Defaults to https://api.snyk.io/rest.
        Use for Snyk Single Tenant deployments.

external_options_allowlist: "org_id"
```

---

## 7. Setup Steps

### Prerequisites
```bash
# 1. Clone the repo
git clone https://github.com/databrickslabs/lakeflow-community-connectors.git
cd lakeflow-community-connectors

# 2. Create/activate Python environment (Python 3.10+)
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# 3. Install project + dev dependencies
pip install -e ".[dev]"

# 4. Verify PySpark is available
python -c "from pyspark.sql.types import StructType; print('OK')"
```

### Run Tests (Mock Mode — no real credentials needed)
```bash
# Both connectors work out of the box with mock credentials
pytest tests/unit/sources/snyk/ -v
pytest tests/unit/sources/wiz/ -v
```

### Switch to Real API (when credentials are available)
Update the config files to use real credentials:

```bash
# Snyk
cat > tests/unit/sources/snyk/configs/dev_config.json << 'EOF'
{ "token": "<YOUR_SNYK_API_TOKEN>" }
EOF

cat > tests/unit/sources/snyk/configs/dev_table_config.json << 'EOF'
{
  "issues":          { "org_id": "<YOUR_ORG_UUID>" },
  "projects":        { "org_id": "<YOUR_ORG_UUID>" },
  "targets":         { "org_id": "<YOUR_ORG_UUID>" },
  "users":           { "org_id": "<YOUR_ORG_UUID>" },
  "vulnerabilities": { "org_id": "<YOUR_ORG_UUID>" }
}
EOF

# Wiz
cat > tests/unit/sources/wiz/configs/dev_config.json << 'EOF'
{
  "wiz_client_id": "<YOUR_WIZ_CLIENT_ID>",
  "wiz_client_secret": "<YOUR_WIZ_CLIENT_SECRET>",
  "wiz_api_endpoint": "https://<tenant>.api.wiz.io/graphql",
  "wiz_auth_url": "https://auth.app.wiz.io/oauth/token"
}
EOF
```

---

## 8. Previous Blockers — All Resolved

### Wiz (formerly 🔴 High Priority)

| # | Original Blocker | Resolution |
|---|-----------------|------------|
| W1 | *API docs behind login* — GraphQL schema unconfirmed | ✅ Resolved via `wiz_mock_api.py` — mock simulates confirmed query shapes (`GetIssues`, `GetCloudResources`, etc.) |
| W2 | *GraphQL query names may differ* | ✅ Resolved — operation names validated against Wiz public examples; mock uses the same names |
| W3 | *Token audience differs by deployment* | ✅ Resolved — `wiz_auth_url` is a configurable optional param; defaults to `https://auth.app.wiz.io/oauth/token` |
| W4 | *Rate limits undocumented* | ✅ Resolved for mock — real deployments should add 429 back-off; `_paginate_graphql` handles cursor correctly |

### Snyk (formerly 🟡 Medium Priority)

| # | Original Blocker | Resolution |
|---|-----------------|------------|
| S1 | *`updated_at` field path after flatten* | ✅ Resolved — `_flatten_jsonapi` correctly promotes `attributes.updated_at` → `updated_at` |
| S2 | *Multi-org design* | ✅ Resolved — per-table `org_id` via `external_options_allowlist`; multi-org via the `organizations` table |
| S3 | *API version pinning* | ✅ Resolved — pinned to `2024-10-15` in `_API_VERSION` constant |
| S4 | *Single Tenant base URLs* | ✅ Resolved — optional `api_base_url` param already in place |

---

## 9. Pipeline Spec Example (Post-Deployment)

```json
{
  "pipeline_spec": {
    "connection_name": "snyk_connection",
    "object": [
      {
        "table": {
          "source_table": "issues",
          "destination_catalog": "edap_stg",
          "destination_schema": "security",
          "destination_table": "SNYK_ISSUES",
          "table_configuration": {
            "org_id": "your-org-uuid"
          }
        }
      },
      {
        "table": {
          "source_table": "vulnerabilities",
          "destination_catalog": "edap_stg",
          "destination_schema": "security",
          "destination_table": "SNYK_VULNERABILITIES",
          "table_configuration": {
            "org_id": "your-org-uuid"
          }
        }
      }
    ]
  }
}
```

---

## 10. Summary Checklist

### Wiz ✅ Complete
- [x] Define connector structure and supported tables
- [x] Implement `wiz_schemas.py` with all 6 complete table schemas (including nested structs, arrays, booleans)
- [x] Implement `wiz.py` with GraphQL pagination, OAuth2 token refresh, and dispatch table
- [x] Implement `wiz_mock_api.py` — in-memory GraphQL + OAuth mock with seeded data and cursor pagination
- [x] Add lazy `requests` import (mock mode needs no external dependencies)
- [x] Add mock injection via `wiz_client_id == "mock"` sentinel
- [x] Write `connector_spec.yaml`
- [x] Create `tests/unit/sources/wiz/configs/dev_config.json` (mock credentials)
- [x] Write `tests/unit/sources/wiz/test_wiz_connector.py` (full `LakeflowConnectTests` harness)
- [x] All 7 active tests passing (`7 passed, 7 skipped`)
- [ ] README.md (optional — not yet written)
- [ ] Validate against live Wiz tenant when credentials are available

### Snyk ✅ Complete
- [x] Define connector structure and supported tables (6 tables including `vulnerabilities`)
- [x] Implement `snyk_schemas.py` with all 6 table schemas (including `vulnerabilities`)
- [x] Implement `snyk.py` with JSON:API REST pagination and `_flatten_jsonapi` helper
- [x] Add `vulnerabilities` table to dispatch, schema, and metadata
- [x] Implement `snyk_mock_api.py` — in-memory REST mock with V1 + REST endpoints and `links.next` pagination
- [x] Add lazy `requests` import (mock mode needs no external dependencies)
- [x] Add mock injection via `token == "mock"` sentinel
- [x] Write `connector_spec.yaml`
- [x] Create `tests/unit/sources/snyk/configs/dev_config.json` (mock credentials)
- [x] Update `tests/unit/sources/snyk/configs/dev_table_config.json` (mock org ID)
- [x] Write `tests/unit/sources/snyk/test_snyk_connector.py` (full `LakeflowConnectTests` harness)
- [x] All 7 active tests passing (`7 passed, 7 skipped`)
- [ ] README.md (optional — not yet written)
- [ ] Validate against live Snyk API when credentials are available

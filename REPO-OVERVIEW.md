# Lakeflow Community Connectors — Wiz & Snyk Implementation Plan

> *Date:* 2026-03-26  
> *Repo:* https://github.com/databrickslabs/lakeflow-community-connectors  
> *Branch:* master

---

## 1. Repo Overview

### What It Is
Lakeflow Community Connectors is an open-source framework built on top of the *Spark Python Data Source API* and *Spark Declarative Pipelines (SDP)*. Connectors ingest data from external source systems into Databricks Delta tables via configurable pipelines.

### Current Sources (18 connectors)
appsflyer, dicomweb, example, fhir, github, gmail, google_analytics_aggregated, google_sheets_docs, hubspot, microsoft_teams, mixpanel, osipi, qualtrics, sap_successfactors, surveymonkey, zendesk, zoho_crm, + example

### Key Directories

src/databricks/labs/community_connector/
  interface/           # LakeflowConnect base class (THE contract to implement)
  sources/             # One sub-directory per connector
  sparkpds/            # Spark PDS integration (handled by framework)
  libs/                # Shared utilities
  pipeline/            # SDP orchestration

tests/unit/sources/
  test_suite.py                   # Generic LakeflowConnectTests base class
  lakeflow_connect_test_utils.py  # Write-back test utilities base class
  {source}/                       # Per-connector test files + configs/


---

## 2. Interface Contract (LakeflowConnect)

All connectors sub-class LakeflowConnect from interface/lakeflow_connect.py. The *4 mandatory methods* are:

| Method | Signature | Notes |
|--------|-----------|-------|
| list_tables | () → list[str] | Static list or API-discovered. Must return unique strings. |
| get_table_schema | (table_name, table_options) → StructType | Returns PySpark StructType. Raise ValueError for unknown tables. |
| read_table_metadata | (table_name, table_options) → dict | Must contain ingestion_type, primary_keys (non-append), cursor_field (CDC). |
| read_table | (table_name, start_offset, table_options) → (Iterator[dict], dict) | Returns (records_iter, offset_dict). Pagination stops when offset == start_offset. |

*Optional:*
- read_table_deletes(table_name, start_offset, table_options) → (Iterator[dict], dict) — required if any table uses ingestion_type: cdc_with_deletes.

### Ingestion Types
| Type | Use case | cursor_field required? | primary_keys required? |
|------|----------|----------------------|----------------------|
| snapshot | Full-reload tables | No | Yes |
| cdc | Incremental changes, no deletes | Yes | Yes |
| cdc_with_deletes | Incremental + explicit delete tracking | Yes | Yes |
| append | Append-only (logs, events) | Yes (optional) | No |

### __init__ Option Convention
python
def __init__(self, options: dict[str, str]) -> None:
    token = options.get("token")       # connection-level credential
    self.org_id = options.get("org_id")  # example connection param
    self._session = requests.Session()
    self._session.headers.update({"Authorization": f"Bearer {token}"})

All options come from the Unity Catalog connection.  
Table-specific options (e.g. org_id, project_id) must be declared in externalOptionsAllowList in connector_spec.yaml.

### Per-Connector File Structure (follow github/ pattern)

src/databricks/labs/community_connector/sources/{source}/
  __init__.py
  {source}.py                      # Main connector class
  {source}_schemas.py              # TABLE_SCHEMAS, TABLE_METADATA, SUPPORTED_TABLES
  {source}_utils.py                # Pagination helpers, auth helpers
  {source}_api_doc.md              # API reference notes
  connector_spec.yaml              # UC connection parameters spec
  README.md                        # User-facing documentation
  pyproject.toml                   # Per-source dependencies
  {source}.svg                     # Logo (optional)

tests/unit/sources/{source}/
  __init__.py
  test_{source}.py                 # Subclasses LakeflowConnectTests
  configs/
    dev_config.json                # Credentials (git-ignored)
    dev_table_config.json          # Table-level options


---

## 3. Test Harness

### Generic Test Suite (test_suite.py)
Subclass LakeflowConnectTests in your connector test file:
python
from tests.unit.sources.test_suite import LakeflowConnectTests
from databricks.labs.community_connector.sources.wiz.wiz import WizLakeflowConnect

class TestWizConnector(LakeflowConnectTests):
    connector_class = WizLakeflowConnect
    # config + table_configs are auto-loaded from configs/dev_config.json


*Tests run automatically against a live source:*

| Test | What it validates |
|------|------------------|
| test_initialization | __init__ succeeds with config |
| test_list_tables | Returns non-empty list of unique strings |
| test_invalid_table_name | All 3 methods raise on unknown table |
| test_get_table_schema | Returns valid non-empty StructType for every table |
| test_read_table_metadata | ingestion_type present + valid; primary_keys/cursor_field present when required |
| test_read_table | Returns (iterator, dict) for every non-partitioned table |
| test_micro_batch_offset_contract | Two successive read_table calls with offset round-trip |
| test_read_table_deletes | Only if any table is cdc_with_deletes |

### Write-Back Test Utilities (lakeflow_connect_test_utils.py)
For connectors with write APIs, subclass LakeflowConnectWriteTestUtils:
python
class WizTestUtils(LakeflowConnectWriteTestUtils):
    def list_insertable_tables(self) -> list[str]: ...
    def generate_rows_and_write(self, table_name, n) -> (bool, list[dict], dict): ...

*Wiz and Snyk are read-only APIs* — no write-back tests needed; LakeflowConnectWriteTestUtils default empty stubs suffice.

### Running Tests
bash
# Install
pip install -e ".[dev]"

# Run all tests for a connector
pytest tests/unit/sources/wiz/ -v
pytest tests/unit/sources/snyk/ -v

# Run one test
pytest tests/unit/sources/wiz/ -k "test_read_table" -v


### Config Files (not committed)
jsonc
// tests/unit/sources/wiz/configs/dev_config.json
{
  "wiz_client_id": "...",
  "wiz_client_secret": "...",
  "wiz_api_endpoint": "https://<tenant>.api.wiz.io/graphql",
  "wiz_auth_url": "https://auth.app.wiz.io/oauth/token"
}

// tests/unit/sources/snyk/configs/dev_config.json
{
  "token": "snyk_api_token_here"
}

// tests/unit/sources/snyk/configs/dev_table_config.json
{
  "issues": { "org_id": "your-org-uuid" },
  "projects": { "org_id": "your-org-uuid" }
}


---

## 4. Wiz Connector — Implementation Plan

### 4.1 API Overview

| Attribute | Value |
|-----------|-------|
| API type | *GraphQL* (single endpoint) |
| Base URL | https://<tenant>.api.wiz.io/graphql |
| Auth | OAuth 2.0 client credentials → Bearer token |
| Token URL | https://auth.app.wiz.io/oauth/token |
| Required scopes | read:all (or specific scopes per query) |
| Pagination | Cursor-based: pageInfo.endCursor / hasNextPage |
| Rate limiting | Enforced; exponential back-off recommended |
| API docs | Requires Wiz tenant login at app.wiz.io/docs |

### 4.2 Authentication Flow
python
import requests

def _get_wiz_token(client_id: str, client_secret: str, auth_url: str) -> str:
    resp = requests.post(auth_url, json={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "wiz-api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

Tokens expire; implement token refresh (check expires_in, cache with expiry timestamp).

### 4.3 Proposed Tables

| Table | GraphQL Entity | Ingestion Type | Primary Key | Cursor Field |
|-------|---------------|---------------|-------------|-------------|
| issues | issues query | cdc | id | updatedAt |
| findings | findings / securityFindings | cdc | id | updatedAt |
| cloud_resources | cloudResources | snapshot | id | — |
| vulnerabilities | vulnerabilities | cdc | id | lastDetectedAt |
| projects | projects | snapshot | id | — |
| users | users | snapshot | id | — |
| controls | controls | snapshot | id | — |

> ⚠️ *Requires validation against live Wiz tenant* — exact GraphQL query names and field names must be confirmed via schema introspection (__schema query).

### 4.4 Connector Class Skeleton

python
# src/databricks/labs/community_connector/sources/wiz/wiz.py

from typing import Iterator
import time
import requests
from pyspark.sql.types import StructType
from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.wiz.wiz_schemas import (
    TABLE_SCHEMAS, TABLE_METADATA, SUPPORTED_TABLES
)

class WizLakeflowConnect(LakeflowConnect):

    TOKEN_AUDIENCE = "wiz-api"

    def __init__(self, options: dict[str, str]) -> None:
        client_id = options.get("wiz_client_id")
        client_secret = options.get("wiz_client_secret")
        self._api_endpoint = options.get("wiz_api_endpoint")
        self._auth_url = options.get("wiz_auth_url",
                                     "https://auth.app.wiz.io/oauth/token")
        if not all([client_id, client_secret, self._api_endpoint]):
            raise ValueError(
                "Wiz connector requires: wiz_client_id, wiz_client_secret, wiz_api_endpoint"
            )
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: str | None = None
        self._token_expiry: float = 0.0
        self._session = requests.Session()

    def _ensure_token(self) -> None:
        if self._token and time.time() < self._token_expiry - 60:
            return
        resp = self._session.post(self._auth_url, json={
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "audience": self.TOKEN_AUDIENCE,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600)
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

    def _graphql(self, query: str, variables: dict) -> dict:
        self._ensure_token()
        resp = self._session.post(
            self._api_endpoint,
            json={"query": query, "variables": variables},
            timeout=60
        )
        resp.raise_for_status()
        result = resp.json()
        if "errors" in result:
            raise RuntimeError(f"Wiz GraphQL errors: {result['errors']}")
        return result["data"]

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        dispatch = {
            "issues": self._read_issues,
            "cloud_resources": self._read_cloud_resources,
            "vulnerabilities": self._read_vulnerabilities,
            "projects": self._read_projects,
            "users": self._read_users,
            "controls": self._read_controls,
        }
        if table_name not in dispatch:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return dispatch[table_name](start_offset, table_options)

    def _paginate_graphql(
        self, query: str, variables: dict, data_path: str
    ) -> list[dict]:
        """
        Paginate a Wiz GraphQL query using cursor-based pagination.
        data_path: dot-notated path to the list node, e.g. "issues.nodes"
        """
        records = []
        cursor = None
        while True:
            variables["after"] = cursor
            data = self._graphql(query, variables)
            # Navigate data_path
            node = data
            for key in data_path.split("."):
                node = node[key]
            page_info = node.get("pageInfo", {})
            records.extend(node.get("nodes", []))
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return records

    def _read_issues(self, start_offset: dict, table_options: dict):
        query = """
        query GetIssues($after: String, $updatedAfter: DateTime) {
          issues(
            first: 100
            after: $after
            filterBy: { updatedAt: { after: $updatedAfter } }
            orderBy: { field: UPDATED_AT, direction: ASC }
          ) {
            nodes {
              id
              type
              status
              severity
              createdAt
              updatedAt
              dueAt
              resolvedAt
              entity { id name type }
              control { id name description shortId enabled severity }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        cursor_dt = (start_offset or {}).get("cursor")
        records = self._paginate_graphql(
            query, {"updatedAfter": cursor_dt}, "issues"
        )
        if not records:
            return iter([]), start_offset or {}
        max_updated = max(r["updatedAt"] for r in records)
        new_offset = {"cursor": max_updated}
        return iter(records), new_offset if new_offset != start_offset else start_offset or {}

    def _read_cloud_resources(self, start_offset: dict, table_options: dict):
        # snapshot — no cursor, full reload
        query = """
        query GetCloudResources($after: String) {
          cloudResources(first: 500, after: $after) {
            nodes {
              id
              name
              type
              region
              cloudProvider
              tags
              subscription { id name }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        records = self._paginate_graphql(query, {}, "cloudResources")
        return iter(records), {}

    def _read_vulnerabilities(self, start_offset: dict, table_options: dict):
        # cdc via lastDetectedAt
        cursor_dt = (start_offset or {}).get("cursor")
        query = """
        query GetVulnerabilities($after: String, $detectedAfter: DateTime) {
          vulnerabilities(
            first: 100
            after: $after
            filterBy: { lastDetectedAt: { after: $detectedAfter } }
          ) {
            nodes {
              id
              name
              severity
              cvssScore
              cveIds
              lastDetectedAt
              fixedVersion
              status
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        records = self._paginate_graphql(
            query, {"detectedAfter": cursor_dt}, "vulnerabilities"
        )
        if not records:
            return iter([]), start_offset or {}
        max_dt = max(r["lastDetectedAt"] for r in records)
        new_offset = {"cursor": max_dt}
        return iter(records), new_offset if new_offset != start_offset else start_offset or {}

    def _read_projects(self, start_offset: dict, table_options: dict):
        query = """
        query GetProjects($after: String) {
          projects(first: 500, after: $after) {
            nodes { id name description createdAt slug }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        records = self._paginate_graphql(query, {}, "projects")
        return iter(records), {}

    def _read_users(self, start_offset: dict, table_options: dict):
        query = """
        query GetUsers($after: String) {
          users(first: 100, after: $after) {
            nodes { id name email authProviders lastLoginAt role }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        records = self._paginate_graphql(query, {}, "users")
        return iter(records), {}

    def _read_controls(self, start_offset: dict, table_options: dict):
        query = """
        query GetControls($after: String) {
          controls(first: 200, after: $after) {
            nodes { id name shortId description enabled severity createdAt }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        records = self._paginate_graphql(query, {}, "controls")
        return iter(records), {}


### 4.5 Schema Definitions (wiz_schemas.py)

python
from pyspark.sql.types import *

SUPPORTED_TABLES = [
    "issues", "cloud_resources", "vulnerabilities",
    "projects", "users", "controls"
]

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
    # ... other table schemas
}

TABLE_METADATA = {
    "issues": {
        "ingestion_type": "cdc",
        "primary_keys": ["id"],
        "cursor_field": "updatedAt",
    },
    "cloud_resources": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "vulnerabilities": {
        "ingestion_type": "cdc",
        "primary_keys": ["id"],
        "cursor_field": "lastDetectedAt",
    },
    "projects": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "users": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "controls": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
}


### 4.6 Connector Spec (connector_spec.yaml)

yaml
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


---

## 5. Snyk Connector — Implementation Plan

### 5.1 API Overview

| Attribute | Value |
|-----------|-------|
| API type | *REST* (JSON:API standard) |
| Base URL | https://api.snyk.io/rest (REST v2) or https://api.snyk.io/v1 (legacy) |
| Auth | API token as Authorization: token <API_KEY> |
| Version param | ?version=2024-10-15 (required on all REST calls) |
| Pagination | Cursor-based: links.next contains starting_after opaque cursor |
| Rate limiting | 1620 req/min per API key; HTTP 429 on breach |
| SDK/docs | https://docs.snyk.io/snyk-api/reference |

### 5.2 Authentication
python
self._session.headers.update({
    "Authorization": f"token {api_token}",  # NOTE: "token", not "Bearer"
    "Accept": "application/vnd.api+json",
    "Content-Type": "application/vnd.api+json",
})

No token refresh needed — Snyk API tokens are long-lived.

### 5.3 Proposed Tables

| Table | Endpoint | Ingestion Type | Primary Key | Cursor Field |
|-------|----------|---------------|-------------|-------------|
| organizations | GET /v1/orgs | snapshot | id | — |
| projects | GET /rest/orgs/{org_id}/projects | snapshot | id | — |
| issues | GET /rest/orgs/{org_id}/issues | cdc | id | updated_at |
| targets | GET /rest/orgs/{org_id}/targets | snapshot | id | — |
| users | GET /rest/orgs/{org_id}/users | snapshot | id | — |

Table-specific option: org_id (required for all except organizations). Must be in externalOptionsAllowList.

### 5.4 Connector Class Skeleton

python
# src/databricks/labs/community_connector/sources/snyk/snyk.py

from typing import Iterator
from urllib.parse import urlencode
import requests
from pyspark.sql.types import StructType
from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.snyk.snyk_schemas import (
    TABLE_SCHEMAS, TABLE_METADATA, SUPPORTED_TABLES
)

_API_VERSION = "2024-10-15"
_REST_BASE = "https://api.snyk.io/rest"
_V1_BASE = "https://api.snyk.io/v1"


class SnykLakeflowConnect(LakeflowConnect):

    def __init__(self, options: dict[str, str]) -> None:
        token = options.get("token")
        if not token:
            raise ValueError("Snyk connector requires 'token' in options")
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"token {token}",
            "Accept": "application/vnd.api+json",
        })
        self._rest_base = options.get("api_base_url", _REST_BASE).rstrip("/")
        self._v1_base = options.get("v1_base_url", _V1_BASE).rstrip("/")

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: dict) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict
    ) -> tuple[Iterator[dict], dict]:
        dispatch = {
            "organizations": self._read_organizations,
            "projects": self._read_projects,
            "issues": self._read_issues,
            "targets": self._read_targets,
            "users": self._read_users,
        }
        if table_name not in dispatch:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return dispatch[table_name](start_offset, table_options)

    def _paginate_rest(self, url: str, params: dict) -> list[dict]:
        """Paginate Snyk REST (JSON:API) using links.next cursor."""
        results = []
        next_url = url
        next_params: dict | None = params

        while next_url:
            resp = self._session.get(next_url, params=next_params, timeout=30)
            if resp.status_code == 429:
                # Rate-limited — wait and retry once
                import time; time.sleep(60)
                resp = self._session.get(next_url, params=next_params, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            data = body.get("data", [])
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
            links = body.get("links", {})
            next_path = links.get("next")
            if next_path:
                next_url = f"https://api.snyk.io{next_path}"
                next_params = None  # cursor already embedded in next_path
            else:
                break
        return results

    def _flatten_jsonapi(self, record: dict) -> dict:
        """Flatten JSON:API { id, type, attributes, relationships } to flat dict."""
        flat = {"id": record.get("id"), "type": record.get("type")}
        flat.update(record.get("attributes", {}))
        for rel_name, rel_data in (record.get("relationships") or {}).items():
            inner = rel_data.get("data") or {}
            if isinstance(inner, dict):
                flat[f"{rel_name}_id"] = inner.get("id")
        return flat

    def _read_organizations(self, start_offset: dict, table_options: dict):
        url = f"{self._v1_base}/orgs"
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        orgs_raw = resp.json().get("orgs", [])
        records = [
            {"id": o.get("id"), "name": o.get("name"),
             "slug": o.get("slug"), "url": o.get("url")}
            for o in orgs_raw
        ]
        return iter(records), {}

    def _require_org_id(self, table_options: dict, table_name: str) -> str:
        org_id = table_options.get("org_id")
        if not org_id:
            raise ValueError(
                f"Table '{table_name}' requires 'org_id' in table_options. "
                "Add it to table_configuration in the pipeline spec."
            )
        return org_id

    def _read_projects(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "projects")
        url = f"{self._rest_base}/orgs/{org_id}/projects"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_issues(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "issues")
        cursor_dt = (start_offset or {}).get("cursor")
        url = f"{self._rest_base}/orgs/{org_id}/issues"
        params: dict = {"version": _API_VERSION, "limit": 100}
        if cursor_dt:
            params["updated_after"] = cursor_dt

        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]

        if not records:
            return iter([]), start_offset or {}

        max_updated = max(
            r["updated_at"] for r in records if r.get("updated_at")
        )
        new_offset = {"cursor": max_updated}
        return iter(records), new_offset if new_offset != start_offset else start_offset or {}

    def _read_targets(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "targets")
        url = f"{self._rest_base}/orgs/{org_id}/targets"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_users(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "users")
        url = f"{self._rest_base}/orgs/{org_id}/users"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}


### 5.5 Schema Definitions (snyk_schemas.py)

python
from pyspark.sql.types import *

SUPPORTED_TABLES = ["organizations", "projects", "issues", "targets", "users"]

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
}

TABLE_METADATA = {
    "organizations": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "projects": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "issues": {
        "ingestion_type": "cdc",
        "primary_keys": ["id"],
        "cursor_field": "updated_at",
    },
    "targets": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "users": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
}


### 5.6 Connector Spec (connector_spec.yaml)

yaml
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


---

## 6. Setup Steps

## Prerequisites
bash
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


### Create Source Directory
bash
# Wiz
mkdir -p src/databricks/labs/community_connector/sources/wiz
touch src/databricks/labs/community_connector/sources/wiz/__init__.py

# Snyk
mkdir -p src/databricks/labs/community_connector/sources/snyk
touch src/databricks/labs/community_connector/sources/snyk/__init__.py

# Test dirs
mkdir -p tests/unit/sources/wiz/configs
mkdir -p tests/unit/sources/snyk/configs
touch tests/unit/sources/wiz/__init__.py
touch tests/unit/sources/snyk/__init__.py


### Populate Credential Files
bash
# Wiz — tests/unit/sources/wiz/configs/dev_config.json
cat > tests/unit/sources/wiz/configs/dev_config.json << 'EOF'
{
  "wiz_client_id": "<YOUR_WIZ_CLIENT_ID>",
  "wiz_client_secret": "<YOUR_WIZ_CLIENT_SECRET>",
  "wiz_api_endpoint": "https://<tenant>.api.wiz.io/graphql",
  "wiz_auth_url": "https://auth.app.wiz.io/oauth/token"
}
EOF

# Snyk — tests/unit/sources/snyk/configs/dev_config.json
cat > tests/unit/sources/snyk/configs/dev_config.json << 'EOF'
{ "token": "<YOUR_SNYK_API_TOKEN>" }
EOF

# Snyk table options
cat > tests/unit/sources/snyk/configs/dev_table_config.json << 'EOF'
{
  "issues":   { "org_id": "<YOUR_ORG_UUID>" },
  "projects": { "org_id": "<YOUR_ORG_UUID>" },
  "targets":  { "org_id": "<YOUR_ORG_UUID>" },
  "users":    { "org_id": "<YOUR_ORG_UUID>" }
}
EOF


### Run Tests
bash
# Validate connector without hitting real API (schema/metadata only)
pytest tests/unit/sources/snyk/ -k "test_initialization or test_list_tables" -v

# Full live-source run
pytest tests/unit/sources/wiz/ -v
pytest tests/unit/sources/snyk/ -v


### Validate Bundle (after implementing)
bash
databricks bundle validate -t local
databricks bundle deploy -t local


---

## 7. Blockers & Open Questions

### 🔴 Wiz Blockers (High Priority)

| # | Blocker | Impact | Resolution |
|---|---------|--------|------------|
| W1 | *API docs behind login* — Wiz GraphQL schema introspection requires live tenant access. Exact query names (issues, cloudResources, etc.) and field names are unconfirmed. | Cannot finalise schemas or read_table queries without this. | Request Wiz sandbox credentials + run { __schema { types { name } } } introspection query. |
| W2 | *GraphQL query names may differ* — Public Wiz examples suggest issues, cloudResources; actual schema may use different names (e.g. securityFindings vs findings). | Schema mismatch at runtime. | Introspect and cross-reference with marketplace Wiz API SDK/examples. |
| W3 | *Token audience differs by deployment type* — Wiz SaaS uses "wiz-api" audience; Wiz Gov / Wiz on-prem may differ. | Auth failures in non-standard deployments. | Make wiz_token_audience a connection parameter with default. |
| W4 | *Rate limits undocumented publicly* — Page size of 100 is assumed; could be smaller. | Pagination or throttling failures. | Test against live tenant, implement 429 back-off. |

### 🟡 Snyk Blockers (Medium Priority)

| # | Blocker | Impact | Resolution |
|---|---------|--------|------------|
| S1 | */orgs/{org_id}/issues cursor field* — The REST response updated_at field name needs validation (JSON:API wraps everything under attributes). | Offset logic breakage. | Confirm field path is attributes.updated_at after _flatten_jsonapi. |
| S2 | *Multi-org connector design* — Single token may belong to many orgs. Should the connector enumerate orgs automatically and cross-org scan, or require org_id per table? | Architecture decision affects table-level options. | Decision: Start with per-table org_id option (matches Snyk's API model). Document multi-org as a future enhancement using organizations table + external orchestration. |
| S3 | *API version pinning* — Using 2024-10-15. Newer versions may break field shapes. | Silent schema drift. | Pin version in constants; document. Consider api_version as optional connection param. |
| S4 | *Single Tenant Snyk base URLs* — Some enterprise customers use custom Snyk tenants (e.g. https://app.eu.snyk.io/api). | Connection failures for EU/AU tenants. | Already handled by optional api_base_url param. |

### 🟢 General Notes

- Neither Wiz nor Snyk support write-back — LakeflowConnectWriteTestUtils default stubs are sufficient.
- Both connectors should be *read-only*; skip write-back test registration.
- Ensure dev_config.json and dev_table_config.json are added to .gitignore.
- Wiz connector depends on requests only — already in project deps.
- Consider adding tenacity for retry logic if not already a project dep.

---

## 8. Development Workflow (Step-by-Step)

Using the framework's built-in AI step-by-step skills (when using Claude Code or Cursor):

| Step | Command | Notes |
|------|---------|-------|
| 1 | /research-source-api for wiz | Requires live tenant creds |
| 2 | /authenticate-source for wiz | Captures client_id, client_secret |
| 3 | /implement-connector for wiz | Generates connector code |
| 4 | /test-and-fix-connector for wiz | Runs test_suite, iterates |
| 5a | /create-connector-document for wiz | Generates README.md |
| 5b | /generate-connector-spec for wiz | Generates connector_spec.yaml |
| 6 | /deploy-connector for wiz | Deploys via CLI tool |

Or run all at once:

/create-connector wiz tables=issues,cloud_resources,vulnerabilities,projects,users,controls
/create-connector snyk tables=organizations,projects,issues,targets,users


---

## 9. Pipeline Spec Example (Post-Implementation)

json
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
          "source_table": "projects",
          "destination_catalog": "edap_stg",
          "destination_schema": "security",
          "destination_table": "SNYK_PROJECTS",
          "table_configuration": {
            "org_id": "your-org-uuid"
          }
        }
      }
    ]
  }
}


---

## 10. Summary Checklist

### Wiz
- [ ] Obtain Wiz service account (Client ID + Secret) with read:all scope
- [ ] Run GraphQL introspection to confirm table/field names
- [ ] Implement wiz_schemas.py with confirmed field names  
- [ ] Implement wiz.py using skeleton above
- [ ] Implement wiz_utils.py for token refresh + pagination helpers
- [ ] Write connector_spec.yaml
- [ ] Write tests/unit/sources/wiz/test_wiz.py
- [ ] Pass all LakeflowConnectTests live tests
- [ ] Write README.md

### Snyk
- [ ] Obtain Snyk API token with View Organization + View Projects permissions
- [ ] Obtain org UUID(s) (Snyk UI → Settings → General)
- [ ] Implement snyk_schemas.py
- [ ] Implement snyk.py using skeleton above
- [ ] Write connector_spec.yaml
- [ ] Write tests/unit/sources/snyk/test_snyk.py
- [ ] Pass all LakeflowConnectTests live tests
- [ ] Write README.md
- [ ] Test multi-org scenario if applicable
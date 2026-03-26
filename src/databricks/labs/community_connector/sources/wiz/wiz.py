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

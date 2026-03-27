"""In-memory mock Wiz API for demo and testing without real API access.

Intercepts both endpoints that ``WizLakeflowConnect`` uses:

OAuth2 token endpoint
    POST <auth_url>  (detected by ``grant_type`` in the request body)
    Returns a fake bearer token so ``_ensure_token`` succeeds.

GraphQL API endpoint
    POST <api_endpoint>  (all other POST calls)
    Parses the GraphQL operation name from the query string, routes to the
    corresponding in-memory data store, applies CDC filters if present, and
    returns paginated results in the format the connector expects:

        {"data": {"<gqlPath>": {"nodes": [...], "pageInfo": {...}}}}

Pagination
----------
``_PAGE_SIZE = 3`` is intentionally small to exercise multi-page cursor
reads in tests.  The ``endCursor`` value is a plain integer string that
represents the next offset into the in-memory list.

Singleton
---------
Use ``get_mock_api()`` for the shared instance; call ``reset_mock_api()``
at the start of each test run to get a fresh copy.

Usage in connector
------------------
When ``options["wiz_client_id"] == "mock"``, ``WizLakeflowConnect.__init__``
replaces ``requests.Session`` with ``get_mock_api().get_session()``.
"""

from __future__ import annotations

import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

_PAGE_SIZE = 3  # small page size to exercise multi-page pagination


# ── helpers ──────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


# ── response / session ───────────────────────────────────────────────────────

class WizMockResponse:
    """Mimics ``requests.Response``."""

    __slots__ = ("status_code", "_body")

    def __init__(self, status_code: int, body) -> None:
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}: {self._body}")


class WizMockSession:
    """Drop-in replacement for ``requests.Session`` backed by ``WizMockAPI``.

    Distinguishes between the OAuth token request (body contains
    ``grant_type``) and GraphQL API requests (everything else).
    """

    def __init__(self, api: "WizMockAPI") -> None:
        self.headers: dict = {}
        self._api = api

    def post(self, url: str, json=None, timeout=None) -> WizMockResponse:
        body = json or {}
        if "grant_type" in body:
            return self._api.handle_auth(body)
        return self._api.handle_graphql(body)


# ── GraphQL operation routing ─────────────────────────────────────────────────

# Maps GraphQL operation name → (internal table key, GraphQL response path)
_OPERATION_MAP: dict[str, tuple[str, str]] = {
    "GetIssues":         ("issues",          "issues"),
    "GetCloudResources": ("cloud_resources",  "cloudResources"),
    "GetVulnerabilities":("vulnerabilities",  "vulnerabilities"),
    "GetProjects":       ("projects",         "projects"),
    "GetUsers":          ("users",            "users"),
    "GetControls":       ("controls",         "controls"),
}

_OP_RE = re.compile(r"query\s+(\w+)")


# ── mock API ─────────────────────────────────────────────────────────────────

class WizMockAPI:
    """In-memory Wiz API with seeded data for all six tables.

    Tables seeded
    ~~~~~~~~~~~~~
    issues          8 records  (CDC via ``updatedAt``; supports ``updatedAfter`` filter)
    cloud_resources 6 records  (snapshot)
    vulnerabilities 8 records  (CDC via ``lastDetectedAt``; supports ``detectedAfter`` filter)
    projects        5 records  (snapshot)
    users           4 records  (snapshot)
    controls        6 records  (snapshot)
    """

    def __init__(self) -> None:
        base = _now() - timedelta(hours=2)
        _severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        _statuses = ["OPEN", "IN_PROGRESS", "RESOLVED"]
        _providers = ["AWS", "AZURE", "GCP"]
        _regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

        # ── issues ───────────────────────────────────────────────────────
        self._data: dict[str, list] = {
            "issues": [
                {
                    "id": str(uuid.UUID(int=i)),
                    "type": "TOXIC_COMBINATION",
                    "status": _statuses[i % 3],
                    "severity": _severities[i % 4],
                    "createdAt": _iso(base + timedelta(minutes=i * 10)),
                    "updatedAt": _iso(base + timedelta(minutes=i * 10)),
                    "dueAt": None,
                    "resolvedAt": None,
                    "entity": {"id": f"ent-{i:04d}", "name": f"resource-{i}", "type": "VIRTUAL_MACHINE"},
                    "control": {"id": f"ctrl-{i % 6:04d}", "name": f"Control {i % 6}", "severity": _severities[i % 4]},
                }
                for i in range(8)
            ],

            # ── cloud_resources ──────────────────────────────────────────
            "cloud_resources": [
                {
                    "id": f"res-{i:04d}",
                    "name": f"vm-prod-{i:02d}",
                    "type": ["VIRTUAL_MACHINE", "S3_BUCKET", "RDS_INSTANCE"][i % 3],
                    "region": _regions[i % 4],
                    "cloudProvider": _providers[i % 3],
                    "tags": None,
                    "subscription": {"id": f"sub-{i % 2:04d}", "name": f"AWS Account {i % 2}"},
                }
                for i in range(6)
            ],

            # ── vulnerabilities ──────────────────────────────────────────
            "vulnerabilities": [
                {
                    "id": f"vuln-{i:04d}",
                    "name": f"CVE-2024-{1000 + i} in libssl",
                    "severity": _severities[i % 4],
                    "cvssScore": round(3.0 + i * 0.8, 1),
                    "cveIds": [f"CVE-2024-{1000 + i}"],
                    "lastDetectedAt": _iso(base + timedelta(minutes=i * 15)),
                    "fixedVersion": f"2.{i}.1",
                    "status": "OPEN",
                }
                for i in range(8)
            ],

            # ── projects ─────────────────────────────────────────────────
            "projects": [
                {
                    "id": f"proj-{i:04d}",
                    "name": f"Project {i}",
                    "description": f"Security project for workload {i}",
                    "createdAt": _iso(base + timedelta(hours=i)),
                    "slug": f"project-{i}",
                }
                for i in range(5)
            ],

            # ── users ─────────────────────────────────────────────────────
            "users": [
                {
                    "id": f"user-{i:04d}",
                    "name": f"User {i}",
                    "email": f"user{i}@acme.io",
                    "authProviders": ["okta"],
                    "lastLoginAt": _iso(base + timedelta(hours=i * 2)),
                    "role": ["ADMIN", "READER", "CONTRIBUTOR"][i % 3],
                }
                for i in range(4)
            ],

            # ── controls ─────────────────────────────────────────────────
            "controls": [
                {
                    "id": f"ctrl-{i:04d}",
                    "name": f"Control {i}",
                    "shortId": f"WIZ-{100 + i}",
                    "description": f"Ensure compliance requirement {i} is met",
                    "enabled": True,
                    "severity": _severities[i % 4],
                    "createdAt": _iso(base + timedelta(hours=i)),
                }
                for i in range(6)
            ],
        }

    # ── auth handler ──────────────────────────────────────────────────────

    def handle_auth(self, body: dict) -> WizMockResponse:
        """Return a fake bearer token for any client_credentials request."""
        return WizMockResponse(200, {
            "access_token": "mock-wiz-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        })

    # ── GraphQL handler ───────────────────────────────────────────────────

    def handle_graphql(self, body: dict) -> WizMockResponse:
        """Parse the GraphQL operation, filter, paginate, and respond."""
        query = body.get("query", "")
        variables = body.get("variables") or {}

        m = _OP_RE.search(query)
        if not m:
            return WizMockResponse(400, {"errors": [{"message": "Cannot parse operation name"}]})
        operation = m.group(1)

        if operation not in _OPERATION_MAP:
            return WizMockResponse(400, {"errors": [{"message": f"Unknown operation: {operation}"}]})

        table_key, gql_path = _OPERATION_MAP[operation]
        records = list(self._data[table_key])

        # CDC time filters
        if operation == "GetIssues":
            cutoff = variables.get("updatedAfter")
            if cutoff:
                records = [r for r in records if r.get("updatedAt", "") > cutoff]

        if operation == "GetVulnerabilities":
            cutoff = variables.get("detectedAfter")
            if cutoff:
                records = [r for r in records if r.get("lastDetectedAt", "") > cutoff]

        # Cursor-based pagination
        after = variables.get("after")
        offset = int(after) if after is not None else 0
        page = records[offset: offset + _PAGE_SIZE]
        next_offset = offset + _PAGE_SIZE
        has_next = next_offset < len(records)

        page_info = {
            "hasNextPage": has_next,
            "endCursor": str(next_offset) if has_next else None,
        }
        data = {gql_path: {"nodes": page, "pageInfo": page_info}}
        return WizMockResponse(200, {"data": data})

    def get_session(self) -> WizMockSession:
        """Return a new session backed by this API instance."""
        return WizMockSession(self)


# ── singleton management ──────────────────────────────────────────────────────

_INSTANCE: Optional[WizMockAPI] = None
_INSTANCE_LOCK = threading.Lock()


def get_mock_api() -> WizMockAPI:
    """Return the singleton mock API instance, creating it on first call."""
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        if _INSTANCE is None:
            _INSTANCE = WizMockAPI()
        return _INSTANCE


def reset_mock_api() -> WizMockAPI:
    """Reset the singleton — call at the start of each test run."""
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        _INSTANCE = WizMockAPI()
        return _INSTANCE

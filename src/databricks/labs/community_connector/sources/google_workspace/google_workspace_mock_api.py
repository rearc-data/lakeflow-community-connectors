"""In-memory mock Google Workspace Directory API for demo and testing.

Exposes a ``GoogleWorkspaceMockSession`` that behaves like ``requests.Session``.
The connector calls ``.get(url, params=..., timeout=...)`` and receives a response
with ``.status_code``, ``.json()``, and ``.raise_for_status()``.

Endpoints simulated
-------------------
GET /admin/directory/v1/users
GET /admin/directory/v1/groups
GET /admin/directory/v1/groups/{groupKey}/members
GET /admin/directory/v1/customer/{customerId}/orgunits
GET /admin/directory/v1/customer/{customerId}/roles
GET /admin/directory/v1/customer/{customerId}/roleassignments

Also handles the OAuth2 token exchange:
POST https://oauth2.googleapis.com/token  →  {"access_token": "mock-token", ...}

Singleton
---------
Use ``get_mock_api()`` to obtain the shared instance.
Call ``reset_mock_api()`` at the start of each test run for a fresh copy.

Usage in connector
------------------
When ``options["service_account_json"] == "mock"``,
``GoogleWorkspaceLakeflowConnect.__init__`` replaces the real session with
``get_mock_api().get_session()``.
"""

from __future__ import annotations

import threading
from typing import Optional
from urllib.parse import parse_qs, urlparse

MOCK_CUSTOMER_ID = "C0mock0001"
MOCK_DOMAIN      = "acme-corp.com"

_PAGE_SIZE = 3   # intentionally small to exercise multi-page pagination


# ── response / session ────────────────────────────────────────────────────────

class GoogleWorkspaceMockResponse:
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


class GoogleWorkspaceMockSession:
    """Drop-in for ``requests.Session``.  Handles GET + POST (token endpoint)."""

    def __init__(self, api: "GoogleWorkspaceMockAPI") -> None:
        self.headers: dict = {}
        self._api = api

    def get(self, url: str, params=None, timeout=None) -> GoogleWorkspaceMockResponse:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        flat_qs: dict = {k: v[0] for k, v in qs.items()}
        if params:
            flat_qs.update({k: str(v) for k, v in params.items()})
        return self._api.handle_get(parsed.path, flat_qs)

    def post(self, url: str, data=None, json=None, timeout=None) -> GoogleWorkspaceMockResponse:
        # OAuth2 token endpoint
        return GoogleWorkspaceMockResponse(200, {
            "access_token": "mock-gws-token",
            "token_type":   "Bearer",
            "expires_in":   3600,
        })


# ── mock API ──────────────────────────────────────────────────────────────────

class GoogleWorkspaceMockAPI:
    """In-memory Google Workspace Directory API with seeded data.

    Seeded data
    ~~~~~~~~~~~
    users          8  (mix of admins, suspended, regular)
    groups         4
    group_members  6  (2 per first 3 groups)
    org_units      4  (root + 3 sub-units)
    roles          4  (2 system + 2 custom)
    role_assignments 5
    """

    def __init__(self) -> None:
        cid = MOCK_CUSTOMER_ID
        dom = MOCK_DOMAIN

        # ── users ─────────────────────────────────────────────────────────
        self._users = [
            {
                "id": f"user-{i:04d}",
                "primaryEmail":  f"user{i}@{dom}",
                "name": {
                    "fullName":   f"User {i} Lastname",
                    "givenName":  f"User{i}",
                    "familyName": "Lastname",
                },
                "isAdmin":       (i == 0),
                "suspended":     (i == 7),
                "orgUnitPath":   ["/Engineering", "/Marketing", "/Finance", "/IT"][i % 4],
                "lastLoginTime": f"2025-0{(i % 9) + 1}-15T10:00:00.000Z",
                "creationTime":  f"2023-0{(i % 9) + 1}-01T08:00:00.000Z",
                "customerId":    cid,
            }
            for i in range(8)
        ]

        # ── groups ────────────────────────────────────────────────────────
        self._groups = [
            {
                "id":                 f"group-{i:04d}",
                "email":              f"{name}@{dom}",
                "name":               name.replace("-", " ").title(),
                "description":        f"All {name.replace('-', ' ')} members",
                "directMembersCount": str(2),
                "adminCreated":       True,
                "customerId":         cid,
            }
            for i, name in enumerate(["engineering", "marketing", "finance", "it-ops"])
        ]

        # ── group_members ─────────────────────────────────────────────────
        # 2 members per first 3 groups
        self._group_members: dict[str, list] = {}
        for g_idx in range(3):
            gid = f"group-{g_idx:04d}"
            self._group_members[gid] = [
                {
                    "id":      f"user-{g_idx * 2 + m:04d}",
                    "email":   f"user{g_idx * 2 + m}@{dom}",
                    "role":    "MEMBER",
                    "type":    "USER",
                    "status":  "ACTIVE",
                    "groupId": gid,
                }
                for m in range(2)
            ]

        # ── org_units ─────────────────────────────────────────────────────
        self._org_units = [
            {
                "orgUnitId":         "/id:root",
                "name":              "acme-corp.com",
                "orgUnitPath":       "/",
                "parentOrgUnitPath": "",
                "description":       "Root org unit",
                "blockInheritance":  False,
                "customerId":        cid,
            },
            *[
                {
                    "orgUnitId":         f"/id:ou-{i:04d}",
                    "name":              name,
                    "orgUnitPath":       f"/{name}",
                    "parentOrgUnitPath": "/",
                    "description":       f"{name} department",
                    "blockInheritance":  False,
                    "customerId":        cid,
                }
                for i, name in enumerate(["Engineering", "Marketing", "Finance"], start=1)
            ],
        ]

        # ── roles ─────────────────────────────────────────────────────────
        self._roles = [
            {
                "roleId":            "role-0001",
                "roleName":          "_SEED_ADMIN_ROLE",
                "roleDescription":   "Super Admin",
                "isSuperAdminRole":  True,
                "isSystemRole":      True,
                "customerId":        cid,
            },
            {
                "roleId":            "role-0002",
                "roleName":          "_USER_MANAGEMENT_ROLE",
                "roleDescription":   "User Management",
                "isSuperAdminRole":  False,
                "isSystemRole":      True,
                "customerId":        cid,
            },
            {
                "roleId":            "role-0003",
                "roleName":          "Security Reviewer",
                "roleDescription":   "Read-only security audit role",
                "isSuperAdminRole":  False,
                "isSystemRole":      False,
                "customerId":        cid,
            },
            {
                "roleId":            "role-0004",
                "roleName":          "Help Desk Admin",
                "roleDescription":   "Resets passwords and unlocks accounts",
                "isSuperAdminRole":  False,
                "isSystemRole":      False,
                "customerId":        cid,
            },
        ]

        # ── role_assignments ──────────────────────────────────────────────
        self._role_assignments = [
            {
                "roleAssignmentId": f"ra-{i:04d}",
                "roleId":           ["role-0001", "role-0002", "role-0003",
                                     "role-0004", "role-0003"][i],
                "assignedTo":       f"user-{i:04d}",
                "scopeType":        "CUSTOMER",
                "orgUnitId":        "",
                "customerId":       cid,
            }
            for i in range(5)
        ]

    # ── pagination ────────────────────────────────────────────────────────

    def _paginate(self, records: list, params: dict, next_key: str) -> GoogleWorkspaceMockResponse:
        """Return one page; embed nextPageToken in response if more remain."""
        token = params.get("pageToken", "0")
        offset = int(token) if token.isdigit() else 0
        page = records[offset: offset + _PAGE_SIZE]
        body = {next_key: page}
        next_offset = offset + _PAGE_SIZE
        if next_offset < len(records):
            body["nextPageToken"] = str(next_offset)
        return GoogleWorkspaceMockResponse(200, body)

    # ── routing ───────────────────────────────────────────────────────────

    def handle_get(self, path: str, params: dict) -> GoogleWorkspaceMockResponse:
        parts = [p for p in path.strip("/").split("/") if p]
        # /admin/directory/v1/users
        if parts == ["admin", "directory", "v1", "users"]:
            return self._paginate(self._users, params, "users")

        # /admin/directory/v1/groups
        if parts == ["admin", "directory", "v1", "groups"]:
            return self._paginate(self._groups, params, "groups")

        # /admin/directory/v1/groups/{groupKey}/members
        if (len(parts) == 6 and parts[:4] == ["admin", "directory", "v1", "groups"]
                and parts[5] == "members"):
            group_id = parts[4]
            members = self._group_members.get(group_id, [])
            return GoogleWorkspaceMockResponse(200, {"members": members})

        # /admin/directory/v1/customer/{customerId}/orgunits
        if (len(parts) == 6 and parts[:4] == ["admin", "directory", "v1", "customer"]
                and parts[5] == "orgunits"):
            return GoogleWorkspaceMockResponse(200, {"organizationUnits": self._org_units})

        # /admin/directory/v1/customer/{customerId}/roles
        if (len(parts) == 6 and parts[:4] == ["admin", "directory", "v1", "customer"]
                and parts[5] == "roles"):
            return self._paginate(self._roles, params, "items")

        # /admin/directory/v1/customer/{customerId}/roleassignments
        if (len(parts) == 6 and parts[:4] == ["admin", "directory", "v1", "customer"]
                and parts[5] == "roleassignments"):
            return self._paginate(self._role_assignments, params, "items")

        return GoogleWorkspaceMockResponse(404, {"error": f"No route for GET {path}"})

    def get_session(self) -> GoogleWorkspaceMockSession:
        return GoogleWorkspaceMockSession(self)


# ── singleton management ──────────────────────────────────────────────────────

_INSTANCE: Optional[GoogleWorkspaceMockAPI] = None
_INSTANCE_LOCK = threading.Lock()


def get_mock_api() -> GoogleWorkspaceMockAPI:
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        if _INSTANCE is None:
            _INSTANCE = GoogleWorkspaceMockAPI()
        return _INSTANCE


def reset_mock_api() -> GoogleWorkspaceMockAPI:
    global _INSTANCE  # noqa: PLW0603
    with _INSTANCE_LOCK:
        _INSTANCE = GoogleWorkspaceMockAPI()
        return _INSTANCE

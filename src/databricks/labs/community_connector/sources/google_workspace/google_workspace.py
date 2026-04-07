"""Google Workspace Directory connector for Lakeflow Community Connectors.

Ingests user inventory data from the Google Workspace Admin SDK Directory API.
Supports both a real service account (JSON key) and mock mode for demos/tests.

Tables
------
users             All users in the domain — identity, admin status, suspension, OU
groups            Security/mailing groups
group_members     Members of each group (requires domain or customer in table_options)
org_units         Organisational unit hierarchy
roles             Custom and system admin roles
role_assignments  Who holds which admin role

Authentication
--------------
Real mode  : Service Account JSON key with domain-wide delegation.
             Required OAuth scopes (set in Google Admin Console):
               https://www.googleapis.com/auth/admin.directory.user.readonly
               https://www.googleapis.com/auth/admin.directory.group.readonly
               https://www.googleapis.com/auth/admin.directory.orgunit.readonly
               https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly

Mock mode  : Pass ``service_account_json = "mock"`` — no Google account needed.
"""

import json
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.google_workspace.google_workspace_mock_api import get_mock_api
from databricks.labs.community_connector.sources.google_workspace.google_workspace_schemas import (
    SUPPORTED_TABLES, TABLE_METADATA, TABLE_SCHEMAS,
)

_DIRECTORY_BASE = "https://admin.googleapis.com/admin/directory/v1"
_TOKEN_URL      = "https://oauth2.googleapis.com/token"
_SCOPES = " ".join([
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
    "https://www.googleapis.com/auth/admin.directory.group.readonly",
    "https://www.googleapis.com/auth/admin.directory.orgunit.readonly",
    "https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly",
])


class GoogleWorkspaceLakeflowConnect(LakeflowConnect):

    def __init__(self, options: dict[str, str]) -> None:
        sa_json = options.get("service_account_json", "")
        if not sa_json:
            raise ValueError(
                "Google Workspace connector requires 'service_account_json' in options. "
                "Pass the full service account key JSON as a string, or 'mock' for demo mode."
            )

        self._customer_id = options.get("customer_id", "my_customer")
        self._domain      = options.get("domain", "")

        if sa_json == "mock":
            self._session = get_mock_api().get_session()
        else:
            self._session = self._build_session(sa_json, options.get("admin_email", ""))

    # ── session factory (real mode only) ──────────────────────────────────

    def _build_session(self, sa_json_str: str, admin_email: str):
        """Build a requests.Session with a service-account Bearer token.

        Uses the google-auth library when available; falls back to a manual
        JWT flow so the connector works without the google-auth extras package.
        """
        import requests

        try:
            import google.auth.transport.requests as gtr
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_info(
                json.loads(sa_json_str),
                scopes=_SCOPES.split(),
                subject=admin_email or None,
            )
            creds.refresh(gtr.Request())
            token = creds.token
        except ImportError:
            # Manual JWT → access-token exchange (no google-auth dependency)
            import time, base64, hashlib, hmac

            sa = json.loads(sa_json_str)
            now = int(time.time())
            header  = {"alg": "RS256", "typ": "JWT"}
            payload = {
                "iss":   sa["client_email"],
                "sub":   admin_email or sa["client_email"],
                "scope": _SCOPES,
                "aud":   _TOKEN_URL,
                "iat":   now,
                "exp":   now + 3600,
            }

            def _b64(obj):
                return base64.urlsafe_b64encode(
                    json.dumps(obj, separators=(",", ":")).encode()
                ).rstrip(b"=").decode()

            unsigned = f"{_b64(header)}.{_b64(payload)}"

            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding

            private_key = serialization.load_pem_private_key(
                sa["private_key"].encode(), password=None
            )
            sig = private_key.sign(unsigned.encode(), padding.PKCS1v15(), hashes.SHA256())
            jwt = f"{unsigned}.{base64.urlsafe_b64encode(sig).rstrip(b'=').decode()}"

            resp = requests.post(_TOKEN_URL,
                                 data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                                       "assertion": jwt},
                                 timeout=30)
            resp.raise_for_status()
            token = resp.json()["access_token"]

        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})
        return session

    # ── LakeflowConnect interface ─────────────────────────────────────────

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
            "users":            self._read_users,
            "groups":           self._read_groups,
            "group_members":    self._read_group_members,
            "org_units":        self._read_org_units,
            "roles":            self._read_roles,
            "role_assignments": self._read_role_assignments,
        }
        if table_name not in dispatch:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return dispatch[table_name](start_offset, table_options)

    # ── pagination helper ─────────────────────────────────────────────────

    def _paginate(self, url: str, params: dict, data_key: str) -> list[dict]:
        """Follow nextPageToken until all pages are consumed."""
        results = []
        next_token = None
        while True:
            req_params = dict(params)
            if next_token:
                req_params["pageToken"] = next_token
            resp = self._session.get(url, params=req_params, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            results.extend(body.get(data_key, []))
            next_token = body.get("nextPageToken")
            if not next_token:
                break
        return results

    # ── table readers ─────────────────────────────────────────────────────

    def _read_users(self, start_offset: dict, table_options: dict):
        params = {"maxResults": 500}
        if self._domain:
            params["domain"] = self._domain
        else:
            params["customer"] = self._customer_id

        raw = self._paginate(f"{_DIRECTORY_BASE}/users", params, "users")
        records = [
            {
                "id":              u.get("id"),
                "primary_email":   u.get("primaryEmail"),
                "full_name":       (u.get("name") or {}).get("fullName"),
                "given_name":      (u.get("name") or {}).get("givenName"),
                "family_name":     (u.get("name") or {}).get("familyName"),
                "is_admin":        u.get("isAdmin", False),
                "is_suspended":    u.get("suspended", False),
                "org_unit_path":   u.get("orgUnitPath"),
                "last_login_time": u.get("lastLoginTime"),
                "creation_time":   u.get("creationTime"),
                "customer_id":     u.get("customerId"),
            }
            for u in raw
        ]
        return iter(records), {}

    def _read_groups(self, start_offset: dict, table_options: dict):
        params = {"maxResults": 200}
        if self._domain:
            params["domain"] = self._domain
        else:
            params["customer"] = self._customer_id

        raw = self._paginate(f"{_DIRECTORY_BASE}/groups", params, "groups")
        records = [
            {
                "id":                  g.get("id"),
                "email":               g.get("email"),
                "name":                g.get("name"),
                "description":         g.get("description"),
                "direct_members_count": g.get("directMembersCount"),
                "admin_created":       g.get("adminCreated", False),
                "customer_id":         g.get("customerId"),
            }
            for g in raw
        ]
        return iter(records), {}

    def _read_group_members(self, start_offset: dict, table_options: dict):
        """Fetch members for every group found in the domain."""
        _, _ = self._read_groups({}, table_options)

        groups_params = {"maxResults": 200}
        if self._domain:
            groups_params["domain"] = self._domain
        else:
            groups_params["customer"] = self._customer_id

        groups_raw = self._paginate(f"{_DIRECTORY_BASE}/groups", groups_params, "groups")

        all_members = []
        for group in groups_raw:
            group_id = group.get("id")
            url = f"{_DIRECTORY_BASE}/groups/{group_id}/members"
            resp = self._session.get(url, timeout=30)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            for m in resp.json().get("members", []):
                all_members.append({
                    "id":       m.get("id"),
                    "email":    m.get("email"),
                    "role":     m.get("role"),
                    "type":     m.get("type"),
                    "status":   m.get("status"),
                    "group_id": group_id,
                })

        return iter(all_members), {}

    def _read_org_units(self, start_offset: dict, table_options: dict):
        url = f"{_DIRECTORY_BASE}/customer/{self._customer_id}/orgunits"
        resp = self._session.get(url, params={"type": "all"}, timeout=30)
        resp.raise_for_status()
        raw = resp.json().get("organizationUnits", [])
        records = [
            {
                "org_unit_id":          o.get("orgUnitId"),
                "name":                 o.get("name"),
                "org_unit_path":        o.get("orgUnitPath"),
                "parent_org_unit_path": o.get("parentOrgUnitPath"),
                "description":          o.get("description"),
                "block_inheritance":    o.get("blockInheritance", False),
                "customer_id":          o.get("customerId"),
            }
            for o in raw
        ]
        return iter(records), {}

    def _read_roles(self, start_offset: dict, table_options: dict):
        url = f"{_DIRECTORY_BASE}/customer/{self._customer_id}/roles"
        raw = self._paginate(url, {"maxResults": 100}, "items")
        records = [
            {
                "role_id":             r.get("roleId"),
                "role_name":           r.get("roleName"),
                "role_description":    r.get("roleDescription"),
                "is_super_admin_role": r.get("isSuperAdminRole", False),
                "is_system_role":      r.get("isSystemRole", False),
                "customer_id":         self._customer_id,
            }
            for r in raw
        ]
        return iter(records), {}

    def _read_role_assignments(self, start_offset: dict, table_options: dict):
        url = f"{_DIRECTORY_BASE}/customer/{self._customer_id}/roleassignments"
        raw = self._paginate(url, {"maxResults": 200}, "items")
        records = [
            {
                "role_assignment_id": ra.get("roleAssignmentId"),
                "role_id":            ra.get("roleId"),
                "assigned_to":        ra.get("assignedTo"),
                "scope_type":         ra.get("scopeType"),
                "org_unit_id":        ra.get("orgUnitId", ""),
                "customer_id":        self._customer_id,
            }
            for ra in raw
        ]
        return iter(records), {}

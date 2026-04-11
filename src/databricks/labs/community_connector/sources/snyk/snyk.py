import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Iterator
from pyspark.sql.types import StructType
from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.snyk.snyk_mock_api import get_mock_api
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

        api_base_url = options.get("api_base_url", "").rstrip("/")
        self._rest_base = api_base_url or _REST_BASE.rstrip("/")
        self._v1_base = options.get("v1_base_url", _V1_BASE).rstrip("/")
        # Events endpoint lives at the host root (e.g. https://faker.host/api/events)
        self._events_base = api_base_url or _REST_BASE.rstrip("/").replace("/rest", "")
        self._mock = (token == "mock")

        # Store only picklable auth config; session is created lazily in _get_session().
        # requests.Session holds thread locks and cannot be pickled by Spark workers.
        if not self._mock:
            if api_base_url and api_base_url != _REST_BASE.rstrip("/"):
                self._auth_headers = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                }
            else:
                self._auth_headers = {
                    "Authorization": f"token {token}",
                    "Accept": "application/vnd.api+json",
                }

    def _get_session(self):
        """Create a requests.Session on demand (not stored, so the connector stays picklable)."""
        if self._mock:
            return get_mock_api().get_session()
        import requests
        session = requests.Session()
        session.headers.update(self._auth_headers)
        return session

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
            "detections_unified": self._read_detections_unified,
            "organizations": self._read_organizations,
            "projects": self._read_projects,
            "issues": self._read_issues,
            "targets": self._read_targets,
            "users": self._read_users,
            "vulnerabilities": self._read_vulnerabilities,
            "events": self._read_events,
        }
        if table_name not in dispatch:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return dispatch[table_name](start_offset, table_options)

    def _read_detections_unified(self, start_offset: dict, table_options: dict):
        """Single snapshot stream for detections: issues, events, vulnerabilities → bronze-shaped rows.

        Each row matches ``detections_unified`` schema (JSON in ``data`` / ``_raw`` for downstream VARIANT cast).
        Configure ``streams`` in table_configuration (comma-separated), default
        ``issues,events,vulnerabilities``.
        """
        return self._iter_detections_unified(table_options), {}

    def _iter_detections_unified(self, table_options: dict):
        streams_raw = table_options.get("streams", "issues,events,vulnerabilities")
        if isinstance(streams_raw, str):
            streams = [x.strip() for x in streams_raw.split(",") if x.strip()]
        else:
            streams = list(streams_raw)
        preferred_order = ("events", "issues", "vulnerabilities")
        streams = [s for s in preferred_order if s in streams]

        for name in streams:
            if name == "events":
                records, _ = self._read_events({}, table_options)
                for row in records:
                    payload = dict(row)
                    payload["record_type"] = "event"
                    yield self._bronze_row_from_payload("event", payload)
            elif name == "issues":
                records, _ = self._read_issues({}, table_options)
                for row in records:
                    payload = dict(row)
                    payload["record_type"] = "issue"
                    yield self._bronze_row_from_payload("issue", payload)
            elif name == "vulnerabilities":
                records, _ = self._read_vulnerabilities({}, table_options)
                for row in records:
                    payload = dict(row)
                    payload["record_type"] = "vulnerability"
                    yield self._bronze_row_from_payload("vulnerability", payload)

    def _time_key_for_record(self, record_type: str, payload: dict) -> str:
        if record_type == "event":
            return str(payload.get("modification_time") or payload.get("creation_time") or "")
        if record_type == "issue":
            return str(payload.get("updated_at") or payload.get("created_at") or "")
        return str(payload.get("publication_time") or payload.get("disclosure_time") or "")

    def _event_time_to_datetime(self, tkey: str):
        """Parse API time string to UTC datetime for bronze ``time`` (TimestampType)."""
        if not tkey or not str(tkey).strip():
            return datetime.now(timezone.utc)
        ts = str(tkey).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            return datetime.now(timezone.utc)

    def _bronze_row_from_payload(self, record_type: str, payload: dict) -> dict:
        """Build one Lakeflow row aligned with UC bronze: VARIANT data/_raw, timestamps, rawstr, _metadata."""
        raw_json = json.dumps(payload, default=str, sort_keys=True)
        rid = str(payload.get("id", ""))
        tkey = self._time_key_for_record(record_type, payload)
        lw_id = hashlib.sha256(f"{rid}|{tkey}".encode()).hexdigest()
        team = str(payload.get("org") or payload.get("organization_id") or "")
        time_dt = self._event_time_to_datetime(tkey)
        now_dt = datetime.now(timezone.utc)
        meta = {
            "file_path": f"snyk://detections/{record_type}/{rid}",
            "file_name": "snyk_events",
            "file_size": 0,
            "file_block_start": 0,
            "file_block_length": 0,
            "file_modification_time": now_dt,
        }
        return {
            "lw_id": lw_id,
            "time": time_dt,
            "team_id": team,
            "data": raw_json,
            "_raw": raw_json,
            "rawstr": raw_json,
            "_metadata": meta,
            "ingest_time_utc": now_dt,
        }

    def _paginate_rest(self, url: str, params: dict) -> list[dict]:
        """Paginate Snyk REST (JSON:API) using links.next cursor."""
        session = self._get_session()
        results = []
        next_url = url
        next_params: dict | None = params

        while next_url:
            resp = session.get(next_url, params=next_params, timeout=30)
            if resp.status_code == 429:
                # Rate-limited — wait and retry once
                time.sleep(60)
                resp = session.get(next_url, params=next_params, timeout=30)
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
        if self._is_faker_mode:
            return iter([]), {}
        url = f"{self._v1_base}/orgs"
        resp = self._get_session().get(url, timeout=30)
        resp.raise_for_status()
        orgs_raw = resp.json().get("orgs", [])
        records = [
            {"id": o.get("id"), "name": o.get("name"),
             "slug": o.get("slug"), "url": o.get("url")}
            for o in orgs_raw
        ]
        return iter(records), {}

    @property
    def _is_faker_mode(self) -> bool:
        """True when connected to a custom/faker host instead of the real Snyk API."""
        return self._rest_base != _REST_BASE.rstrip("/")

    def _require_org_id(self, table_options: dict, table_name: str):
        """Return the org_id, or None when running against a faker/custom host.

        For the real Snyk API, org_id is required and a missing/placeholder
        value raises ValueError.  For a custom base URL (e.g. the faker) the
        org-scoped REST endpoints don't exist, so we return None to signal
        that the caller should yield an empty result set.
        """
        org_id = table_options.get("org_id", "")
        if not org_id or org_id.startswith("<"):
            if self._is_faker_mode:
                return None  # silently skip — faker has no org-based endpoints
            raise ValueError(
                f"Table '{table_name}' requires 'org_id' in table_options. "
                "Add it to table_configuration in the pipeline spec."
            )
        return org_id

    def _read_projects(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "projects")
        if org_id is None:
            return iter([]), {}
        url = f"{self._rest_base}/orgs/{org_id}/projects"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_issues(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "issues")
        if org_id is None:
            return iter([]), {}
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
        if org_id is None:
            return iter([]), {}
        url = f"{self._rest_base}/orgs/{org_id}/targets"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_users(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "users")
        if org_id is None:
            return iter([]), {}
        url = f"{self._rest_base}/orgs/{org_id}/users"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_vulnerabilities(self, start_offset: dict, table_options: dict):
        org_id = self._require_org_id(table_options, "vulnerabilities")
        if org_id is None:
            return iter([]), {}
        url = f"{self._rest_base}/orgs/{org_id}/vulnerabilities"
        params = {"version": _API_VERSION, "limit": 100}
        raw = self._paginate_rest(url, params)
        records = [self._flatten_jsonapi(r) for r in raw]
        return iter(records), {}

    def _read_events(self, start_offset: dict, table_options: dict):
        """Read from the /api/events NDJSON endpoint (Snyk faker / custom deployments)."""
        url = f"{self._events_base}/api/events"
        resp = self._get_session().get(url, timeout=30)
        resp.raise_for_status()
        records = []
        for line in resp.text.strip().splitlines():
            if not line.strip():
                continue
            raw = json.loads(line)
            records.append({
                "id": raw.get("id"),
                "title": raw.get("title"),
                "severity": raw.get("severity"),
                "cvss_score": raw.get("cvssScore"),
                "language": raw.get("language"),
                "package_name": raw.get("packageName"),
                "module_name": raw.get("moduleName"),
                "package_manager": raw.get("packageManager"),
                "version": raw.get("version"),
                "primary_fixed_version": raw.get("primaryFixedVersion"),
                "fixed_in": json.dumps(raw.get("fixedIn") or []),
                "cve_ids": json.dumps(raw.get("cveIds") or []),
                "exploit": raw.get("exploit"),
                "org": raw.get("org"),
                "project_name": raw.get("projectName"),
                "path": raw.get("path"),
                "is_upgradable": raw.get("isUpgradable"),
                "is_patchable": raw.get("isPatchable"),
                "is_pinnable": raw.get("isPinnable"),
                "malicious": raw.get("malicious"),
                "disclosure_time": raw.get("disclosureTime"),
                "publication_time": raw.get("publicationTime"),
                "creation_time": raw.get("creationTime"),
                "modification_time": raw.get("modificationTime"),
                "event_type": raw.get("eventType"),
                "description": raw.get("description"),
            })
        return iter(records), {}

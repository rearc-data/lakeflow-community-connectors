"""Example connector for the simulated source API."""

import copy
import json
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional, List
import hashlib


from pyspark.sql.types import StructType

from .wiz_client import get_wiz_client
from .wiz_client_mock import get_mock_wiz_client
from .wiz_schemas import EVENT_CONFIGS, TABLES, BRONZE_SCHEMA
from databricks.labs.community_connector.interface import LakeflowConnect


class WizLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the simulated Example source."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        # ── Client selection: real or mock ────────────────────────────────────
        # options["use_mock"] = "true"  → WizMockClient (no credentials needed)
        # options["use_mock"] absent    → WizGraphQLClient (real Wiz API)
        #
        # In production (spec.yaml): use_mock is absent.
        # In local dev / unit tests:  set options["use_mock"] = "true"
        # In the Databricks job UI:   add use_mock = "true" as a task param
        #   to point the entire pipeline at mock data.
        use_mock = options.get("use_mock", "false").lower() == "true"

        if use_mock:
            self._client = get_mock_wiz_client(options)
            print("[WizLakeflowConnect] Using MOCK client — no real API calls")
        else:
            # Credentials come from the Databricks secret referenced in spec.yaml.
            # The framework loads the secret JSON and passes it as options.
            # Same JSON format as your existing WIZ_Collector secret:
            #   {"base_url": "...", "client_id": "...", "client_secret": "..."}
            self._client = get_wiz_client(options)
            print(f"[WizLakeflowConnect] Using REAL client → {options.get('base_url')}")

        # ── Config ────────────────────────────────────────────────────────────
        self._portal_url = options.get("portal_base_url", "https://app.wiz.io")
        self._configs    = copy.deepcopy(EVENT_CONFIGS)

        # Inject project_id if provided (mirrors your Step 4 widget injection)
        project_id = options.get("project_id")
        if project_id:
            self._configs["issue"]["filter_variables"]["project"]               = [project_id]
            self._configs["vulnerability_finding"]["filter_variables"]["projectId"] = project_id
            self._configs["detection"]["filter_variables"]["projectId"]         = project_id
            print(f"[WizLakeflowConnect] Scoped to project: {project_id}")

    def list_tables(self) -> list[str]:
        """
        Return the list of tables this connector exposes.
        Equivalent to example's list_tables() — but static (no API call needed).
        """
        return TABLES

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """
        Return the Spark schema for table_name.
        All 4 Wiz tables share the same bronze schema.
        Silver/gold schemas live in your preset YAML — unchanged.
        """
        self._validate_table(table_name)
        return BRONZE_SCHEMA

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """
        Return ingestion metadata for table_name.

        ingestion_type options:
            "cdc"          → cursor-based incremental (issues, vulns, audit_log)
            "append_only"  → cursor-based append, never updates (detection)
            "snapshot"     → full reload every run (not used for Wiz)

        cursor_field is the field name we track for incremental sync.
        The framework uses this in its generic offset validation tests.
        """
        self._validate_table(table_name)
        return {
            "ingestion_type": "cdc",
            "cursor_field": "collected_at",  # or "event_time"
            "description": "Unified Wiz events stream",
        }

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """
        Fetch records for table_name starting from start_offset.

        This is called by the framework on each scheduled pipeline run.
        Returns: (iterator_of_bronze_rows, end_offset)

        start_offset=None → first run → initial/full-load behaviour.
        The end_offset is saved by the framework and passed back as
        start_offset on the next run.

        ROUTING (mirrors example's read_table dispatcher):
            issue               → _read_issues()
            vulnerability_finding → _read_vulns()
            audit_log           → _read_audit_logs()
            detection           → _read_detections()  (cursor-based)
        """
        self._validate_table(table_name)
        now = datetime.now(timezone.utc)

        if table_name == "wiz_api_events":
            return self._read_all_events(start_offset, now)
        else:
            raise ValueError(f"Unsupported table: {table_name}")
        
    def _read_all_events(self, start_offset, now):
        # ── STEP 1 — split offsets ─────────────────────────────────────────────
        issue_offset = start_offset.get("issue") if start_offset else None
        vuln_offset = start_offset.get("vulnerability_finding") if start_offset else None
        audit_offset = start_offset.get("audit_log") if start_offset else None
        detection_offset = start_offset.get("detection") if start_offset else None

        # ── STEP 2 — call individual readers ───────────────────────────────────
        issue_rows, issue_end = self._read_issues(issue_offset, now)
        vuln_rows, vuln_end = self._read_vulns(vuln_offset, now)
        audit_rows, audit_end = self._read_audit_logs(audit_offset, now)
        detection_rows, detection_end = self._read_detections(detection_offset, now)

        # ── STEP 3 — merge rows using generator (memory-safe) ──────────────────
        def _chain(*iterators):
            for it in iterators:
                if it is not None:
                    yield from it

        rows = _chain(issue_rows, vuln_rows, audit_rows, detection_rows)

        # ── STEP 4 — merge offsets ─────────────────────────────────────────────
        end_offset = {
            "issue": issue_end,
            "vulnerability_finding": vuln_end,
            "audit_log": audit_end,
            "detection": detection_end,
        }

        return rows, end_offset

    def _read_issues(
        self,
        start_offset: dict | None,
        now: datetime,
    ) -> tuple[Iterator[dict], dict]:
        """
        Offset shape:
            None                           → initial
            {"cursor": "...", 
            "full_sync": true}            → full_sync  (2-pass: active + RESOLVED)
            {"cursor": "...",
            "re_assessed": true}          → re_assessed
            {"cursor": "..."}              → incremental

        full_sync can be triggered two ways:
            1. Reset the offset to None externally (databricks pipelines reset)
            — this triggers initial, which is equivalent to full_sync pass 1
            2. Set full_sync = "true" in spec.yaml table options
            — preserves the existing cursor, runs both passes, then clears the flag
        """
        cfg       = self._configs["issue"]
        fv        = cfg["filter_variables"]
        # Read from table_options in spec.yaml — false by default
        full_sync = self.options.get("full_sync", "false").lower() == "true"

        def _paginate(filterBy: dict) -> list[dict]:
            return self._client.paginate_connection(
                query=cfg["query"],
                connection_field=cfg["connection_field"],
                variables={"filterBy": filterBy, "orderBy": cfg["order_by"]},
                page_size=cfg["page_size"],
            )

        nodes: list[dict] = []

        # ── initial ───────────────────────────────────────────────────────────────
        if start_offset is None or not start_offset.get("cursor"):
            filterBy = {"status": ["OPEN", "IN_PROGRESS"], "severity": fv["severity"]}
            if fv.get("project"):
                filterBy["project"] = fv["project"]
            nodes.extend(_paginate(filterBy))
            end_offset = {"cursor": self._fmt(now)}

        # ── full_sync — checked before incremental so the flag always wins ────────
        elif full_sync:
            print("  [issue] full_sync — pass 1: OPEN, IN_PROGRESS, REJECTED")
            fby_p1 = {"status": ["OPEN", "IN_PROGRESS", "REJECTED"], "severity": fv["severity"]}
            if fv.get("project"):
                fby_p1["project"] = fv["project"]
            pass1 = _paginate(fby_p1)
            nodes.extend(pass1)
            print(f"  [issue] full_sync pass 1 — {len(pass1)} records")

            # Pass 2: RESOLVED since last full_sync (falls back to 7d if no prior full_sync)
            last_full = (
                start_offset.get("full_sync_cursor")
                or self._fmt(now - timedelta(days=7))
            )
            print(f"  [issue] full_sync — pass 2: RESOLVED  resolvedAt: {last_full} → {self._fmt(now)}")
            fby_p2 = {
                "status":     ["RESOLVED"],
                "severity":   fv["severity"],
                "resolvedAt": {"after": last_full, "before": self._fmt(now)},
            }
            if fv.get("project"):
                fby_p2["project"] = fv["project"]
            pass2 = _paginate(fby_p2)
            nodes.extend(pass2)
            print(f"  [issue] full_sync pass 2 — {len(pass2)} RESOLVED")

            # Advance both cursors; consumer must set full_sync=false after this run
            end_offset = {
                "cursor":           self._fmt(now),
                "full_sync_cursor": self._fmt(now),
            }

        # ── re_assessed ───────────────────────────────────────────────────────────
        elif start_offset.get("re_assessed"):
            after     = start_offset.get("cursor", self._fmt(now - timedelta(hours=1)))
            before    = self._fmt(now)
            after_dt  = self._parse_ts(after)
            before_dt = self._parse_ts(before)

            fby_p1 = {"status": ["OPEN", "IN_PROGRESS", "REJECTED"], "severity": fv["severity"]}
            if fv.get("project"):
                fby_p1["project"] = fv["project"]
            for issue in _paginate(fby_p1):
                updated = issue.get("updatedAt", "")
                try:
                    if updated and after_dt < self._parse_ts(updated) < before_dt:
                        nodes.append(issue)
                except ValueError:
                    pass

            fby_p2 = {
                "status":          ["RESOLVED"],
                "severity":        fv["severity"],
                "statusChangedAt": {"after": after, "before": before},
            }
            if fv.get("project"):
                fby_p2["project"] = fv["project"]
            nodes.extend(_paginate(fby_p2))

            end_offset = {
                "cursor":           self._fmt(now),
                "full_sync_cursor": start_offset.get("full_sync_cursor"),
                "re_assessed":      False,
            }

        # ── incremental ───────────────────────────────────────────────────────────
        else:
            after  = start_offset["cursor"]
            before = self._fmt(now)
            filterBy = {
                "status":          ["OPEN", "IN_PROGRESS", "REJECTED", "RESOLVED"],
                "severity":        fv["severity"],
                "statusChangedAt": {"after": after, "before": before},
            }
            if fv.get("project"):
                filterBy["project"] = fv["project"]
            nodes.extend(_paginate(filterBy))
            end_offset = {
                "cursor":           self._fmt(now),
                "full_sync_cursor": start_offset.get("full_sync_cursor"),
            }

        for n in nodes:
            n["sourceURL"]  = f"{self._portal_url}/issues#~(issue~'{n.get('id', '')}')"
            n["event_type"] = "issue"

        return iter(self._to_bronze(nodes, now)), end_offset

    # ─────────────────────────────────────────────────────────────────────────

    def _read_vulns(
        self,
        start_offset: dict | None,
        now: datetime,
    ) -> tuple[Iterator[dict], dict]:
        """
        REPLACES: collect_vulns() from notebook Step 9.

        Offset shape:
            None                          → initial (all statuses, no time filter)
            {"cursor": "<ISO timestamp>"} → incremental (statusUpdatedAt + overlap)
        """
        cfg = self._configs["vulnerability_finding"]
        fv  = cfg["filter_variables"]
        inc = cfg["incremental"]
        full_sync = self.options.get("full_sync", "false").lower() == "true"

        def _build_filter(extra: dict | None = None) -> dict:
            f: dict = {
                "status":         fv["status"],
                "assetType":      fv["assetType"],
                "vendorSeverity": fv["vendorSeverity"],
            }
            if fv.get("projectId"):            f["projectId"]            = fv["projectId"]
            if fv.get("relatedIssueSeverity"): f["relatedIssueSeverity"] = fv["relatedIssueSeverity"]
            if extra:                          f.update(extra)
            return f

        order = {"field": "FIRST_DETECTED_AT", "direction": "ASC"}

        # ── initial ───────────────────────────────────────────────
        if start_offset is None or not start_offset.get("cursor") or full_sync:
            nodes = self._client.paginate_connection(
                query=cfg["query"], connection_field=cfg["connection_field"],
                variables={"filterBy": _build_filter(), "orderBy": order},
                page_size=cfg["page_size"],
            )
            end_offset = {"cursor": self._fmt(now)}

        # ── incremental ───────────────────────────────────────────────────────
        else:
            overlap   = inc["overlap_hours"]
            last_dt   = self._parse_ts(start_offset["cursor"])
            after     = self._fmt(last_dt - timedelta(hours=overlap))
            before    = self._fmt(now)
            nodes = self._client.paginate_connection(
                query=cfg["query"], connection_field=cfg["connection_field"],
                variables={
                    "filterBy": _build_filter({"statusUpdatedAt": {"after": after, "before": before}}),
                    "orderBy":  order,
                },
                page_size=cfg["page_size"],
            )
            end_offset = {
                "cursor":          self._fmt(now)
            }

        for n in nodes:
            n["event_type"] = "vulnerability_finding"

        return iter(self._to_bronze(nodes, now)), end_offset

    # ─────────────────────────────────────────────────────────────────────────

    def _read_audit_logs(
        self,
        start_offset: dict | None,
        now: datetime,
    ) -> tuple[Iterator[dict], dict]:
        """
        REPLACES: collect_audit_logs() from notebook Step 10.

        Offset shape:
            None                          → initial (now - historical_days → now)
            {"cursor": "<ISO timestamp>"} → incremental (last_timestamp → now)
        """
        cfg = self._configs["audit_log"]
        inc = cfg["incremental"]

        if start_offset is None or not start_offset.get("cursor"):
            after = self._fmt(now - timedelta(days=inc["historical_days"]))
        else:
            after = start_offset["cursor"]

        filterBy = {
            **cfg["filter_variables"],
            "timestamp": {"after": after, "before": self._fmt(now)},
        }

        nodes = self._client.paginate_connection(
            query=cfg["query"], connection_field=cfg["connection_field"],
            variables={"filterBy": filterBy},
            page_size=cfg["page_size"],
        )

        for n in nodes:
            n["event_type"] = "audit_log"

        end_offset = {"cursor": self._fmt(now)}
        return iter(self._to_bronze(nodes, now)), end_offset

    # ─────────────────────────────────────────────────────────────────────────

    def _read_detections(
        self,
        start_offset: dict | None,
        now: datetime,
    ) -> tuple[Iterator[dict], dict]:
        """
        REPLACES: collect_detections() from notebook Step 11.

        Offset shape:
            None              → initial (inLast=historical_days, cursor=None)
            {"cursor": "..."}  → incremental (resume from saved endCursor)

        NOTE ON CURSOR SAVING:
        The original Splunk TA specifies that the endCursor should be saved
        after every page to the checkpoint Delta table. 
        The framework saves the offset only after read_table() returns. 
        
        This means a mid-run cluster crash restarts
        from the beginning of the inLast window. No data is lost because
        the window is rolling — it just re-fetches. For very large
        environments, lower historical_days in EVENT_CONFIGS and run more
        frequently to reduce re-fetch scope.
        """
        cfg               = self._configs["detection"]
        fv                = cfg["filter_variables"]
        inc               = cfg["incremental"]
        include_triggering = cfg["include_triggering_events"]
        page_size          = cfg["page_size"] if include_triggering else 500

        resume_cursor = start_offset.get("cursor") if start_offset else None

        filterBy: dict = {
            "severity":  fv["severity"],
            "createdAt": {
                "inLast": {
                    "amount": inc["historical_days"],
                    "unit":   "DurationFilterValueUnitDays",
                }
            },
        }
        if fv.get("projectId"):
            filterBy["projectId"] = fv["projectId"]

        variables: dict = {
            "filterBy":                filterBy,
            "orderBy":                 cfg["order_by"],
            "includeTriggeringEvents": include_triggering,
            "first":                   page_size,
            "after":                   resume_cursor,
        }

        # ── Manual pagination — tracks last cursor ────────────────────────────
        nodes: list[dict]      = []
        last_cursor: str | None = resume_cursor

        while True:
            data       = self._client.execute(query=cfg["query"], variables=variables)
            connection = data.get(cfg["connection_field"], {})
            batch      = [n for n in connection.get("nodes", []) if isinstance(n, dict)]
            nodes.extend(batch)

            page_info  = connection.get("pageInfo", {})
            has_next   = bool(page_info.get("hasNextPage"))
            end_cursor = page_info.get("endCursor")

            if end_cursor:
                last_cursor = end_cursor
            if not has_next or not (isinstance(end_cursor, str) and end_cursor):
                break
            variables["after"] = end_cursor

        for n in nodes:
            n["event_type"] = "detection"

        end_offset = {
            "cursor": last_cursor or (start_offset.get("cursor") if start_offset else None)
        }
        return iter(self._to_bronze(nodes, now)), end_offset

    @staticmethod
    def python_sortable_id(
        record: dict,
        time_value: datetime,
        hash_column_values: Optional[List] = None,
        n_hash_chars: int = 11,
    ):
        # ── Step 1: time prefix (same as Spark)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

        if time_value.tzinfo is None:
            time_value = time_value.replace(tzinfo=timezone.utc)

        micros = int((time_value - epoch).total_seconds() * 1_000_000)
        prefix = format(micros, "x")   # hex string

        # ── Step 2: hash input (match Spark intent)
        if hash_column_values and len(hash_column_values) > 0:
            # mimic Spark sorting of expressions
            values = sorted(hash_column_values, key=lambda x: str(x))
        else:
            values = [time_value]

        hash_input = "|".join(str(v) for v in values)
        hash_hex = hashlib.sha256(hash_input.encode()).hexdigest()

        suffix = hash_hex[-n_hash_chars:]

        return prefix + suffix

    # ─────────────────────────────────────────────────────────────────────────
    # Shared helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _to_bronze2(self, nodes: list[dict], collected_at: datetime) -> list[dict]:
        """
        Convert raw API nodes → bronze rows.
        Same shape as your write_to_bronze() in notebook Step 7,
        minus the Spark write (the framework does that).
        """
        return [
            {
                "id":           str(n.get("id", "")),
                "event_type":   n.get("event_type", ""),
                "raw":          json.dumps(n, default=str),
                "collected_at": collected_at,
            }
            for n in nodes
        ]
    
    def _to_bronze(self, nodes: list[dict], collected_at: datetime) -> list[dict]:
        rows = []

        for n in nodes:
            event_type = n.get("event_type")

            # ── derive event time ─────────────────────────────────────────
            if event_type == "issue":
                time_val = n.get("createdAt")
            elif event_type == "vulnerability_finding":
                time_val = n.get("firstDetectedAt")
            elif event_type == "audit_log":
                time_val = n.get("timestamp")
            elif event_type == "detection":
                time_val = n.get("createdAt")
            else:
                time_val = None

            # convert string → datetime
            if isinstance(time_val, str):
                try:
                    time_val = datetime.fromisoformat(time_val.replace("Z", "+00:00"))
                except Exception:
                    time_val = None

            # fallback
            if not time_val:
                time_val = collected_at

            record_id = str(n.get("id", ""))

            # ── generate dasl_id ─────────────────────────────────────────
            dasl_id = self.python_sortable_id(
                record={"record_id": record_id},
                time_value=time_val,
                hash_column_values=[record_id],
            )

            rows.append({
                "dasl_id": dasl_id,
                "time": time_val,
                "_raw_json": json.dumps(n, default=str),              # for Lakewatch
                "collected_at": collected_at,
                "event_type": event_type,
                "record_id": record_id,
                "_metadata": None            # safe to keep null
            })

        return rows

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported: {TABLES}"
            )

    @staticmethod
    def _fmt(dt: datetime) -> str:
        """ISO-8601 UTC string with Z suffix — same as fmt() in notebook Step 5."""
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _parse_ts(s: str) -> datetime:
        """Parse ISO-8601 string → datetime — same as parse_ts() in notebook Step 5."""
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
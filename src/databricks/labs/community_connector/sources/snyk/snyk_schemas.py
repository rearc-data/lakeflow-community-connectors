from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

SUPPORTED_TABLES = [
    "detections_unified",
    "organizations",
    "projects",
    "issues",
    "targets",
    "users",
    "vulnerabilities",
    "events",
]

_METADATA_BRONZE = StructType(
    [
        StructField("file_path", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_size", LongType(), True),
        StructField("file_block_start", LongType(), True),
        StructField("file_block_length", LongType(), True),
        StructField("file_modification_time", TimestampType(), True),
    ]
)

# Single-stream bronze for Lakeflow → cyber_prod.bronze.snyk_events (matches Lakewatch bronze: VARIANT + timestamps + rawstr).
DETECTIONS_UNIFIED_SCHEMA = StructType(
    [
        StructField("lw_id", StringType(), False),
        StructField("time", TimestampType(), True),
        StructField("team_id", StringType(), True),
        StructField("data", VariantType(), True),
        StructField("_raw", VariantType(), True),
        StructField("rawstr", StringType(), True),
        StructField("_metadata", _METADATA_BRONZE, True),
        StructField("ingest_time_utc", TimestampType(), True),
    ]
)

TABLE_SCHEMAS = {
    "detections_unified": DETECTIONS_UNIFIED_SCHEMA,
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
    "events": StructType([
        StructField("id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("cvss_score", DoubleType(), True),
        StructField("language", StringType(), True),
        StructField("package_name", StringType(), True),
        StructField("module_name", StringType(), True),
        StructField("package_manager", StringType(), True),
        StructField("version", StringType(), True),
        StructField("primary_fixed_version", StringType(), True),
        StructField("fixed_in", StringType(), True),
        StructField("cve_ids", StringType(), True),
        StructField("exploit", StringType(), True),
        StructField("org", StringType(), True),
        StructField("project_name", StringType(), True),
        StructField("path", StringType(), True),
        StructField("is_upgradable", BooleanType(), True),
        StructField("is_patchable", BooleanType(), True),
        StructField("is_pinnable", BooleanType(), True),
        StructField("malicious", BooleanType(), True),
        StructField("disclosure_time", StringType(), True),
        StructField("publication_time", StringType(), True),
        StructField("creation_time", StringType(), True),
        StructField("modification_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("description", StringType(), True),
    ]),
}

TABLE_METADATA = {
    "detections_unified": {
        "ingestion_type": "snapshot",
        "primary_keys": ["lw_id"],
    },
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
    "vulnerabilities": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "events": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
}
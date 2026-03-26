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

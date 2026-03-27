from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, StringType, StructField, StructType
)

SUPPORTED_TABLES = [
    "issues", "cloud_resources", "vulnerabilities",
    "projects", "users", "controls",
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
    "cloud_resources": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("cloudProvider", StringType(), True),
        StructField("tags", StringType(), True),
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

from pyspark.sql.types import (
    BooleanType, StringType, StructField, StructType
)

SUPPORTED_TABLES = [
    "users",
    "groups",
    "group_members",
    "org_units",
    "roles",
    "role_assignments",
]

TABLE_SCHEMAS = {
    "users": StructType([
        StructField("id",               StringType(),  False),
        StructField("primary_email",    StringType(),  True),
        StructField("full_name",        StringType(),  True),
        StructField("given_name",       StringType(),  True),
        StructField("family_name",      StringType(),  True),
        StructField("is_admin",         BooleanType(), True),
        StructField("is_suspended",     BooleanType(), True),
        StructField("org_unit_path",    StringType(),  True),
        StructField("last_login_time",  StringType(),  True),
        StructField("creation_time",    StringType(),  True),
        StructField("customer_id",      StringType(),  True),
    ]),
    "groups": StructType([
        StructField("id",               StringType(),  False),
        StructField("email",            StringType(),  True),
        StructField("name",             StringType(),  True),
        StructField("description",      StringType(),  True),
        StructField("direct_members_count", StringType(), True),
        StructField("admin_created",    BooleanType(), True),
        StructField("customer_id",      StringType(),  True),
    ]),
    "group_members": StructType([
        StructField("id",               StringType(),  False),
        StructField("email",            StringType(),  True),
        StructField("role",             StringType(),  True),
        StructField("type",             StringType(),  True),
        StructField("status",           StringType(),  True),
        StructField("group_id",         StringType(),  True),
    ]),
    "org_units": StructType([
        StructField("org_unit_id",      StringType(),  False),
        StructField("name",             StringType(),  True),
        StructField("org_unit_path",    StringType(),  True),
        StructField("parent_org_unit_path", StringType(), True),
        StructField("description",      StringType(),  True),
        StructField("block_inheritance", BooleanType(), True),
        StructField("customer_id",      StringType(),  True),
    ]),
    "roles": StructType([
        StructField("role_id",          StringType(),  False),
        StructField("role_name",        StringType(),  True),
        StructField("role_description", StringType(),  True),
        StructField("is_super_admin_role", BooleanType(), True),
        StructField("is_system_role",   BooleanType(), True),
        StructField("customer_id",      StringType(),  True),
    ]),
    "role_assignments": StructType([
        StructField("role_assignment_id", StringType(), False),
        StructField("role_id",            StringType(), True),
        StructField("assigned_to",        StringType(), True),
        StructField("scope_type",         StringType(), True),
        StructField("org_unit_id",        StringType(), True),
        StructField("customer_id",        StringType(), True),
    ]),
}

# All tables are snapshot — the Directory API does not expose a change cursor.
# Re-run the pipeline to pick up the latest state.
TABLE_METADATA = {
    "users": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "groups": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "group_members": {
        "ingestion_type": "snapshot",
        "primary_keys": ["id"],
    },
    "org_units": {
        "ingestion_type": "snapshot",
        "primary_keys": ["org_unit_id"],
    },
    "roles": {
        "ingestion_type": "snapshot",
        "primary_keys": ["role_id"],
    },
    "role_assignments": {
        "ingestion_type": "snapshot",
        "primary_keys": ["role_assignment_id"],
    },
}

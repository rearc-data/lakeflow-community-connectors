from databricks.labs.community_connector.sources.wiz.wiz import WizLakeflowConnect
from datetime import datetime, timezone

# 👇 EXACTLY what Lakeflow passes
options = {
    "config_json": """
    {
        "base_url": "https://mock-wiz-api.noop.app",
        "client_id": "cyber-solutions",
        "client_secret": "cyber-solutions",
        "audience": "wiz-api",
        "auth_enabled": true
    }
    """,
    "use_mock": "true"
}

connector = WizLakeflowConnect(options)

rows_iter, end_offset = connector.read_table(
    table_name="wiz_api_events",
    start_offset=None,
    table_options={}
)

rows = list(rows_iter)

print("Total rows:", len(rows))
print("Event types:", set(r["event_type"] for r in rows))
print("Offset:", end_offset)
print("First row:", rows[0])


rows_iter2, end_offset2 = connector.read_table(
    table_name="wiz_api_events",
    start_offset=end_offset,
    table_options={}
)

rows2 = list(rows_iter2)

print("Incremental rows:", len(rows2))
print("New offset:", end_offset2)
# Lakeflow Community Connectors

Lakeflow community connectors are built on top of the [Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) and [Spark Declarative Pipeline (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). These connectors enable users to ingest data from various source systems.

> **Note**: Lakeflow community connectors provide access to additional data sources beyond Databricks managed connectors. They are maintained by community contributors and are not subject to official Databricks SLAs, certifications, or guaranteed compatibility.

## Interface and Testing

Two implementation approaches (see [interface README](src/databricks/labs/community_connector/interface/README.md)):

- **[`LakeflowConnect`](src/databricks/labs/community_connector/interface/lakeflow_connect.py) (recommended)** — Define methods for listing tables, schemas, and reading records. Shared libraries handle Spark PDS, streaming, and offset management automatically.

  > **Note**: All built-in AI-assisted development workflows (commands, skills, agents) apply exclusively to this approach.
- **Direct [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)** — Full control over partitioning, schemas, and read logic; requires manual Spark API contract implementation.

Tests run against live source environments (no mocks):
- **Generic test suite** — End-to-end validation using real credentials
- **Unit tests** — For complex connector-specific logic
- **Write-back testing** *(recommended)* — Write data, read it back, verify incremental reads and deletes

## Develop a New Connector

Build connectors with AI-assisted workflows using [Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Cursor](https://www.cursor.com/). All commands and skills are defined under `.claude/` and auto-discovered by both tools.

```bash
git clone https://github.com/databrickslabs/lakeflow-community-connectors.git
```

### One-Command Agent

A single command orchestrates the entire workflow — API research through deployment:

```
/create-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]
```

The agent pauses once to collect credentials for source authentication.

### Step-by-Step Skills

For more control, run each step individually. Replace `{source}` with your connector name.

| Command | Description |
|---------|-------------|
| `/research-source-api for {source} on [these tables] with [doc url]` | Research the source API |
| `/authenticate-source for {source}` | Set up authentication |
| `/implement-connector for {source}` | Implement the connector |
| `/test-and-fix-connector for {source}` | Test and fix |
| `/create-connector-document for {source}` | Generate documentation |
| `/generate-connector-spec for {source}` | Generate the connector spec |
| `/deploy-connector for {source}` | Build and deploy |

**Optional: Write-Back Testing** — Run between steps 4 and 5. Skip for read-only sources or when writes are expensive/risky.

| Step | Command |
|------|---------|
| Research write APIs | `/research-write-api-of-source for {source}` |
| Implement write-back tests | `/write-back-testing for {source}` |

## Known Limitations

- **Cross-file imports in SDP**: The Spark Declarative Pipeline runtime does not support cross-file module imports for Python Data Source implementations. All connectors must be merged into a single deployable file named `_generated_{source_name}_python_source.py` before deployment. This is handled automatically by the deploy workflow, but can also be run manually:

  ```bash
  python tools/scripts/merge_python_source.py <source_name>
  # or regenerate all at once:
  python tools/scripts/merge_python_source.py all
  ```

- **Custom connector UI**: The Unity Catalog connection creation UI is not automatically generated from the connector spec. When setting up a custom connector through the UI, you must manually enter the required field names alongside their values.

## Deploy and Run

Each connector runs as a configurable SDP pipeline. Define a **pipeline spec** to configure tables and destinations.

- **Databricks UI** — Navigate to the **Add Data** page and scroll to the **Community connectors** section. Select a pre-built source, or select **Custom Connector** to deploy from your own Git repository (provide the source name and repository URL — the system pulls from `main` or `master` by default).
- **CLI tool** — Run `/deploy-connector` in Cursor or Claude Code for guided deployment, or use the CLI directly. See [tools/community_connector](tools/community_connector/README.md).

## Project Structure

```
lakeflow-community-connectors/
|
|___ src/databricks/labs/community_connector/   # Core modules
|       |___ interface/          # The interface each source connector needs to implement 
|       |___ sources/            # Source connectors
|       |       |___ github/     
|       |       |___ zendesk/
|       |       |___ stripe/
|       |       |___ ...         # Each connector: python code, docs, spec and etc. 
|       |___ sparkpds/           # PySpark Data Source implementation and registry
|       |___ libs/               # Shared utilities (spec parsing, data types, module loading)
|       |___ pipeline/           # SDP ingestion orchestration
|
|___ tests/                      # Test suites
|       |___ unit/
|               |___ sources/    # Per-connector tests + generic test harness
|               |___ libs/       # Shared library tests
|               |___ pipeline/   # Pipeline tests
|
|___ tools/                      # Build and deployment tooling
|       |___ community_connector/  # CLI tool for workspace setup and deployment
|       |___ scripts/              # Build scripts (e.g., merge_python_source.py)
|
|___ templates/                  # Templates and guide for AI-assisted development
|
|___ .claude/                    # AI-assisted development (auto-discovered by Claude Code and Cursor)
        |___ skills/             # Skill files for each workflow step
        |___ agents/             # Subagents for different development phases
        |___ commands/           # Slash commands (e.g., /create-connector)
```

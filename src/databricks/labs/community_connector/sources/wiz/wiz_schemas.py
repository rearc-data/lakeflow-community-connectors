"""Schemas, metadata, and constants for the Example connector.

The ``metrics`` table is hidden (not discoverable via the API), so its
Spark schema and metadata must be hard-coded here.  All other table
schemas are fetched at runtime from ``GET /tables/{table}/schema`` and
converted with ``build_spark_type``.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Bronze table schema — same 4 columns for all 4 Wiz tables ────────────────
# id           : the Wiz record ID (issue-123, vuln-456, etc.)
# event_type   : "issue" | "vulnerability_finding" | "audit_log" | "detection"
# raw          : full JSON string of the original API record
# collected_at : when this collector run happened
BRONZE_SCHEMA = StructType([
    StructField("dasl_id", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("_raw_json", StringType(), True),  # or Variant if supported
    StructField("collected_at", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("record_id", StringType(), True),
    StructField("_metadata", StringType(), True),
])

# ── Tables exposed by this connector ─────────────────────────────────────────
TABLES = ["wiz_events"]

# ── GraphQL queries ───────────────────────────────────────────────────────────

ISSUES_QUERY = """
query IssuesTable($filterBy: IssueFilters, $first: Int, $after: String, $orderBy: IssueOrder) {
  issues: issuesV2(filterBy: $filterBy, first: $first, after: $after, orderBy: $orderBy) {
    nodes {
      id
      sourceRule {
        __typename
        ... on Control {
          id name controlDescription: description resolutionRecommendation risks threats
          securitySubCategories { title id category { name id framework { id name } } }
        }
        ... on CloudEventRule {
          id name cloudEventRuleDescription: description sourceType type risks threats
        }
        ... on CloudConfigurationRule {
          id name cloudConfigurationRuleDescription: description remediationInstructions serviceType risks
          securitySubCategories { title id category { name id framework { id name } } }
        }
      }
      createdAt updatedAt dueAt type resolvedAt statusChangedAt status severity
      projects { id name slug businessUnit riskProfile { businessImpact } }
      threatDetectionDetails { eventOrigin }
      entitySnapshot {
        id type nativeType name status cloudPlatform cloudProviderURL providerId region
        resourceGroupExternalId subscriptionExternalId subscriptionName subscriptionTags
        tags createdAt externalId
      }
      serviceTickets { externalId name url }
      notes { createdAt updatedAt text user { name email } serviceAccount { name } }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

VULNS_QUERY = """
query vulnerabilityFindings($first: Int, $after: String, $filterBy: VulnerabilityFindingFilters, $orderBy: VulnerabilityFindingOrder) {
  vulnerabilityFindings(first: $first, after: $after, filterBy: $filterBy, orderBy: $orderBy) {
    nodes {
      id portalUrl name CVEDescription fixDateDescription description CVSSSeverity
      hasExploit score exploitabilityScore impactScore hasCisaKevExploit vendorSeverity
      firstDetectedAt lastDetectedAt resolvedAt remediation locationPath detailedName
      version fixedVersion detectionMethod link status epssSeverity epssPercentile
      epssProbability validatedInRuntime
      layerMetadata { id details isBaseLayer }
      projects { name projectOwners { email } }
      relatedIssueAnalytics {
        criticalSeverityCount highSeverityCount mediumSeverityCount
        lowSeverityCount informationalSeverityCount
      }
      vulnerableAsset {
        ... on VulnerableAssetBase {
          id type name region providerUniqueId cloudProviderURL cloudPlatform status
          subscriptionExternalId subscriptionId subscriptionName tags
          hasLimitedInternetExposure hasWideInternetExposure isAccessibleFromVPN
          isAccessibleFromOtherVnets isAccessibleFromOtherSubscriptions scanSource
        }
        ... on VulnerableAssetVirtualMachine { operatingSystem ipAddresses }
        ... on VulnerableAssetContainerImage  { imageId }
        ... on VulnerableAssetServerless      { runtime }
        ... on VulnerableAssetContainer       { ImageExternalId VmExternalId ServerlessContainer PodNamespace PodName NodeName }
        ... on VulnerableAssetRepositoryBranch { repositoryId repositoryName repositoryExternalId }
      }
    }
    pageInfo { endCursor hasNextPage }
  }
}
"""

AUDIT_LOGS_QUERY = """
query AuditLogTable($first: Int, $after: String, $filterBy: AuditLogEntryFilters) {
  auditLogEntries(first: $first, after: $after, filterBy: $filterBy) {
    nodes {
      id action requestId status timestamp actionParameters userAgent sourceIP
      serviceAccount { id name }
      user { id name }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

DETECTIONS_QUERY = """
query Detections($filterBy: DetectionFilters, $first: Int, $after: String, $orderBy: DetectionOrder, $includeTriggeringEvents: Boolean = true) {
  detections(filterBy: $filterBy, first: $first, after: $after, orderBy: $orderBy, enforceTimestampContinuity: true) {
    nodes {
      id
      issue { id url }
      ruleMatch { rule { id name sourceType securitySubCategories { title category { name framework { name } } } } }
      description severity createdAt startedAt endedAt
      cloudAccounts { cloudProvider externalId name linkedProjects { id name } }
      cloudOrganizations { cloudProvider externalId name }
      actors { id externalId name type nativeType actingAs { id externalId name type nativeType } }
      primaryActor { id }
      resources {
        id externalId name type nativeType region
        cloudAccount { cloudProvider externalId name }
        kubernetesNamespace { id providerUniqueId name }
        kubernetesCluster { id providerUniqueId name }
      }
      primaryResource { id }
      triggeringEvents(first: 10) @include(if: $includeTriggeringEvents) {
        nodes {
          ... on CloudEvent {
            id origin name description cloudProviderUrl cloudPlatform timestamp source category status
            actor { id actingAs { id } }
            actorIP
            actorIPMeta {
              country autonomousSystemNumber autonomousSystemOrganization reputation
              reputationDescription reputationSource relatedAttackGroupNames
              customIPRanges { id name isInternal ipRanges }
            }
            resources { id }
            extraDetails {
              ... on CloudEventRuntimeDetails {
                processTree { command path hash size executionTime runtimeProgramId userId userName
                  container { id externalId name image { id externalId } }
                }
              }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

# ── EVENT_CONFIGS ─────────────────────────────────────────────────────────────
# Copied verbatim from WIZ_Collector notebook Step 4. Zero changes.
# source.py reads from this dict — no hardcoded constants in collector logic.

EVENT_CONFIGS: dict = {

    "issue": {
        "query":            ISSUES_QUERY,
        "connection_field": "issues",
        "page_size":        500,
        "filter_variables": {
            "severity": ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFORMATIONAL"],
            "project":  None,
        },
        "order_by": {"field": "CREATED_AT", "direction": "ASC"},
        "incremental": {
            "overlap_hours": 1,
        },
    },

    "vulnerability_finding": {
        "query":            VULNS_QUERY,
        "connection_field": "vulnerabilityFindings",
        "page_size":        500,
        "filter_variables": {
            "status":               ["OPEN", "IN_PROGRESS", "REJECTED", "RESOLVED"],
            "assetType":            ["SERVERLESS", "CONTAINER_IMAGE", "VIRTUAL_MACHINE",
                                     "CONTAINER", "REPOSITORY_BRANCH", "ENDPOINT"],
            "vendorSeverity":       ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFORMATIONAL"],
            "projectId":            None,
            "relatedIssueSeverity": None,
        },
        "incremental": {
            "overlap_hours": 1,
        },
    },

    "audit_log": {
        "query":            AUDIT_LOGS_QUERY,
        "connection_field": "auditLogEntries",
        "page_size":        500,
        "filter_variables": {},
        "incremental": {
            "historical_days": 10,
        },
    },

    "detection": {
        "query":            DETECTIONS_QUERY,
        "connection_field": "detections",
        "page_size":        50,
        "filter_variables": {
            "severity":  {"equals": ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFORMATIONAL"]},
            "projectId": None,
        },
        "order_by":                  {"field": "CREATED_AT", "direction": "ASC"},
        "include_triggering_events": True,
        "incremental": {
            "historical_days": 3,
        },
    },
}


def get_schema(table_name: str) -> StructType:
    """
    Minimal schema for Wiz connector.
    Matches what we emit in fetch functions.
    """

    return BRONZE_SCHEMA
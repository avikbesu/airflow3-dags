"""
Shared Asset and AssetAlias definitions for dags/assets/.

Import from this module in both producer and consumer DAGs so that the URI
(and therefore the identity Airflow uses to match producers to consumers)
stays in one place.

Asset URI conventions used here:
  s3://my-data-lake/<tier>/<name>.parquet

Tiers follow the medallion architecture:
  bronze → raw ingestion / landed data
  silver → cleaned, deduplicated, validated
  gold   → aggregated, report-ready
"""

from __future__ import annotations

from airflow.sdk import Asset, AssetAlias

# ---------------------------------------------------------------------------
# Bronze tier — raw landing zones
# ---------------------------------------------------------------------------
asset_orders_raw = Asset(
    uri="s3://my-data-lake/raw/orders.parquet",
    name="orders_raw",
    extra={"tier": "bronze", "owner": "data-eng", "format": "parquet"},
)

asset_customers_raw = Asset(
    uri="s3://my-data-lake/raw/customers.parquet",
    name="customers_raw",
    extra={"tier": "bronze", "owner": "data-eng", "format": "parquet"},
)

# ---------------------------------------------------------------------------
# Silver tier — cleaned / validated
# ---------------------------------------------------------------------------
asset_orders_clean = Asset(
    uri="s3://my-data-lake/clean/orders.parquet",
    name="orders_clean",
    extra={"tier": "silver", "owner": "data-eng", "format": "parquet"},
)

# ---------------------------------------------------------------------------
# Gold tier — report-ready aggregates
# ---------------------------------------------------------------------------
asset_daily_report = Asset(
    uri="s3://my-data-lake/reports/daily_summary.parquet",
    name="daily_report",
    extra={"tier": "gold", "owner": "analytics", "format": "parquet"},
)

# ---------------------------------------------------------------------------
# AssetAlias — producer resolves the real Asset at runtime;
# consumer DAG schedules on the alias name without knowing the concrete URI.
# Used in a3_asset_alias.py.
# ---------------------------------------------------------------------------
alias_bronze_ingestion = AssetAlias(name="bronze-ingestion")

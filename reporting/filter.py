#!/usr/bin/env python3
"""
Exclusion of third-party AWS Marketplace charges, shared by the report and
the dashboard so both see the same rows.

AWS CURs bill third-party AWS Marketplace subscriptions (MongoDB Atlas,
Elastic Cloud, Grafana Cloud, ...) alongside AWS's own services. Those
subscriptions run on the seller's infrastructure, not on AWS-operated
hardware, so SPRUCE has nothing to model them from and AWS's own Carbon
Footprint Tool does not report on them either. Left in, they only depress
the coverage percentage with charges no cloud environmental model can
account for, so they are filtered out before any query runs.

This module is pure Python (no third-party dependencies) so it can be unit
tested without DuckDB or the Streamlit stack.
"""

from __future__ import annotations

# AWS-native column, absent from FOCUS and Azure outputs. Callers add it as a
# NULL VARCHAR when the input does not carry it.
BILLING_ENTITY_COLUMN = "bill_billing_entity"

# The column holds 'AWS' or 'AWS Marketplace' on AWS CURs and is absent
# everywhere else, so NULL is kept and inputs without it are unaffected.
MARKETPLACE_FILTER = f"{BILLING_ENTITY_COLUMN} IS NULL OR {BILLING_ENTITY_COLUMN} = 'AWS'"


def create_cur_views(con, select_sql: str) -> None:
    """Create the unfiltered ``cur_all`` view from ``select_sql`` and the
    ``cur`` view every query runs on, which leaves out marketplace rows."""
    con.execute(f"CREATE VIEW cur_all AS {select_sql}")
    con.execute(f"CREATE VIEW cur AS SELECT * FROM cur_all WHERE {MARKETPLACE_FILTER}")


def excluded_rows(con) -> tuple[int, float]:
    """Return the (line items, BilledCost) left out of ``cur``, both 0 on
    inputs that carry no marketplace rows."""
    rows, cost = con.execute(f"""
        SELECT count(*), coalesce(sum(BilledCost), 0)
        FROM cur_all
        WHERE NOT ({MARKETPLACE_FILTER})
    """).fetchone()
    return int(rows), float(cost)


def coverage_note(rows: int, cost: float) -> str:
    """Markdown sentence stating what the figures cover on AWS inputs."""
    note = (
        "**Coverage: AWS services only.** Third-party AWS Marketplace subscriptions "
        "are excluded. They are billed through AWS but run on the seller's own "
        "infrastructure, so their energy and emissions cannot be derived from "
        "this billing data, and AWS's Carbon Footprint Tool does not report on "
        "them either."
    )
    if rows:
        note += f" {rows:,} line items (${cost:,.2f}) were excluded on that basis."
    return note

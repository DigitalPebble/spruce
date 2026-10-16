#!/usr/bin/env python3
"""
Version of SPRUCE which enriched the data, shared by the report and the
dashboard so both credit it the same way.

SparkJob stamps every output row with a ``spruce_version`` column. Outputs
written before that column existed do not carry it, and a dataset can mix
several versions since each billing period may have been enriched by a
different run.

This module is pure Python (no third-party dependencies) so it can be unit
tested without DuckDB or the Streamlit stack.
"""

from __future__ import annotations

VERSION_COLUMN = "spruce_version"


def spruce_versions(con) -> list[str]:
    """Return the distinct SPRUCE versions found in ``cur``, sorted, or an
    empty list if the data predates the ``spruce_version`` column."""
    columns = {str(row[0]).lower() for row in con.execute("DESCRIBE cur").fetchall()}
    if VERSION_COLUMN not in columns:
        return []
    rows = con.execute(f"""
        SELECT DISTINCT {VERSION_COLUMN}
        FROM cur
        WHERE {VERSION_COLUMN} IS NOT NULL
        ORDER BY 1
    """).fetchall()
    return [str(row[0]) for row in rows]


def version_label(versions: list[str]) -> str:
    """Versions as shown after 'SPRUCE', e.g. 'v1.4' or 'v1.3, v1.4';
    empty when unknown."""
    return ", ".join(f"v{v}" for v in versions)


def mixed_versions_note(versions: list[str]) -> str:
    """Warning to show when the data was enriched by more than one version,
    empty otherwise."""
    if len(versions) < 2:
        return ""
    return (
        f"The data was enriched with several versions of SPRUCE "
        f"({version_label(versions)}); figures from different billing periods "
        f"may not be directly comparable."
    )

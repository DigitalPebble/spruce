#!/usr/bin/env python3
"""
Usage by day of week and hour of day, for the static report.

Averages the operational energy of each (weekday, hour) slot over the period
covered by the data, in the timezone of the people using the resources. A
grid that is dark during office hours and pale at night and at weekends shows
test and development environments being switched off when nobody uses them;
a uniform grid shows resources running around the clock.

Only line items spanning exactly one hour can be placed on the grid, so the
input has to come from an hourly export (AWS CUR or FOCUS with hourly
granularity). Line items covering a day or a month, and exports with a daily
granularity, are left out and their share of the energy is reported.

The rendering functions are pure Python so they can be unit tested without
DuckDB.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

# Office hours as [start, end) in local time, Monday to Friday.
OFFICE_START = 8
OFFICE_END = 18

# Number of colour levels in the HTML heatmap, excluding the empty level 0.
LEVELS = 5

# Background colours for levels 1..LEVELS, from the SPRUCE accent (#BC6554)
# blended with white.
LEVEL_COLOURS = ("#f5e6e3", "#e8c4bc", "#d9a295", "#cb836f", "#bc6554")


@dataclass
class HourlyProfile:
    """Energy summed per (weekday, hour) slot, with the number of times each
    slot occurs in the period. Index [d][h] with d=0 for Monday."""
    sums: list[list[float]]
    counts: list[list[int]]
    hourly_kwh: float
    total_kwh: float
    first_hour: object = None
    last_hour: object = None

    def average(self, day: int, hour: int) -> float | None:
        count = self.counts[day][hour]
        if not count:
            return None
        return self.sums[day][hour] / count

    def mean_over(self, slots) -> float | None:
        """Average energy per hour over a set of (day, hour) slots."""
        total = sum(self.sums[d][h] for d, h in slots)
        count = sum(self.counts[d][h] for d, h in slots)
        return total / count if count else None


def is_office_hour(day: int, hour: int) -> bool:
    return day < 5 and OFFICE_START <= hour < OFFICE_END


OFFICE_SLOTS = [(d, h) for d in range(5) for h in range(OFFICE_START, OFFICE_END)]
WEEKDAY_NIGHT_SLOTS = [(d, h) for d in range(5) for h in range(24)
                       if not OFFICE_START <= h < OFFICE_END]
WEEKEND_SLOTS = [(d, h) for d in (5, 6) for h in range(24)]


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def query(con, timezone: str) -> HourlyProfile:
    """Build the profile from the ``cur`` view. ChargePeriodStart and
    ChargePeriodEnd are emitted as ISO 8601 strings by the AWS pipelines and
    as timestamps by some exports, so both are read through VARCHAR, with
    timestamps lacking an offset taken as UTC."""
    con.execute("SET TimeZone = 'UTC'")
    tz = timezone.replace("'", "''")
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW heatmap_rows AS
        WITH spans AS (
            SELECT
                try_cast(CAST(ChargePeriodStart AS VARCHAR) AS TIMESTAMPTZ) AS s,
                try_cast(CAST(ChargePeriodEnd AS VARCHAR) AS TIMESTAMPTZ)   AS e,
                operational_energy_kwh                                       AS kwh
            FROM cur
            WHERE operational_energy_kwh IS NOT NULL
        )
        SELECT s AS utc_start, timezone('{tz}', s) AS local_start, kwh,
               coalesce(e - s = INTERVAL 1 HOUR, false) AS hourly
        FROM spans
    """)

    total_kwh, hourly_kwh, first_hour, last_hour = con.execute("""
        SELECT coalesce(sum(kwh), 0),
               coalesce(sum(kwh) FILTER (WHERE hourly), 0),
               min(local_start) FILTER (WHERE hourly),
               max(local_start) FILTER (WHERE hourly)
        FROM heatmap_rows
    """).fetchone()

    sums = [[0.0] * 24 for _ in DAYS]
    counts = [[0] * 24 for _ in DAYS]
    profile = HourlyProfile(sums, counts, float(hourly_kwh), float(total_kwh),
                            first_hour, last_hour)
    if first_hour is None:
        return profile

    for dow, hour, kwh in con.execute("""
        SELECT isodow(local_start), hour(local_start), sum(kwh)
        FROM heatmap_rows
        WHERE hourly
        GROUP BY 1, 2
    """).fetchall():
        sums[dow - 1][hour] = float(kwh)

    # Number of times each slot occurs between the first and last hour of
    # data, so that a month with five Mondays does not inflate Monday. The
    # hours are generated in UTC and then converted, so that the local hour
    # repeated when clocks go back is counted twice and the one skipped when
    # they go forward is not counted.
    for dow, hour, n in con.execute(f"""
        SELECT isodow(t), hour(t), count(*)
        FROM (
            SELECT timezone('{tz}', unnest(generate_series(
                date_trunc('hour', min(utc_start)),
                date_trunc('hour', max(utc_start)),
                INTERVAL 1 HOUR))) AS t
            FROM heatmap_rows
            WHERE hourly
        )
        GROUP BY 1, 2
    """).fetchall():
        counts[dow - 1][hour] = int(n)

    return profile


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _scale(profile: HourlyProfile):
    """Pick a unit and number of decimals suited to the largest average, so
    every cell of the grid uses the same format."""
    peak = max((profile.average(d, h) or 0.0
                for d in range(7) for h in range(24)), default=0.0)
    factor, unit = (1.0, "kWh") if peak >= 1 else (1000.0, "Wh")
    scaled = peak * factor
    decimals = 0 if scaled >= 100 else 1 if scaled >= 10 else 2
    return factor, unit, decimals, peak


def _format(value, factor, decimals):
    if value is None:
        return ""
    return f"{value * factor:,.{decimals}f}"


def level(value: float | None, low: float, peak: float) -> int:
    """Colour level 0..LEVELS of a cell, spread between the quietest and the
    busiest slot. Starting the scale at the quietest slot rather than at zero
    keeps resources running around the clock from tinting every cell and
    hiding the daily pattern. Slots without energy stay blank."""
    if not value or value <= 0 or not peak:
        return 0
    if peak <= low:
        return LEVELS
    position = (value - low) / (peak - low)
    return max(1, min(LEVELS, 1 + math.floor(position * LEVELS)))


def markdown_grid(profile: HourlyProfile) -> str:
    """The grid as a GFM table, days as rows and hours as columns."""
    factor, unit, decimals, _ = _scale(profile)
    header = [unit] + [f"{h:02d}" for h in range(24)]
    lines = ["| " + " | ".join(header) + " |",
             "|" + "|".join(["---"] + ["--:"] * 24) + "|"]
    for d, day in enumerate(DAYS):
        cells = [_format(profile.average(d, h), factor, decimals) for h in range(24)]
        lines.append("| " + " | ".join([f"**{day}**"] + cells) + " |")
    return "\n".join(lines) + "\n"


def html_grid(profile: HourlyProfile) -> str:
    """The grid as an HTML table coloured by level, with office hours set
    apart from nights and weekends."""
    factor, unit, decimals, peak = _scale(profile)
    low = min((v for d in range(7) for h in range(24)
               if (v := profile.average(d, h)) is not None and v > 0), default=0.0)
    out = ['<table class="heatmap">',
           "<thead><tr><th>" + unit + "</th>"
           + "".join(f"<th>{h:02d}</th>" for h in range(24))
           + "</tr></thead><tbody>"]
    for d, day in enumerate(DAYS):
        row = [f"<tr><th>{day}</th>"]
        for h in range(24):
            value = profile.average(d, h)
            classes = [f"l{level(value, low, peak)}"]
            if not is_office_hour(d, h):
                classes.append("off")
            row.append(f'<td class="{" ".join(classes)}">'
                       f"{_format(value, factor, decimals)}</td>")
        out.append("".join(row) + "</tr>")
    out.append("</tbody></table>")
    legend = "".join(
        f'<span class="swatch" style="background:{c}"></span>' for c in LEVEL_COLOURS
    )
    out.append(f'<p class="heatmap-legend">Less {legend} More &middot; '
               f"cells with a thin border are outside office hours</p>")
    return "\n".join(out)


def html_style() -> str:
    """CSS rules for the HTML/PDF heatmap."""
    rules = [
        ".heatmap { width: 100%; table-layout: fixed; font-size: 8px; }",
        ".heatmap th, .heatmap td { padding: 3px 2px; text-align: center; "
        "min-width: 22px; border: 2px solid #fff; }",
        ".heatmap thead th, .heatmap tbody th { background: none; color: #555; }",
        ".heatmap td.off { border: 1px solid #ccc; }",
        ".heatmap td.l0 { background: #fff; }",
        ".heatmap-legend { font-size: 9px; color: #555; }",
        ".heatmap-legend .swatch { display: inline-block; width: 10px; height: 10px; "
        "margin: 0 1px; vertical-align: middle; }",
    ]
    for i, colour in enumerate(LEVEL_COLOURS, 1):
        text = "#fff" if i >= 4 else "#111"
        rules.append(f".heatmap td.l{i} {{ background: {colour}; color: {text}; }}")
    return "\n  ".join(rules)


def summary(profile: HourlyProfile, timezone: str) -> str:
    """Markdown introduction and comparison of office hours, weekday nights
    and weekends, placed above the grid."""
    office = profile.mean_over(OFFICE_SLOTS)
    nights = profile.mean_over(WEEKDAY_NIGHT_SLOTS)
    weekends = profile.mean_over(WEEKEND_SLOTS)
    factor, unit, decimals, _ = _scale(profile)

    def ratio(value):
        if value is None or not office:
            return "n/a"
        return f"{value * 100 / office:.0f} %"

    rows = [
        (f"Office hours (Mon-Fri {OFFICE_START:02d}:00-{OFFICE_END:02d}:00)",
         _format(office, factor, decimals), "100 %" if office else "n/a"),
        ("Weekday nights", _format(nights, factor, decimals), ratio(nights)),
        ("Weekends", _format(weekends, factor, decimals), ratio(weekends)),
    ]
    first = profile.first_hour.strftime("%Y-%m-%d")
    last = profile.last_hour.strftime("%Y-%m-%d")
    excluded = profile.total_kwh - profile.hourly_kwh
    lines = [
        f"Average operational energy per hour, {timezone} time, "
        f"from {first} to {last}. ",
    ]
    if excluded > 0 and profile.total_kwh:
        share = excluded * 100 / profile.total_kwh
        lines.append(
            f"Line items covering more than an hour (daily or monthly charges) "
            f"cannot be placed on the grid and are left out: "
            f"{share:.1f} % of the period's energy."
        )
    body = "".join(lines) + "\n\n"
    header = ["period", f"avg_{unit.lower()}_per_hour", "vs_office_hours"]
    widths = [max(len(header[i]), *(len(str(r[i])) for r in rows)) for i in range(3)]
    table = ["| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(header)) + " |",
             "| " + " | ".join("-" * w for w in widths) + " |"]
    for r in rows:
        table.append("| " + " | ".join(str(r[i]).ljust(widths[i]) for i in range(3)) + " |")
    return body + "\n".join(table) + "\n"


def no_data_note(profile: HourlyProfile) -> str:
    if not profile.total_kwh:
        return "_No operational energy in the data._\n"
    return (
        "_No line items spanning a single hour were found. This section needs "
        "a cost report exported with hourly granularity; daily and monthly "
        "exports cannot be split by hour of the day._\n"
    )

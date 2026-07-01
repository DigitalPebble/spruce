#!/usr/bin/env python3
"""
Human-readable impact equivalences for the SPRUCE dashboard overview.

The overview shows raw totals (kg CO2e, kWh, litres). These helpers translate
those totals into everyday comparisons so the numbers are easier to grasp.

Conversion factors are documented, order-of-magnitude figures meant to build
intuition, not precise accounting. Each factor cites a source; adjust the
constants here to change every equivalence at once.

This module is pure Python (no third-party dependencies) so it can be unit
tested without the Streamlit stack.
"""

from __future__ import annotations

from dataclasses import dataclass

# --- Emissions (kg CO2e) ---------------------------------------------------
# Average family car, UK DEFRA/BEIS 2023 GHG conversion factors (~0.17 kg/km).
CO2_KG_PER_CAR_KM = 0.170
# One-way economy long-haul flight London -> New York (~0.5 t CO2e; atmosfair/ICAO).
FLIGHT_ROUTE = "London → New York, one-way economy"
CO2_KG_PER_FLIGHT = 500.0

# --- Energy (kWh) ----------------------------------------------------------
# Average EU household electricity use per year (~3,500 kWh; Eurostat/Odyssee).
KWH_PER_HOUSEHOLD_YEAR = 3500.0
# Boiling one ~0.25 L mug of water in an electric kettle (~0.03 kWh).
KWH_PER_CUP_OF_TEA = 0.03

# --- Water (litres) --------------------------------------------------------
# Olympic-size swimming pool volume: 2,500 m3.
L_PER_OLYMPIC_POOL = 2_500_000.0
# Typical filled domestic bathtub (~150 L).
L_PER_BATHTUB = 150.0


@dataclass(frozen=True)
class Equivalence:
    """A single everyday comparison, e.g. 1,234 "km driven by an average car"."""

    quantity: float
    unit: str
    note: str = ""


@dataclass(frozen=True)
class EquivalenceGroup:
    """Equivalences derived from one overview metric."""

    metric: str
    items: tuple[Equivalence, ...]


def _non_negative(value: object) -> float:
    """Coerce a possibly-missing numeric value to a non-negative float."""
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    if number != number or number < 0:  # NaN or negative
        return 0.0
    return number


def emission_equivalences(co2_kg: object) -> EquivalenceGroup:
    kg = _non_negative(co2_kg)
    return EquivalenceGroup(
        "Emissions",
        (
            Equivalence(kg / CO2_KG_PER_CAR_KM, "km in a family car"),
            Equivalence(kg / CO2_KG_PER_FLIGHT, "flights", FLIGHT_ROUTE),
        ),
    )


def energy_equivalences(kwh: object) -> EquivalenceGroup:
    energy = _non_negative(kwh)
    return EquivalenceGroup(
        "Energy",
        (
            Equivalence(
                energy / KWH_PER_HOUSEHOLD_YEAR, "homes powered for a year"
            ),
            Equivalence(energy / KWH_PER_CUP_OF_TEA, "cups of tea boiled"),
        ),
    )


def water_equivalences(litres: object) -> EquivalenceGroup:
    water = _non_negative(litres)
    return EquivalenceGroup(
        "Water",
        (
            Equivalence(water / L_PER_OLYMPIC_POOL, "Olympic swimming pools"),
            Equivalence(water / L_PER_BATHTUB, "bathtubs"),
        ),
    )


def overview_equivalences(
    co2_kg: object, energy_kwh: object, water_l: object
) -> list[EquivalenceGroup]:
    """Build every equivalence group shown under the dashboard overview."""
    return [
        emission_equivalences(co2_kg),
        energy_equivalences(energy_kwh),
        water_equivalences(water_l),
    ]


def format_quantity(quantity: float) -> str:
    """Format an equivalence quantity with thousands separators and adaptive precision."""
    number = _non_negative(quantity)
    if number >= 10:
        return f"{number:,.0f}"
    if number >= 1:
        return f"{number:,.1f}"
    return f"{number:,.2f}"

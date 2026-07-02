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
# One hour of video streaming (~36 g CO2e; IEA 2020 global average).
CO2_KG_PER_STREAMING_HOUR = 0.036
# CO2 absorbed by one mature tree in a year (~21 kg; EPA/Arbor Day figures).
CO2_KG_PER_TREE_YEAR = 21.0

# --- Energy (kWh) ----------------------------------------------------------
# Average EU household electricity use per year (~3,500 kWh; Eurostat/Odyssee).
KWH_PER_HOUSEHOLD_YEAR = 3500.0
# Boiling one ~0.25 L mug of water in an electric kettle (~0.03 kWh).
KWH_PER_CUP_OF_TEA = 0.03
# Fully charging one smartphone (~0.012 kWh; EPA GHG equivalences).
KWH_PER_PHONE_CHARGE = 0.012
# Daily output of one ~400 W rooftop solar panel (~1.5 kWh/day).
KWH_PER_SOLAR_PANEL_DAY = 1.5

# --- Water (litres) --------------------------------------------------------
# Olympic-size swimming pool volume: 2,500 m3.
L_PER_OLYMPIC_POOL = 2_500_000.0
# Typical filled domestic bathtub (~150 L).
L_PER_BATHTUB = 150.0
# One washing machine cycle on a modern front loader (~50 L).
L_PER_WASHING_CYCLE = 50.0
# A year of rain on one square metre of land (~750 L; ~715 mm global average).
L_PER_SQM_RAIN_YEAR = 750.0

# Selectable comparison styles for the second slot of each metric. The first
# slot (car km, homes, pools) stays fixed so totals remain comparable.
FLAVORS = ("Everyday", "Tech", "Nature")


@dataclass(frozen=True)
class Equivalence:
    """A single everyday comparison, e.g. 1,234 "km in a family car"."""

    quantity: float
    unit: str
    note: str = ""
    icon: str = ""


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


def _second_emission(kg: float, flavor: str) -> Equivalence:
    if flavor == "Tech":
        return Equivalence(
            kg / CO2_KG_PER_STREAMING_HOUR, "hours of video streaming", icon="📺"
        )
    if flavor == "Nature":
        return Equivalence(
            kg / CO2_KG_PER_TREE_YEAR, "tree-years to absorb it", icon="🌳"
        )
    return Equivalence(kg / CO2_KG_PER_FLIGHT, "flights", FLIGHT_ROUTE, icon="✈️")


def _second_energy(energy: float, flavor: str) -> Equivalence:
    if flavor == "Tech":
        return Equivalence(
            energy / KWH_PER_PHONE_CHARGE, "smartphone charges", icon="📱"
        )
    if flavor == "Nature":
        return Equivalence(
            energy / KWH_PER_SOLAR_PANEL_DAY, "solar-panel days", icon="☀️"
        )
    return Equivalence(energy / KWH_PER_CUP_OF_TEA, "cups of tea boiled", icon="☕")


def _second_water(water: float, flavor: str) -> Equivalence:
    if flavor == "Tech":
        return Equivalence(
            water / L_PER_WASHING_CYCLE, "washing machine cycles", icon="🧺"
        )
    if flavor == "Nature":
        return Equivalence(
            water / L_PER_SQM_RAIN_YEAR, "m²-years of rainfall", icon="🌧️"
        )
    return Equivalence(water / L_PER_BATHTUB, "bathtubs filled", icon="🛁")


def emission_equivalences(co2_kg: object, flavor: str = "Everyday") -> EquivalenceGroup:
    kg = _non_negative(co2_kg)
    return EquivalenceGroup(
        "Emissions",
        (
            Equivalence(kg / CO2_KG_PER_CAR_KM, "km in a family car", icon="🚗"),
            _second_emission(kg, flavor),
        ),
    )


def energy_equivalences(kwh: object, flavor: str = "Everyday") -> EquivalenceGroup:
    energy = _non_negative(kwh)
    return EquivalenceGroup(
        "Energy",
        (
            Equivalence(
                energy / KWH_PER_HOUSEHOLD_YEAR, "homes powered for a year", icon="🏠"
            ),
            _second_energy(energy, flavor),
        ),
    )


def water_equivalences(litres: object, flavor: str = "Everyday") -> EquivalenceGroup:
    water = _non_negative(litres)
    return EquivalenceGroup(
        "Water",
        (
            Equivalence(water / L_PER_OLYMPIC_POOL, "Olympic swimming pools", icon="🏊"),
            _second_water(water, flavor),
        ),
    )


def overview_equivalences(
    co2_kg: object, energy_kwh: object, water_l: object, flavor: str = "Everyday"
) -> list[EquivalenceGroup]:
    """Build every equivalence group shown under the dashboard overview."""
    return [
        emission_equivalences(co2_kg, flavor),
        energy_equivalences(energy_kwh, flavor),
        water_equivalences(water_l, flavor),
    ]


def format_quantity(quantity: float) -> str:
    """Format an equivalence quantity with thousands separators and adaptive precision."""
    number = _non_negative(quantity)
    if number >= 10:
        return f"{number:,.0f}"
    text = f"{number:,.1f}" if number >= 1 else f"{number:,.2f}"
    return text.rstrip("0").rstrip(".") if "." in text else text

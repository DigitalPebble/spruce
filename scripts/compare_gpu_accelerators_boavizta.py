#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Compare GPU impacts between the CCF-derived Accelerators resources and BoaviztAPI.

The Accelerators module (src/main/resources/ccf/accelerators.json) estimates the
use-phase electricity of GPUs on EC2 instances from CCF min/max wattage data.

This script measures, for every EC2 instance type with a GPU:

- coverage: which instance types each source knows about;
- agreement: GPU count and GPU model per instance type;
- use phase: hourly GPU electricity per the Accelerators formula vs whether
  BoaviztAPI implements GPU use-phase power at all;
- embodied: total embodied gwp from the live API vs the values bundled in
  src/main/resources/boavizta/instanceTypes.csv (spots a stale bundle).

By default the public BoaviztAPI instance is used. You can also use a local instance:

    docker run --rm -p 5000:5000 ghcr.io/boavizta/boaviztapi:2.3.0

Usage: compare_gpu_accelerators_boavizta.py [--api http://localhost:5000]
Run from the project root. Only the Python standard library is needed.
"""

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request

ACCELERATORS_JSON = "src/main/resources/ccf/accelerators.json"
INSTANCE_TYPES_CSV = "src/main/resources/boavizta/instanceTypes.csv"
GPU_UTILISATION_PERCENT = 50  # default of the Accelerators module
MJ_TO_KWH = 0.277778  # same factor as BoaviztAPIClient


def get_json(base, path, **params):
    url = f"{base}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url) as resp:
        return json.load(resp)


def fetch_boavizta_gpu_instances(api):
    """Return {instance_type: info} for every AWS instance with gpu_units > 0."""
    instances = get_json(api, "/v1/cloud/instance/all_instances", provider="aws")
    platforms = {}
    gpu_instances = {}
    for name in instances:
        config = get_json(
            api,
            "/v1/cloud/instance/instance_config",
            provider="aws",
            instance_type=name,
        )
        gpu_units = (config.get("gpu_units") or {}).get("default") or 0
        if gpu_units <= 0:
            continue
        platform = (config.get("platform") or {}).get("default")
        if platform and platform not in platforms:
            server = get_json(api, "/v1/server/archetype_config", archetype=platform)
            gpu = server.get("GPU") or {}
            platforms[platform] = {
                "gpu_name": (gpu.get("name") or {}).get("default"),
                "gpu_units": (gpu.get("units") or {}).get("default"),
            }
        impacts = get_json(
            api,
            "/v1/cloud/instance",
            provider="aws",
            instance_type=name,
            verbose="true",
            duration=1,
            criteria="gwp",
        )
        gwp = impacts["impacts"]["gwp"]
        gpu_component = (impacts.get("verbose") or {}).get("GPU-1") or {}
        gpu_gwp = (gpu_component.get("impacts") or {}).get("gwp") or {}
        gpu_instances[name] = {
            "gpu_units": gpu_units,
            "platform": platform,
            "gpu_name": platforms.get(platform, {}).get("gpu_name"),
            "embedded_gwp_g": gwp["embedded"]["value"] * 1000,
            "gpu_embedded_gwp_g": (gpu_gwp.get("embedded") or {}).get("value", 0)
            * 1000,
            "gpu_use": gpu_gwp.get("use"),
        }
    return gpu_instances


def load_accelerators():
    with open(ACCELERATORS_JSON) as f:
        data = json.load(f)
    return data["GPU_INSTANCES_TYPES"], data["GPU_INFO"]


def load_bundled_embodied():
    """instance_type -> embodied gwp (g) from the bundled static CSV."""
    embodied = {}
    with open(INSTANCE_TYPES_CSV) as f:
        for row in csv.reader(f):
            if not row or row[0].startswith("#"):
                continue
            embodied[row[0].strip()] = float(row[2])
    return embodied


def accelerators_hourly_kwh(instance, gpu_types, gpu_info):
    """Hourly GPU electricity (kWh) as computed by the Accelerators module."""
    info = gpu_types[instance]
    watts = gpu_info[info["type"]]
    power = watts["min"] + GPU_UTILISATION_PERCENT / 100 * (watts["max"] - watts["min"])
    return power * info["quantity"] / 1000


def normalise_gpu_name(name):
    """'NVIDIA_TESLA_V100' / 'Tesla V100' -> 'v100' (vendor prefixes dropped)."""
    key = "".join(c for c in (name or "").lower() if c.isalnum())
    for vendor in ("nvidia", "tesla", "amd", "radeonpro"):
        key = key.replace(vendor, "")
    return key


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--api",
        default="http://localhost:5000",
        help="base URL of a running BoaviztAPI (default %(default)s)",
    )
    args = parser.parse_args()

    try:
        get_json(args.api, "/v1/cloud/instance/all_instances", provider="aws")
    except OSError as e:
        sys.exit(
            f"Cannot reach BoaviztAPI at {args.api}: {e}\n"
            "Start one with: docker run --rm -p 5000:5000 ghcr.io/boavizta/boaviztapi:2.3.0"
        )

    gpu_types, gpu_info = load_accelerators()
    bundled = load_bundled_embodied()
    print(f"Querying {args.api} for AWS GPU instances...", file=sys.stderr)
    boavizta = fetch_boavizta_gpu_instances(args.api)

    acc_set, boa_set = set(gpu_types), set(boavizta)

    print("# GPU coverage")
    print(f"Accelerators (ccf/accelerators.json): {len(acc_set)} instance types")
    print(f"BoaviztAPI:                           {len(boa_set)} instance types")
    only_boa = sorted(boa_set - acc_set)
    only_acc = sorted(acc_set - boa_set)
    print(f"\nIn BoaviztAPI but not in Accelerators ({len(only_boa)}):")
    for name in only_boa:
        info = boavizta[name]
        print(f"  {name}: {info['gpu_units']:g} x {info['gpu_name']}")
    print(f"\nIn Accelerators but not in BoaviztAPI ({len(only_acc)}):")
    for name in only_acc:
        info = gpu_types[name]
        print(f"  {name}: {info['quantity']} x {info['type']}")

    print("\n# Agreement on instance types known to both")
    mismatches = 0
    for name in sorted(acc_set & boa_set):
        acc, boa = gpu_types[name], boavizta[name]
        qty_ok = acc["quantity"] == boa["gpu_units"]
        # CCF names are like NVIDIA_TESLA_V100, Boavizta ones like Tesla V100
        model_ok = normalise_gpu_name(acc["type"]) == normalise_gpu_name(
            boa["gpu_name"]
        )
        if not (qty_ok and model_ok):
            mismatches += 1
            print(
                f"  {name}: Accelerators {acc['quantity']} x {acc['type']} "
                f"vs BoaviztAPI {boa['gpu_units']:g} x {boa['gpu_name']}"
            )
    if mismatches == 0:
        print("  GPU count and model agree for all shared instance types")

    print("\n# Use phase")
    not_implemented = sum(
        1 for i in boavizta.values() if i["gpu_use"] == "not implemented"
    )
    print(
        f"BoaviztAPI GPU use-phase 'not implemented': {not_implemented}/{len(boavizta)} instances"
    )
    print(
        "Hourly GPU electricity per the Accelerators formula "
        f"(utilisation {GPU_UTILISATION_PERCENT}%):"
    )
    for name in sorted(acc_set & boa_set):
        kwh = accelerators_hourly_kwh(name, gpu_types, gpu_info)
        print(f"  {name}: {kwh:.3f} kWh/h (BoaviztAPI: none)")

    print("\n# Embodied gwp: live API vs bundled instanceTypes.csv (g CO2eq/h)")
    print(
        f"{'instance type':<18} {'bundled':>10} {'API':>10} {'ratio':>6}  GPU share of API total"
    )
    for name in sorted(boa_set):
        boa = boavizta[name]
        old = bundled.get(name)
        ratio = f"{boa['embedded_gwp_g'] / old:.2f}" if old else "n/a"
        share = boa["gpu_embedded_gwp_g"] / boa["embedded_gwp_g"] * 100
        print(
            f"{name:<18} {old if old is not None else 'missing':>10} "
            f"{boa['embedded_gwp_g']:>10.3f} {ratio:>6}  {share:.0f}%"
        )


if __name__ == "__main__":
    main()

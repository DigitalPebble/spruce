#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check the Accelerators GPU data against the instance types AWS actually offers.

The Accelerators module (src/main/resources/ccf/accelerators.json) only knows
about the instance types listed in GPU_INSTANCES_TYPES; anything else with a
GPU is silently left without an operational_energy_kwh estimate.

This script asks the EC2 API for every instance type with a GPU and reports:

- instance types AWS offers that accelerators.json does not cover, and whether
  their GPU model already has wattage data in GPU_INFO;
- instance types in accelerators.json that the queried regions do not offer
  (usually a retired generation, or one that region simply does not have);
- disagreements on the GPU count -- including fractional GPUs, e.g. g6f.large
  gets 0.125 of an L4 -- or on the GPU model;
- GPU_INFO entries referenced without wattage data, or never referenced.

Instance types are region-specific, so pass every region you care about:

    python3 scripts/compare_gpu_accelerators_aws.py --region us-east-1 --region eu-west-2

Requires the AWS CLI with credentials that allow ec2:DescribeInstanceTypes (a
read-only call). Run it from the project root; only the Python standard library
is needed.
"""

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path

ACCELERATORS_JSON = "src/main/resources/ccf/accelerators.json"


def gpu_units(gpu):
    """Number of GPUs an instance gets, as a float.

    Instances sharing a partitioned GPU (g6f, gr6f) are reported by AWS with
    Count 0 plus a fractional GpuPartitionSize, e.g. 0.125 of an L4.
    """
    count = gpu.get("Count", 0)
    if count:
        return float(count)
    partition = gpu.get("GpuPartitionSize")
    if partition:
        return gpu.get("LogicalGpuCount", 1) * float(partition)
    return 0.0


def fetch_aws_gpu_instances(region):
    """{instance_type: info} for every instance type with a GPU in one region."""
    instances = {}
    next_token = None
    while True:
        cmd = ["aws", "ec2", "describe-instance-types", "--region", region,
               "--output", "json"]
        if next_token:
            cmd.extend(["--next-token", next_token])
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        for it in data.get("InstanceTypes", []):
            gpus = (it.get("GpuInfo") or {}).get("Gpus") or []
            if not gpus:
                continue
            # a single instance type never mixes GPU models in practice
            main = max(gpus, key=gpu_units)
            instances[it["InstanceType"]] = {
                "quantity": sum(gpu_units(g) for g in gpus),
                "manufacturer": main.get("Manufacturer", ""),
                "name": main.get("Name", ""),
                "regions": {region},
            }
        next_token = data.get("NextToken")
        if not next_token:
            return instances


def fetch_all_regions(regions):
    """Union of the GPU instance types offered across the given regions."""
    merged = {}
    for region in regions:
        print(f"Querying EC2 in {region}...", file=sys.stderr)
        for name, info in fetch_aws_gpu_instances(region).items():
            if name in merged:
                merged[name]["regions"] |= info["regions"]
            else:
                merged[name] = info
    return merged


def load_accelerators(path):
    with open(path) as f:
        data = json.load(f)
    return data["GPU_INSTANCES_TYPES"], data["GPU_INFO"]


def normalise_gpu_name(name):
    """'NVIDIA_TESLA_V100' / 'Tesla V100' -> 'v100' (vendor prefixes dropped)."""
    key = "".join(c for c in (name or "").lower() if c.isalnum())
    for vendor in ("nvidia", "tesla", "amd", "radeonpro"):
        key = key.replace(vendor, "")
    return key


def gpu_info_key(aws_info, gpu_info):
    """The GPU_INFO key matching an AWS GPU model, or None if there is none."""
    target = normalise_gpu_name(f"{aws_info['manufacturer']} {aws_info['name']}")
    for key in gpu_info:
        if normalise_gpu_name(key) == target:
            return key
    return None


def fmt_qty(value):
    """Render a GPU count without a trailing .0 (0.125 stays 0.125)."""
    return f"{value:g}"


def family(instance_type):
    return instance_type.split(".")[0]


def by_family(names):
    """Group instance types by family, preserving sorted order."""
    families = {}
    for name in sorted(names):
        families.setdefault(family(name), []).append(name)
    return families


def compare(gpu_types, gpu_info, aws):
    acc_set, aws_set = set(gpu_types), set(aws)
    quantity_diffs = []
    model_diffs = []
    for name in sorted(acc_set & aws_set):
        acc, a = gpu_types[name], aws[name]
        if not math.isclose(float(acc["quantity"]), a["quantity"], rel_tol=1e-9,
                            abs_tol=1e-9):
            quantity_diffs.append(name)
        if normalise_gpu_name(acc["type"]) != normalise_gpu_name(
                f"{a['manufacturer']} {a['name']}"):
            model_diffs.append(name)
    return {
        "missing": sorted(aws_set - acc_set),
        "not_offered": sorted(acc_set - aws_set),
        "common": sorted(acc_set & aws_set),
        "quantity_diffs": quantity_diffs,
        "model_diffs": model_diffs,
    }


def print_report(result, gpu_types, gpu_info, aws, regions):
    print("# GPU instance coverage")
    print(f"Regions queried:        {', '.join(regions)}")
    print(f"AWS instance types with a GPU: {len(aws)}")
    print(f"accelerators.json:             {len(gpu_types)}")
    print(f"Known to both:                 {len(result['common'])}")

    missing = result["missing"]
    print(f"\n## Offered by AWS but missing from accelerators.json ({len(missing)})")
    if not missing:
        print("  none")
    for fam, names in by_family(missing).items():
        print(f"  {fam}")
        for name in names:
            a = aws[name]
            key = gpu_info_key(a, gpu_info)
            wattage = (f"GPU_INFO has {key}" if key
                       else f"no GPU_INFO entry for {a['manufacturer']} {a['name']}")
            print(f"    {name}: {fmt_qty(a['quantity'])} x "
                  f"{a['manufacturer']} {a['name']}  [{wattage}]")

    not_offered = result["not_offered"]
    print(f"\n## In accelerators.json but not offered in the queried regions "
          f"({len(not_offered)})")
    print("   (a retired generation, or one those regions do not have)")
    if not not_offered:
        print("  none")
    for fam, names in by_family(not_offered).items():
        entries = ", ".join(
            f"{name} ({fmt_qty(float(gpu_types[name]['quantity']))} x "
            f"{gpu_types[name]['type']})" for name in names)
        print(f"  {fam}: {entries}")

    print(f"\n## GPU count mismatches ({len(result['quantity_diffs'])})")
    if not result["quantity_diffs"]:
        print("  none")
    for name in result["quantity_diffs"]:
        print(f"  {name}: accelerators.json "
              f"{fmt_qty(float(gpu_types[name]['quantity']))} vs AWS "
              f"{fmt_qty(aws[name]['quantity'])}")

    print(f"\n## GPU model mismatches ({len(result['model_diffs'])})")
    if not result["model_diffs"]:
        print("  none")
    for name in result["model_diffs"]:
        a = aws[name]
        print(f"  {name}: accelerators.json {gpu_types[name]['type']} vs AWS "
              f"{a['manufacturer']} {a['name']}")

    print("\n# GPU_INFO")
    referenced = {info["type"] for info in gpu_types.values()}
    undefined = sorted(referenced - set(gpu_info))
    unused = sorted(set(gpu_info) - referenced)
    print(f"## Referenced by an instance type but without wattage data "
          f"({len(undefined)})")
    print("  " + (", ".join(undefined) if undefined else "none"))
    print(f"## Defined but used by no instance type ({len(unused)})")
    print("  " + (", ".join(unused) if unused else "none"))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--region", action="append", dest="regions", metavar="REGION",
        help="AWS region to query; repeat for several (default us-east-1)")
    parser.add_argument(
        "--accelerators", default=ACCELERATORS_JSON, type=Path,
        help="path to accelerators.json (default %(default)s)")
    parser.add_argument(
        "--strict", action="store_true",
        help="exit 1 if anything is missing from accelerators.json or disagrees "
             "with AWS (instance types not offered in the queried regions do not "
             "count)")
    args = parser.parse_args()

    regions = args.regions or ["us-east-1"]

    if not args.accelerators.exists():
        sys.exit(f"{args.accelerators} not found -- run this from the project root")
    gpu_types, gpu_info = load_accelerators(args.accelerators)

    try:
        aws = fetch_all_regions(regions)
    except FileNotFoundError:
        sys.exit("AWS CLI not found: https://aws.amazon.com/cli/")
    except subprocess.CalledProcessError as e:
        sys.exit(f"aws ec2 describe-instance-types failed:\n{e.stderr.strip()}")

    result = compare(gpu_types, gpu_info, aws)
    print_report(result, gpu_types, gpu_info, aws, regions)

    if args.strict and (result["missing"] or result["quantity_diffs"]
                        or result["model_diffs"]):
        sys.exit(1)


if __name__ == "__main__":
    main()

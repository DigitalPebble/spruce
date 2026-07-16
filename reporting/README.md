# SPRUCE Reporting Tools

This directory contains Python scripts for generating reports and interactive dashboards from SPRUCE-enriched billing data. The queries use the [FOCUS](https://focus.finops.org/) (FinOps Open Cost & Usage Specification) columns emitted by all SPRUCE pipelines, so the same tools work on the output of every supported input format (AWS CUR, AWS FOCUS, Azure cost details, Azure FOCUS).

## Prerequisites

Install the required dependencies:

```bash
# For the report generator
pip install -r reporting/requirements-report.txt

# For the interactive dashboard
pip install -r reporting/requirements-dashboard.txt
```

## Input Data

Both tools expect SPRUCE-enriched Parquet files as input. These files contain:

- The original provider columns (cost, usage, resource metadata)
- FOCUS columns, present natively in FOCUS exports and added by the
  `FOCUSColumns` bridge modules for native exports
- SPRUCE-added sustainability metrics:
  - `operational_energy_kwh` - Energy consumption
  - `operational_emissions_co2eq_g` - Operational carbon emissions
  - `embodied_emissions_co2eq_g` - Embodied carbon emissions  
  - `water_cooling_l` - Water used for cooling
  - `water_electricity_production_l` - Water used for electricity production
  - `water_consumption_stress_area_l` - Water consumption in stress areas
  - `carbon_intensity` - Carbon intensity of the region
  - `power_usage_effectiveness` - PUE of the datacenter
  - `region` - Normalized region identifier

## Report Generator

Generate a static report with summary statistics and recommendations:

```bash
python3 reporting/report.py --input path/to/enriched/ --output report.md
```

### Options

- `-i / --input`: Path to enriched Parquet file, directory, glob pattern, or S3 URI (required)
- `-o / --output`: Output file; format inferred from the suffix (`.md`, `.html`, `.pdf`); defaults to Markdown on stdout
- `-t / --top-tags`: Maximum number of resource tags offered for interactive breakdown (default 10; 0 disables)

### Output

The generated report includes:
- Summary by billing period (energy, emissions, water) with everyday equivalences
- Top emitters by service
- Top instance types (derived from `SkuMeter`, where populated)
- Coverage (% of billed costs with emissions data, top uncovered services)
- Regional analysis
- Interactive tag breakdowns
- Recommendations generated from the data

## Interactive Dashboard

Launch a Streamlit-based interactive dashboard:

```bash
streamlit run reporting/dashboard.py -- --input path/to/enriched/
```

### Features

- **Overview**: Summary metrics and coverage statistics
- **Trend**: Time series visualization of emissions, energy, and water usage
- **Top Emitters**: Breakdown of highest-emitting services per region
- **Regions**: Regional analysis with emissions distribution
- **Tags**: Resource tag-based breakdowns (when the `Tags` column is available)

### Navigation

Use the sidebar to:
- Select input path (local directory or S3 URI)
- Filter by billing period
- Filter by region
- Select resource tag for breakdown (if available)
- Adjust number of top emitters to display

### S3 Support

The dashboard supports reading directly from S3:

```bash
streamlit run reporting/dashboard.py -- --input s3://bucket/path/to/enriched/
```

Requires AWS credentials configured in your environment.

## Data Requirements

### Columns Used

The queries rely on standard FOCUS columns and SPRUCE-generated columns only
(no provider-specific `x_` columns):

```
BILLING_PERIOD
region
ServiceName
ChargeCategory
BilledCost
ListCost
SkuMeter
operational_energy_kwh
operational_emissions_co2eq_g
embodied_emissions_co2eq_g
water_cooling_l
water_electricity_production_l
water_consumption_stress_area_l
carbon_intensity
power_usage_effectiveness
```

Any of these columns missing from the input is added as a typed NULL, so the
same queries run on the output of every pipeline — the corresponding report
sections simply come out empty. When the input is not hive-partitioned by
`BILLING_PERIOD`, the YYYY-MM billing periods are inferred from the FOCUS
`ChargePeriodStart` column.

### Optional Columns

- `Tags`: resource tags for tag-based breakdowns. AWS outputs carry it as a
  `MAP<VARCHAR,VARCHAR>`, Azure ones as a JSON string; both are supported.

If `Tags` is not present, the Tags tab will not be displayed in the dashboard.

## Output Format

Both tools produce visualizations using Plotly and display data in tabular format. The dashboard provides interactive filtering and exploration capabilities.

## Troubleshooting

### Missing Columns

If you encounter errors about missing columns:
1. Verify your input data was processed through SPRUCE
2. Check that all required columns are present
3. Use the latest version of SPRUCE for complete column coverage

### Performance

For large datasets:
- The dashboard caches query results for 5 minutes
- Filter data by period/region to improve responsiveness
- Use the report generator for static analysis of large time ranges

### S3 Access

For S3 access issues:
- Ensure AWS credentials are properly configured
- Verify the S3 bucket and path are accessible
- Check network connectivity to AWS S3 endpoints

## License

These reporting tools are part of SPRUCE and are licensed under the Apache License 2.0.
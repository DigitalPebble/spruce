# SPRUCE Reporting Tools

This directory contains Python scripts for generating reports and interactive dashboards from SPRUCE-enriched AWS Cost & Usage Report (CUR) data.

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

- Original AWS CUR columns (cost, usage, resource metadata)
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

Generate a static HTML report with summary statistics and visualizations:

```bash
python3 reporting/report.py --input path/to/enriched.parquet --output report.html
```

### Options

- `--input`: Path to enriched Parquet file or directory (supports glob patterns)
- `--output`: Output HTML file path (default: `report.html`)
- `--title`: Report title (default: "SPRUCE Report")
- `--period`: Filter by billing period (e.g., "2024-01")
- `--region`: Filter by region (e.g., "us-east-1")

### Output

The generated report includes:
- Summary metrics (total cost, emissions, energy, water)
- Time series charts for emissions, energy, and water usage
- Top emitters breakdown
- Regional analysis
- Service breakdown

## Interactive Dashboard

Launch a Streamlit-based interactive dashboard:

```bash
streamlit run reporting/dashboard.py -- --input path/to/enriched/
```

### Features

- **Overview**: Summary metrics and coverage statistics
- **Trend**: Time series visualization of emissions, energy, and water usage
- **Top Emitters**: Breakdown of highest-emitting services and resources
- **Regions**: Regional analysis with emissions distribution
- **Tags**: Resource tag-based breakdowns (when `resource_tags` column is available)

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

### Required Columns

The tools expect these columns to be present in the enriched data:

```
BILLING_PERIOD
region
product_region_code
line_item_product_code
product_servicecode
line_item_operation
line_item_line_item_type
line_item_unblended_cost
pricing_public_on_demand_cost
operational_energy_kwh
operational_emissions_co2eq_g
embodied_emissions_co2eq_g
water_cooling_l
water_electricity_production_l
water_consumption_stress_area_l
carbon_intensity
power_usage_effectiveness
```

### Optional Columns

- `resource_tags`: MAP<VARCHAR,VARCHAR> column for tag-based breakdowns

If `resource_tags` is not present, the Tags tab will not be displayed in the dashboard.

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
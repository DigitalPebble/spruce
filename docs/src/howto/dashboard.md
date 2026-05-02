# Build a simple dashboard

SPRUCE writes enriched Parquet output, so a dashboard can read the same files as
the automated report and run interactive DuckDB queries locally. The repository
includes `reporting/dashboard.py`, a [Streamlit](https://streamlit.io/)
example for exploring cost, energy, emissions, water, region, and tag
breakdowns.

## Install

Install the Python dependencies for the dashboard:

```shell
pip install -r reporting/requirements-dashboard.txt
```

This requirements file is separate from `requirements-report.txt` so the report
CLI stays small for users who do not need an interactive app.

## Run

Point the dashboard at a local output directory, a glob, a single Parquet file,
or an S3 URI:

```shell
streamlit run reporting/dashboard.py -- --input output/
streamlit run reporting/dashboard.py -- --input 'output/**/*.parquet'
streamlit run reporting/dashboard.py -- --input s3://my-bucket/spruced/
```

The `--` separator is required when passing dashboard arguments through
Streamlit; without it, Streamlit consumes options such as `--input`.

For S3 input, DuckDB uses its `httpfs` extension and the ambient AWS credential
chain, matching the pattern used by `report.py`.

To generate a small local dataset for review or demos:

```shell
python scripts/gen_synthetic.py -o output/synth.parquet
streamlit run reporting/dashboard.py -- --input output/
```

## What it shows

The dashboard mirrors the core sections from `report.py`:

| Section | What it shows |
|---|---|
| Overview | Total usage cost, energy, operational emissions, embodied emissions, water usage, and cost coverage for the full input |
| Trend | Separate energy, total emissions, and water charts by billing period |
| Top emitters | Sorted top product/service/operation combinations as a table-first view |
| Regions | Emissions share by region, plus regional KPI cards and detail table |
| Tags | Sorted tag-value chart and table for a selected `resource_tags` key |

Use the sidebar to filter by billing period and region. If `resource_tags` are
present, choose one tag key to break down emissions by tag value. The cost
coverage KPI intentionally matches `report.py` and is calculated across the full
input, not only the currently selected filters.

## Screenshots

Screenshots should be captured from a representative enriched dataset after the
dashboard layout is finalized. Use `scripts/gen_synthetic.py` for local review
data if real SPRUCE output is not available yet.

## Customize

The dashboard is designed as a starter example rather than a framework. To add
metrics or charts, edit the SQL query helpers in `reporting/dashboard.py`. They
intentionally match the query shapes in `report.py`, so changes can
usually be copied between the report and the dashboard.

`reporting/requirements-dashboard.txt` intentionally keeps versions unpinned,
following the lightweight style of `requirements-report.txt`. For
reproducible deployments, pin versions in your own environment or lock file.

The dashboard ships a `.streamlit/config.toml` with the SPRUCE brand color
(`#BC6554`) and embeds `logo.png` from the repository root in the page header,
sidebar, and browser tab. To swap colors or remove the logo, edit
`.streamlit/config.toml`, `reporting/styles.css`, and the `BRAND_COLOR` /
`LOGO_PATH` constants in `reporting/dashboard.py`. If you maintain a global
`~/.streamlit/config.toml`, the project-local file overrides it only while
running from the repository root.

Streamlit caches the DuckDB connection for each input path during a session. If
you switch between several large inputs while reviewing data, restart the app to
release old in-memory connections.

## Alternatives

For a static report, use `python report.py -o report.html` or
`python report.py -o report.pdf`.
For a full BI stack, point Tableau, PowerBI, Superset, or similar tools at the
same enriched Parquet output through DuckDB, Athena, or another query layer. See
the [comparison with other open source tools](../comparison.md) for broader
context.

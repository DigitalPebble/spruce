# Comparison with other open source tools

SPRUCE is part of a growing ecosystem of open source tools focused on measuring and reducing the environmental impact of cloud computing. This page compares SPRUCE with other notable open source projects in this space.

## Cloud Carbon Footprint (CCF)

[Cloud Carbon Footprint](https://www.cloudcarbonfootprint.org/) is an open source tool that provides cloud carbon emissions estimates.

> **Note**: Cloud Carbon Footprint is no longer actively maintained. As a result, its data and methodology may be outdated. SPRUCE implements CCF's core methodology but with actively maintained data sources and models.

### Similarities

- Both tools estimate the carbon footprint of cloud usage
- Both support AWS (as well as GCP and Azure for CCF)
- Both use similar methodologies for calculating operational emissions
- Both are open source and transparent about their calculation methods
- SPRUCE implements several modules based on CCF's methodology (see [Cloud Carbon Footprint modules](modules.md#cloud-carbon-footprint))

### Key Differences

| Feature | SPRUCE | Cloud Carbon Footprint |
|---------|--------|------------------------|
| **Architecture** | Apache Spark-based for scalable data processing | Node.js application with web dashboard |
| **Data Processing** | Batch processing of Cost and Usage Reports (CUR) in Parquet format | Real-time API calls to cloud providers |
| **Primary Use Case** | Enrichment of existing usage reports for GreenOps + FinOps | Standalone dashboard for carbon tracking |
| **Deployment** | Runs on-premises or in the cloud (e.g., EMR) without exposing data | Requires credentials to query cloud provider APIs |
| **Data Privacy** | Processes data locally, no external API calls for core functionality | Requires cloud provider credentials |
| **Modularity** | Highly modular with configurable enrichment pipelines | Fixed calculation pipeline with configuration options |
| **Output** | Enriched Parquet/CSV files for custom analytics and visualization | Pre-built dashboard and recommendations |
| **Embodied Carbon** | Includes embodied emissions via Boavizta integration | Limited embodied carbon estimates |
| **Scalability** | Designed for large-scale data processing with Apache Spark | Suitable for smaller to medium deployments |
| **Carbon Intensity** | Uses ElectricityMaps average data (lifecycle emissions) | Uses ElectricityMaps API for real-time data |
| **Maintenance Status** | Actively maintained with regular updates | No longer actively maintained |

### When to Choose SPRUCE

- You want to combine GreenOps and FinOps data in a single workflow
- You need to process large volumes of historical CUR data
- You prefer to keep your usage data within your own infrastructure
- You want to build custom dashboards and reports with tools like DuckDB, Tableau, or PowerBI
- You need fine-grained control over the calculation methodology through configurable modules

### When to Choose CCF

- You want a ready-to-use dashboard with minimal setup
- You prefer real-time estimates over batch processing
- You want built-in recommendations for reducing cloud carbon footprint
- You need a web-based interface for non-technical stakeholders

## CloudScanner

[CloudScanner](https://github.com/Boavizta/cloud-scanner) is an open source tool by Boavizta that focuses on estimating the environmental impact of cloud resources.

### Similarities

- Both tools estimate the environmental impact of cloud usage
- Both are open source and transparent about their methodologies
- Both can work with AWS cloud resources
- Both aim to provide data for sustainability reporting

### Key Differences

| Feature | SPRUCE | CloudScanner |
|---------|--------|--------------|
| **Primary Purpose** | Enrichment of Cost and Usage Reports (CUR) for GreenOps + FinOps | Direct resource scanning for environmental impact |
| **Architecture** | Apache Spark-based batch processing | Direct API-based resource scanning |
| **Data Source** | Cost and Usage Reports (CUR) in Parquet/CSV format | Live cloud resource inventory via cloud provider APIs |
| **Scope** | Focuses on AWS CUR data enrichment | Scans multiple cloud providers and resource types |
| **Integration** | Enriches existing billing data for FinOps alignment | Standalone tool for environmental assessment |
| **Scalability** | Designed for large-scale historical data processing | Suitable for periodic resource audits |
| **Output** | Enriched reports in Parquet/CSV for custom analytics | Environmental impact reports and metrics |
| **Boavizta Usage** | Uses BoaviztAPI for embodied emissions | Built by Boavizta, deeply integrated with their methodology |

### When to Choose SPRUCE

- You want to combine environmental impact with cost data from CUR reports
- You need to process large volumes of historical usage data
- You prefer batch processing over real-time scanning
- You want fine-grained control through configurable enrichment modules
- You need to integrate with existing FinOps workflows

### When to Choose CloudScanner

- You want to scan live cloud resources directly without CUR data
- You need multi-cloud support beyond AWS
- You prefer periodic audits over continuous batch processing
- You want a tool specifically designed around Boavizta's methodology

## Other Related Tools

### Kepler (Kubernetes Efficient Power Level Exporter)

[Kepler](https://sustainable-computing.io/) is a CNCF project that exports energy-related metrics from Kubernetes clusters.

**Key difference**: Kepler focuses on real-time power consumption metrics at the container/pod level using eBPF, while SPRUCE focuses on enriching historical cost reports with carbon estimates at the service level.

### Scaphandre

[Scaphandre](https://github.com/hubblo-org/scaphandre) is a power consumption monitoring agent that can export metrics to various monitoring systems.

**Key difference**: Scaphandre provides real-time power measurements at the host/process level, while SPRUCE provides carbon estimates based on cloud usage patterns and billing data.

## Summary

SPRUCE is designed to fill a specific niche in the cloud sustainability ecosystem:

- **Scalable batch processing** of cloud usage data with Apache Spark
- **Integration with existing FinOps workflows** by enriching cost reports
- **Flexibility and modularity** through configurable enrichment pipelines  
- **Data privacy** by processing data within your own infrastructure
- **Transparency** through open source methodologies and models

Different tools in this space serve different needs, and organizations may benefit from using multiple tools depending on their requirements for real-time vs. batch processing, security vs. sustainability focus, and dashboard vs. custom analytics preferences.

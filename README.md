# EIA Energy-Tracker: U.S. Energy Information Administration Data Pipeline and Dashboards
## Developed by José Mata

## Purpose

This project implements a serverless Azure data pipeline that automatically ingests hourly ISO New England electricity data from the U.S. Energy Information Administration (EIA) datasets, processes it through Bronze, Silver, and Gold layers in Azure blob storage and SQL Server, and visualizes both detailed and aggregated metrics in clean, decision-ready Power BI dashboards.

Planned future work will expand the pipeline to include additional EIA datasets and data from all regions of the US.  

## Project Scope

- Automatically ingest hourly electricity demand data from the EIA API
- Store raw JSON responses in **Azure Blob Storage** (Bronze layer)
- Transform and clean the raw data into structured tables for **Power BI** Dashboards (Silver layer)
- Prepare aggregated and summarized data, also for **Power BI** dashboards (Gold layer)
- Demonstrate use of **serverless functions**, cloud storage, and **SQL Server** for data engineering
- Tested locally and currently deployed to Azure

## Architecture
![Architecture Diagram](assets/Project%20Architecture.png)

- **Bronze Layer**: Raw JSON files stored in a hierarchical folder structure in Blob Storage (year/month/day/hour/minute-second)
- **Silver Layer**: Cleaned and transformed hourly data stored in an Azure SQL Server Database (structured tables for analysis)
- **Gold Layer**: Aggregated and averaged daily data stored in an Azure SQL Server Database (structured tables for analysis)
_ **BI dashboard**: Power BI: Report including metrics displaying the Silver and Gold data

## Key Technologies

- **Python**: Function implementation, data processing
- **Azure Functions**: Serverless, timer-triggered functions
- **Azure Blob Storage**: Raw data storage for the Bronze layer
- **Azure SQL Server Database**: Cleaned and structured data storage (Silver and Gold layers)
- **Power BI**: Data visualization in dashboards
- **Git/GitHub**: Version control
- **Postman**: API testing
- **VSCode**: IDE used with Azure extensions: Azurite, Azure Tools, Azure Storage, SQL Server, etc.

## Resulting Power BI Silver Dashboard
![Silver Dashboard picture](assets/Silver%20Dashboard.png)

## Resulting Power BI Gold Dashboard
![Gold Dashboard picture](assets/Gold%20Dashboard.png)

## Future Work

- Add more EIA datasets (energy prices, total consumption, renewable generation, etc.)
- Expand the pipeline to include data from all regions of the US.
- Update the Power BI dashboards accordingly by adding more metrics, including a US map to select and visualize all available energy data per geographical region.
- Automate deployment with GitHub Actions

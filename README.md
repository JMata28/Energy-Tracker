# Energy-Tracker: U.S. Energy Information Administration Data Pipeline
## Created by José Mata

## Purpose

This project is a data pipeline that collects hourly electricity demand data for the New England region (ISNE) from the U.S. Energy Information Administration (EIA) API and stores it in a structured format suitable for analytics.  

The current scope demonstrates a full data workflow including ingestion, raw storage (Bronze), transformation (Silver), and aggregation and reporting (Gold) for the initial dataset.  

The project is implemented as an **Azure Function** using **Python**, **Azure Blob Storage**, and (future) **Azure SQL Database**, and includes version control and deployment readiness.

Planned future work will expand the pipeline to include additional EIA datasets and data from all regions of the US.  

## Project Scope

- Collect hourly electricity demand data from the EIA API
- Store raw JSON responses in **Azure Blob Storage** (Bronze layer)
- Transform and clean the raw data into structured tables (Silver layer)
- Prepare aggregated and summarized data for BI dashboards (Gold layer)
- Demonstrate use of **serverless functions**, cloud storage, and SQL for data engineering
- Designed for testing locally and deploying to Azure

## Architecture
EIA API (hourly demand) → 
Azure Function (Python, timer-triggered) →
Azure Blob Storage (Bronze: raw JSON) →
Azure SQL Database (Silver: cleaned tables) → 
BI Dashboards / Gold Layer

- **Bronze Layer**: Raw JSON files stored in a hierarchical folder structure in Blob Storage (year/month/day/hour/minute-second)
- **Silver Layer**: Cleaned and transformed data stored in Azure SQL (structured tables for analysis)
- **Gold Layer**: BI dashboard: Power BI

## Key Technologies

- **Python**: Function implementation, data processing
- **Azure Functions**: Serverless, timer-triggered functions
- **Azure Blob Storage**: Raw data storage for the Bronze layer
- **Azure SQL Database**: Cleaned and structured data storage (Silver layer)
- **Git/GitHub**: Version control
- **Postman / Requests**: API testing
- **Optional BI Tools**: Power BI for dashboards

## Future Work

- Add more EIA datasets (energy prices, total consumption, renewable generation)  
- Transform and store new data in Azure SQL (Silver layer)  
- Build aggregated views and BI dashboards (Gold layer)  
- Expand the pipeline to include data from **all regions of the US**  
- Improve error handling and logging in the Azure Function  
- Automate deployment with GitHub Actions
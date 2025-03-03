Real Estate Data Pipeline with Airflow, Snowflake, and dbt
1. Project Overview
This project aims to build a scalable ETL pipeline that integrates real estate and crime data into a dimensional model in Snowflake, enabling analytics and reporting. The pipeline is orchestrated using Apache Airflow (via Astronomer Cloud) and uses dbt for data transformation and quality testing.

2. Problem Statement
The real estate industry lacks a centralized, clean, and structured dataset that combines property listings, rental prices, economic indicators, and crime statistics for better decision-making. This project integrates disparate data sources into an analytical model, helping real estate investors, policymakers, and data analysts derive actionable insights.

3. KPI and Use Cases
  i.Key Performance Indicators (KPIs)
  ii.Average property price by county 
  iii. Crime rate impact on property prices
   iv.Days on market for property listings
   v.Rental price trends over time
Use Cases
Real estate investors can analyze property price trends.
Policy analysts can assess crime impact on housing demand.
Homebuyers can compare price variations across locations.
Rental market analysis for setting competitive rental prices.
4. Data Sources
Real estate listings dataset (CSV)
Rental property dataset (JSON API)
County-level economic data (Parquet format)
Crime statistics dataset (API)
Each dataset contains over 1 million rows, fulfilling the capstone project requirements.

5. Tables in Snowflake
Fact Table:
FACT_PROPERTY_LISTINGS: Stores real estate listings with timestamps, pricing, and status updates.
Dimension Tables:
DIM_PROPERTY_WITH_LOCATION: Details of properties including location, size, and price.
DIM_PROPERTY_WITH_LOCATION_RENT: Stores rental property details.
DIM_CRIME: Crime statistics related to property locations.
DIM_COUNTY: County-level details.
DIM_FIN_HOUSING_COST: Monthly housing costs by county.
DIM_FIN_GDP: Economic indicators for each county.
DIM_FIN_HHI: Household income statistics by county.
6. Technologies Used
Technology	Purpose
Astronomer Cloud	Deploys Airflow DAGs in the cloud
Apache Airflow	Orchestrates ETL pipeline
Python	Extracts and processes data
Snowflake	Stores the dimensional model
dbt	Cleans, transforms, and models data
SQL	Loads and queries structured data
APIs (JSON)	Fetches crime statistics dynamically
7. Why I Used These Technologies
Airflow (via Astronomer Cloud): Scalable and managed orchestration for periodic data workflows.
Snowflake: Cloud-based, optimized for large-scale analytics.
dbt: Modular approach for data transformation, testing, and documentation.
Python: Flexible scripting for data extraction and cleaning.
8. Architecture
Data Ingestion → Extracts raw data from APIs & files.
Data Transformation → Cleans & validates data with dbt.
Data Loading → Loads structured data into Snowflake.
Orchestration → Airflow DAG automates pipeline execution.
Visualization → KPIs and dashboards built on transformed data.

9. KPI Visualizations
Crime Rate vs. Property Prices (Heatmap)
Rental Price Trends Over Time (Line Chart)
Days on Market for Property Listings (Histogram)
Property Price Distribution by County (Bar Chart)
10. Pipeline
Tasks in DAG
Start Task (EmptyOperator) - Marks the beginning of the workflow.
Python Operators - Execute scripts for processing rental and property listing data:
run_rent_inactive
run_listings_inactive
run_main_script
run_rent
SQL Operators - Load processed data into Snowflake:
load_dim_property_with_location
load_dim_property_with_location_rent
load_fact_property_listings
dbt Transformation - Cleans and transforms data:
dbt_run (Executes dbt models)
dbt_test (Runs tests on transformed data)
End Task (EmptyOperator) - Marks the completion of the workflow.
DBT Tests
Schema validation (Data types, null constraints)
Referential integrity (Foreign key relationships)
Data quality checks (Duplicate records, missing values)
Astronomer Cloud
Hosted Apache Airflow with automated DAG scheduling.
Log monitoring for debugging and pipeline health tracking.
Cloud-based execution to ensure scalability and reliability.
11. Challenges
Handling large datasets: Optimized queries and indexing in Snowflake.
Data cleaning complexity: Used dbt for modular transformation.
API rate limits: Implemented retries and batching.
Pipeline monitoring: Configured logging in Airflow & Astronomer Cloud.
12. Next Steps
Expand the dataset to include more economic indicators.
Improve performance by partitioning and clustering in Snowflake.
Enhance visualizations with a dashboard (Tableau or Power BI).
Integrate machine learning models for property price predictions.
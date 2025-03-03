Here is a properly formatted **GitHub README** file with a **clickable table of contents** that links to respective sections:  

---

# **Real Estate Data Pipeline with Airflow, Snowflake, and dbt**  

## 📌 **Table of Contents**  
- [Project Overview](#project-overview)  
- [Problem Statement](#problem-statement)  
- [KPI and Use Cases](#kpi-and-use-cases)  
- [Data Sources](#data-sources)  
- [Tables in Snowflake](#tables-in-snowflake)  
- [Technologies Used](#technologies-used)  
- [Why I Used These Technologies](#why-i-used-these-technologies)  
- [Architecture](#architecture)  
- [KPI Visualizations](#kpi-visualizations)  
- [Pipeline](#pipeline)  
  - [Tasks in DAG](#tasks-in-dag)  
  - [DBT Tests](#dbt-tests)  
  - [Astronomer Cloud](#astronomer-cloud)  
- [Challenges](#challenges)  
- [Next Steps](#next-steps)  

---

## **📌 Project Overview**  
This project aims to build a **scalable ETL pipeline** that integrates **real estate and crime data** into a **dimensional model in Snowflake**, enabling analytics and reporting.  

The pipeline is orchestrated using **Apache Airflow (via Astronomer Cloud)** and leverages **dbt** for data transformation and quality testing.  

---

## **📌 Problem Statement**  
The real estate industry lacks a **centralized, clean, and structured dataset** that combines **property listings, rental prices, economic indicators, and crime statistics** for better decision-making.  

This project integrates disparate data sources into an analytical model, helping:  
✔ **Real estate investors** analyze price trends.  
✔ **Policy analysts** assess crime impact on housing demand.  
✔ **Homebuyers** compare price variations across locations.  
✔ **Rental market analysts** set competitive rental prices.  

---

## **📌 KPI and Use Cases**  

### **Key Performance Indicators (KPIs)**  
📌 **Average property price** by county and state  
📌 **Crime rate impact** on property prices  
📌 **Days on market** for property listings  
📌 **Rental price trends** over time  

### **Use Cases**  
✅ Investors analyze **price trends**.  
✅ Policymakers assess **crime impact** on real estate.  
✅ Buyers compare **price variations** across regions.  
✅ Market analysts optimize **rental pricing**.  

---

## **📌 Data Sources**  

### **1️⃣ Rentcast API (JSON API)**  
#### **Sale Listing Record Example**  
```json
{
  "id": "3821-Hargis-St,-Austin,-TX-78723",
  "formattedAddress": "3821 Hargis St, Austin, TX 78723",
  "city": "Austin",
  "state": "TX",
  "price": 899000,
  "listingType": "Standard",
  "listedDate": "2024-06-24T00:00:00.000Z",
  "daysOnMarket": 99
}
```

#### **Rental Listing Record Example**  
```json
{
  "id": "2005-Arborside-Dr,-Austin,-TX-78754",
  "formattedAddress": "2005 Arborside Dr, Austin, TX 78754",
  "city": "Austin",
  "state": "TX",
  "price": 2200,
  "listedDate": "2024-09-18T00:00:00.000Z",
  "daysOnMarket": 13
}
```

### **2️⃣ County-Level Economic Data (CSV Format)**  
- **GDP, Household Income, Housing Cost, etc.**  

### **3️⃣ Crime Statistics Dataset (CSV Format)**  
- **County ID, Crime Type, Offense Count, Yearly Trends**  

---

## **📌 Tables in Snowflake**  

### **Fact Table:**  
📌 `FACT_PROPERTY_LISTINGS` – **Stores real estate listings with timestamps, pricing, and status updates.**  

### **Dimension Tables:**  
📌 `DIM_PROPERTY_WITH_LOCATION` – **Property details (location, size, price).**  
📌 `DIM_PROPERTY_WITH_LOCATION_RENT` – **Rental property details.**  
📌 `DIM_CRIME` – **Crime statistics for property locations.**  
📌 `DIM_COUNTY` – **County-level details.**  
📌 `DIM_FIN_HOUSING_COST` – **Monthly housing costs by county.**  
📌 `DIM_FIN_GDP` – **County-wise GDP indicators.**  
📌 `DIM_FIN_HHI` – **Household income statistics.**  

---

## **📌 Technologies Used**  

| **Technology**         | **Purpose**                                    |
|------------------------|----------------------------------------------|
| **Astronomer Cloud**   | Deploys Airflow DAGs in the cloud           |
| **Apache Airflow**     | Orchestrates ETL pipeline                   |
| **Python**            | Extracts and processes data                  |
| **Snowflake**         | Stores the raw data and dimensional model    |
| **dbt**               | Cleans, transforms, and models data          |
| **SQL**               | Loads and queries structured data            |
| **APIs (JSON)**       | Fetches rental data dynamically              |

---

## **📌 Why I Used These Technologies**  
✔ **Airflow (via Astronomer Cloud):** Cloud-based orchestration for scheduled workflows.  
✔ **Snowflake:** Cloud-native, scalable for analytical workloads.  
✔ **dbt:** Enables modular data transformations and quality testing.  
✔ **Python:** Flexible for API extraction and data processing.  

---

## **📌 Architecture**  
📌 **Data Ingestion** → Extracts raw data from APIs & files.  
📌 **Data Transformation** → Cleans & validates data using **dbt**.  
📌 **Data Loading** → Stores structured data in **Snowflake**.  
📌 **Orchestration** → **Airflow DAG** automates ETL pipeline.  
📌 **Visualization** → KPIs and dashboards built from transformed data.  

---

## **📌 KPI Visualizations**  
📊 **Crime Rate vs. Property Prices** *(Heatmap)*  
📊 **Rental Price Trends Over Time** *(Line Chart)*  
📊 **Days on Market for Property Listings** *(Histogram)*  
📊 **Property Price Distribution by County** *(Bar Chart)*  

---

## **📌 Pipeline**  

### **Tasks in DAG**  
1️⃣ **Start Task** → Begins workflow.  
2️⃣ **Python Operators** → Process property and rental data:  
   - `run_rent_inactive`  
   - `run_listings_inactive`  
   - `run_main_script`  
   - `run_rent`  
3️⃣ **SQL Operators** → Load data into **Snowflake**:  
   - `load_dim_property_with_location`  
   - `load_dim_property_with_location_rent`  
   - `load_fact_property_listings`  
4️⃣ **dbt Transformation** → Cleans & transforms data:  
   - `dbt_run`  
   - `dbt_test`  
5️⃣ **End Task** → Marks completion.  

### **DBT Tests**  
✅ **Schema validation** (Data types, null constraints)  
✅ **Referential integrity** (Foreign key relationships)  
✅ **Data quality checks** (Duplicates, missing values)  

### **Astronomer Cloud**  
✔ Hosted **Apache Airflow** with **automated DAG scheduling**.  
✔ **Log monitoring** for debugging & pipeline health tracking.  
✔ **Cloud-based execution** for scalability.  

---

## **📌 Challenges**  
⚠ **Handling large datasets** → Optimized Snowflake queries.  
⚠ **Data cleaning complexity** → Used dbt for modular transformations.  
⚠ **API rate limits** → Implemented retries and batching.  
⚠ **Pipeline monitoring** → Configured logging in Airflow & Astronomer Cloud.  

---

## **📌 Next Steps**  
🚀 Expand the dataset to include **more economic indicators**.  
🚀 Improve performance using **Snowflake partitioning & clustering**.  
🚀 Enhance visualizations with **Tableau or Power BI dashboards**.  
🚀 Integrate **machine learning models** for property price predictions.  

---

This README is now fully structured and **GitHub-friendly** with clickable sections! 🚀 Let me know if you need any tweaks! 😊

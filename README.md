

**NextHome Analytics** by Sarmad Memon and Varun Vuppala

**Purpose of project:** 

We are developing a **comprehensive, all-in-one** dashboard that integrates **real estate, crime, and financial** data for the North Dallas region. The dashboard allows users to input 
various factors and receive insights to make **informed decisions** when renting or buying a property. 

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
📌 **Average property price** by county and city
📌 **Days on market** for property listings 
📌 **Crime impact** on property prices  
📌 **Rental price trends** over time  
📌 **Agent performance**
📌 **GDP growth %**

### **Use Cases**  
✅ Buyers to assess their affordability by **Average property price**, **estimated home cost** and **average household income**.
✅ Investors analyze **price trends** and **GDP growth**.
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
    "addressLine1": "3821 Hargis St",
    "addressLine2": null,
    "city": "Austin",
    "state": "TX",
    "zipCode": "78723",
    "county": "Travis",
    "latitude": 30.290643,
    "longitude": -97.701547,
    "propertyType": "Single Family",
    "bedrooms": 4,
    "bathrooms": 2.5,
    "squareFootage": 2345,
    "lotSize": 3284,
    "yearBuilt": 2008,
    "hoa": {
      "fee": 65
    },
    "status": "Active",
    "price": 899000,
    "listingType": "Standard",
    "listedDate": "2024-06-24T00:00:00.000Z",
    "removedDate": null,
    "createdDate": "2021-06-25T00:00:00.000Z",
    "lastSeenDate": "2024-09-30T13:11:47.157Z",
    "daysOnMarket": 99,
    "mlsName": "UnlockMLS",
    "mlsNumber": "5519228",
    "listingAgent": {
      "name": "Jennifer Welch",
      "phone": "5124313110",
      "email": "jennifer@gottesmanresidential.com",
      "website": "https://www.gottesmanresidential.com"
    },
    "listingOffice": {
      "name": "Gottesman Residential R.E.",
      "phone": "5124512422",
      "email": "nataliem@gottesmanresidential.com",
      "website": "https://www.gottesmanresidential.com"
    },
    "history": {
      "2024-06-24": {
        "event": "Sale Listing",
        "price": 899000,
        "listingType": "Standard",
        "listedDate": "2024-06-24T00:00:00.000Z",
        "removedDate": null,
        "daysOnMarket": 99
      }
    }
  }
```

#### **Rental Listing Record Example**  
```json
{
    "id": "2005-Arborside-Dr,-Austin,-TX-78754",
    "formattedAddress": "2005 Arborside Dr, Austin, TX 78754",
    "addressLine1": "2005 Arborside Dr",
    "addressLine2": null,
    "city": "Austin",
    "state": "TX",
    "zipCode": "78754",
    "county": "Travis",
    "latitude": 30.35837,
    "longitude": -97.66508,
    "propertyType": "Single Family",
    "bedrooms": 3,
    "bathrooms": 2.5,
    "squareFootage": 1681,
    "lotSize": 4360,
    "yearBuilt": 2019,
    "hoa": {
      "fee": 45
    },
    "status": "Active",
    "price": 2200,
    "listingType": "Standard",
    "listedDate": "2024-09-18T00:00:00.000Z",
    "removedDate": null,
    "createdDate": "2024-09-19T00:00:00.000Z",
    "lastSeenDate": "2024-09-30T03:49:20.620Z",
    "daysOnMarket": 13,
    "mlsName": "CentralTexas",
    "mlsNumber": "556965",
    "listingAgent": {
      "name": "Zachary Barton",
      "phone": "5129948203",
      "email": "zak-barton@realtytexas.com",
      "website": "https://zak-barton.realtytexas.homes"
    },
    "listingOffice": {
      "name": "Realty Texas",
      "phone": "5124765348",
      "email": "sales@realtytexas.com",
      "website": "https://www.realtytexas.com"
    },
    "history": {
      "2024-09-18": {
        "event": "Rental Listing",
        "price": 2200,
        "listingType": "Standard",
        "listedDate": "2024-09-18T00:00:00.000Z",
        "removedDate": null,
        "daysOnMarket": 13
      }
    }
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
| **Python**             | Extracts and processes data                  |
| **Snowflake**          | Stores the raw data and dimensional model    |
| **dbt**                | Cleans, transforms, and models data          |
| **SQL**                | Loads and queries structured data            |
| **APIs (JSON)**        | Fetches rental data dynamically              |
| **PowerBI**            | Visualize our Business Case  

---

## **📌 Why I Used These Technologies**  
✔ **Airflow (via Astronomer Cloud):** Cloud-based orchestration for scheduled workflows.  
✔ **Snowflake:** Cloud-native, scalable for analytical workloads.  
✔ **dbt:** Enables modular data transformations and quality testing.  
✔ **Python:** Flexible for API extraction and data processing.  
✔ **PowerBI:** Mature tool to visualize complex metrics with DAX. 
---

## **📌 Architecture**  
📌 **Data Ingestion** → Extracts raw data from APIs & files.  
📌 **Data Transformation** → Cleans & validates data using **dbt**.  
📌 **Data Loading** → Stores structured data in **Snowflake**.  
📌 **Orchestration** → **Airflow DAG** automates ETL pipeline.  
📌 **Visualization** → KPIs and dashboards built from transformed data.  

---

## **📌 KPI Visualizations**  
📊 **An interactive map plotting property locations by latitude and longitude**  *(ArcGIS)*
📊 **Days on Market for Property Listings** *(Bar Chart)* 
📊 **Average Property Prices for Property Listings** *(Bar Chart)* 
📊 **Top agents in North Dallas** *(Table)*
📊 **Crime Rate vs. Property Prices** *(Heatmap)*  
📊 **An interactive calculator showing sq.footage affordability according to Income**(KPI) 



![image](https://github.com/user-attachments/assets/a06641bb-1c56-4247-a311-0fa9eb64c2c3)
![image](https://github.com/user-attachments/assets/55a3f858-d7ba-4527-a7b3-ca8f0cd15bfb)
![image](https://github.com/user-attachments/assets/7f449cb4-076c-4216-a5ea-4975dda4bfb0)
![image](https://github.com/user-attachments/assets/491bb401-1989-4002-b8c8-3bfbaf40cf02)

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


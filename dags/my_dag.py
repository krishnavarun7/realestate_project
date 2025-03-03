# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import sys
# import os
#
# # Add the include directory to sys.path
# sys.path.insert(0, os.path.abspath('/usr/local/airflow/include'))
#
# # Import main function
# from main import main
# from listings_inactive import listings_inactive_main
# from rent import rent_main
# from rent_inactive import rent_inactive_main
# # Ensure main() exists in main.py
#
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 2, 25),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# with DAG('run_main_script_dag',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#
#     task = PythonOperator(
#         task_id='run_main_script',
#         python_callable=main  # Run the main function from main.py
#     )
#
#     run_listings_inactive = PythonOperator(
#         task_id='run_listings_inactive',
#         python_callable= listings_inactive_main
#     )
#
#     # Task to run rent.py
#     run_rent = PythonOperator(
#         task_id='run_rent',
#         python_callable= rent_main
#     )
#
#     # Task to run rent_inactive.py
#     run_rent_inactive = PythonOperator(
#         task_id='run_rent_inactive',
#         python_callable= rent_inactive_main
#     )
#
#     # Define the order of execution (if needed)
#     run_main_script >> run_listings_inactive >> run_rent >> run_rent_inactive
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import sys
# import os
#
# # Add the include directory to sys.path
# sys.path.insert(0, os.path.abspath('/usr/local/airflow/include'))
#
# # Import functions from each file
# from main import main
# from listings_inactive import listings_inactive_main
# from rent import rent_main
# from rent_inactive import rent_inactive_main
#
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 2, 25),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# with DAG('run_all_scripts_dag',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#
#     # Task to run main.py
#     run_main_script = PythonOperator(
#         task_id='run_main_script',
#         python_callable=main
#     )
#
#     # Task to run listings_inactive.py
#     run_listings_inactive = PythonOperator(
#         task_id='run_listings_inactive',
#         python_callable=listings_inactive_main
#     )
#
#     # Task to run rent.py
#     run_rent = PythonOperator(
#         task_id='run_rent',
#         python_callable=rent_main
#     )
#
#     # Task to run rent_inactive.py
#     run_rent_inactive = PythonOperator(
#         task_id='run_rent_inactive',
#         python_callable=rent_inactive_main
#     )
#
#     # Define the order of execution (inside the DAG block)
#     run_main_script >> run_listings_inactive >> run_rent >> run_rent_inactive
#
#
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator  # Airflow >= 2.0
from datetime import datetime, timedelta
import sys
import os

# Add the include directory to sys.path
sys.path.insert(0, os.path.abspath('/usr/local/airflow/include'))

# Import functions from each file
from main import main
from listings_inactive import listings_inactive_main
from rent import rent_main
from rent_inactive import rent_inactive_main

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('combined_python_snowflake_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    # Start task
    start_task = EmptyOperator(task_id='start_task')

    # Task to run main.py
    run_main_script = PythonOperator(
        task_id='run_main_script',
        python_callable=main
    )

    # Task to run listings_inactive.py
    run_listings_inactive = PythonOperator(
        task_id='run_listings_inactive',
        python_callable=listings_inactive_main
    )

    # Task to run rent.py
    run_rent = PythonOperator(
        task_id='run_rent',
        python_callable=rent_main
    )

    # Task to run rent_inactive.py
    run_rent_inactive = PythonOperator(
        task_id='run_rent_inactive',
        python_callable=rent_inactive_main
    )

    # Task to load DIM_PROPERTY_WITH_LOCATION
    load_dim_property_with_location = SQLExecuteQueryOperator(
        task_id='load_dim_property_with_location',
        sql='''
		MERGE INTO DIM_PROPERTY_WITH_LOCATION AS target
USING (
    WITH FILTERED_DATA AS (
        SELECT *
        FROM RAW_PROPERTY_LISTINGS
        WHERE PROPERTY_TYPE IS NOT NULL -- Exclude rows with missing PROPERTY_TYPE
          AND COUNTY IS NOT NULL -- Exclude rows with missing COUNTY
          AND LISTING_TYPE IS NOT NULL -- Exclude rows with missing LISTING_TYPE
          AND LATITUDE IS NOT NULL -- Exclude rows with missing LATITUDE
          AND LONGITUDE IS NOT NULL -- Exclude rows with missing LONGITUDE
    ),
    DEDUPLICATED_DATA AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ID, LISTING_TYPE, LATITUDE, LONGITUDE -- Deduplicate based on ID, LISTING_TYPE, and location (latitude/longitude)
                ORDER BY INGESTION_TIMESTAMP DESC
            ) AS rn
        FROM FILTERED_DATA
    )
    SELECT
        SHA2(
            COALESCE(ID, '') || -- Include ID in the hash
            COALESCE(LISTING_TYPE, '') || -- Include LISTING_TYPE in the hash
            COALESCE(CAST(LATITUDE AS STRING), '') || -- Include LATITUDE in the hash
            COALESCE(CAST(LONGITUDE AS STRING), '') || -- Include LONGITUDE in the hash
            COALESCE(PROPERTY_TYPE, '') || 
            COALESCE(CAST(BEDROOMS AS STRING), '') || 
            COALESCE(CAST(BATHROOMS AS STRING), '') || 
            COALESCE(CAST(SQUARE_FOOTAGE AS STRING), '') || 
            COALESCE(CAST(LOT_SIZE AS STRING), '') || 
            COALESCE(CAST(YEAR_BUILT AS STRING), '') ||
            COALESCE(CAST(PRICE AS STRING), ''), -- Include PRICE in the hash
            256
        ) AS PROPERTY_DIM_ID,
        ID,
        'FOR SALE'AS LISTING_TYPE, -- Include LISTING_TYPE
        COALESCE(FORMATTED_ADDRESS, '') AS FORMATTED_ADDRESS,
        COALESCE(ADDRESS_LINE1, '') AS ADDRESS_LINE1,
        COALESCE(ADDRESS_LINE2, '') AS ADDRESS_LINE2,
        COALESCE(CITY, '') AS CITY,
        COALESCE(STATE, '') AS STATE,
        COALESCE(ZIP_CODE, '') AS ZIP_CODE,
        CASE 
            WHEN UPPER(COUNTY) IN ('DALLAS', 'DALLAS COUNTY', 'TARRANT COUNTY', 'KAUFMAN COUNTY', 'ELLIS COUNTY') THEN 'DALLAS'
            WHEN UPPER(COUNTY) IN ('DENTON', 'DENTON COUNTY', 'COOKE COUNTY', 'GRAYSON COUNTY') THEN 'DENTON'
            WHEN UPPER(COUNTY) IN ('COLLIN', 'COLLIN COUNTY', 'ROCKWALL COUNTY', 'HUNT COUNTY') THEN 'COLLIN'
        -- Exclude all other counti
        END AS COUNTY,
        COALESCE(LATITUDE, 0) AS LATITUDE,
        COALESCE(LONGITUDE, 0) AS LONGITUDE,
        COALESCE(PROPERTY_TYPE, 'UNKNOWN') AS PROPERTY_TYPE,
        COALESCE(BEDROOMS, 0) AS BEDROOMS,
        COALESCE(BATHROOMS, 0.0) AS BATHROOMS,
        COALESCE(SQUARE_FOOTAGE, 0) AS SQUARE_FOOTAGE,
        COALESCE(LOT_SIZE, 0) AS LOT_SIZE,
        COALESCE(YEAR_BUILT, 0) AS YEAR_BUILT,
        COALESCE(PRICE, 0.0) AS PRICE, -- Include PRICE with a default value of 0.0
        CURRENT_TIMESTAMP() AS EFFECTIVE_START_DATE,
        '9999-12-31 23:59:59'::TIMESTAMP_NTZ(9) AS EFFECTIVE_END_DATE,
        TRUE AS IS_CURRENT
    FROM DEDUPLICATED_DATA
    WHERE rn = 1 -- Select only the first occurrence of each duplicate
) AS source
ON target.ID = source.ID 
   AND target.LISTING_TYPE = source.LISTING_TYPE -- Match on ID and LISTING_TYPE
   AND target.LATITUDE = source.LATITUDE -- Match on latitude
   AND target.LONGITUDE = source.LONGITUDE -- Match on longitude
   AND target.IS_CURRENT = TRUE
WHEN MATCHED AND (
    target.PROPERTY_TYPE <> source.PROPERTY_TYPE OR
    target.BEDROOMS <> source.BEDROOMS OR
    target.BATHROOMS <> source.BATHROOMS OR
    target.SQUARE_FOOTAGE <> source.SQUARE_FOOTAGE OR
    target.LOT_SIZE <> source.LOT_SIZE OR
    target.YEAR_BUILT <> source.YEAR_BUILT OR
    target.PRICE <> source.PRICE -- Include PRICE in the comparison
) THEN UPDATE SET
    target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
    target.IS_CURRENT = FALSE
WHEN NOT MATCHED THEN
    INSERT (PROPERTY_DIM_ID, ID, LISTING_TYPE, FORMATTED_ADDRESS, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE, ZIP_CODE, COUNTY, LATITUDE, LONGITUDE, PROPERTY_TYPE, BEDROOMS, BATHROOMS, SQUARE_FOOTAGE, LOT_SIZE, YEAR_BUILT, PRICE, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT)
    VALUES (source.PROPERTY_DIM_ID, source.ID, source.LISTING_TYPE, source.FORMATTED_ADDRESS, source.ADDRESS_LINE1, source.ADDRESS_LINE2, source.CITY, source.STATE, source.ZIP_CODE, source.COUNTY, source.LATITUDE, source.LONGITUDE, source.PROPERTY_TYPE, source.BEDROOMS, source.BATHROOMS, source.SQUARE_FOOTAGE, source.LOT_SIZE, source.YEAR_BUILT, source.PRICE, source.EFFECTIVE_START_DATE, source.EFFECTIVE_END_DATE, source.IS_CURRENT);

                ''',
        conn_id='real_estate',  # Replace with your Snowflake connection ID
    )

    # Task to load DIM_PROPERTY_WITH_LOCATION_RENT
    load_dim_property_with_location_rent = SQLExecuteQueryOperator(
        task_id='load_dim_property_with_location_rent',
        sql='''
      MERGE INTO DIM_PROPERTY_WITH_LOCATION_RENT AS target
USING (
    WITH FILTERED_DATA AS (
        SELECT *
        FROM RENTAL_PROPERTY_LISTINGS
        WHERE PROPERTY_TYPE IS NOT NULL -- Exclude rows with missing PROPERTY_TYPE
          AND COUNTY IS NOT NULL -- Exclude rows with missing COUNTY
          AND LISTING_TYPE IS NOT NULL -- Exclude rows with missing LISTING_TYPE
          AND LATITUDE IS NOT NULL -- Exclude rows with missing LATITUDE
          AND LONGITUDE IS NOT NULL -- Exclude rows with missing LONGITUDE
    ),
    DEDUPLICATED_DATA AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ID, LISTING_TYPE, LATITUDE, LONGITUDE -- Deduplicate based on ID, LISTING_TYPE, and location (latitude/longitude)
                ORDER BY INGESTION_TIMESTAMP DESC
            ) AS rn
        FROM FILTERED_DATA
    )
    SELECT
        SHA2(
            COALESCE(ID, '') || -- Include ID in the hash
            COALESCE(LISTING_TYPE, '') || -- Include LISTING_TYPE in the hash
            COALESCE(CAST(LATITUDE AS STRING), '') || -- Include LATITUDE in the hash
            COALESCE(CAST(LONGITUDE AS STRING), '') || -- Include LONGITUDE in the hash
            COALESCE(PROPERTY_TYPE, '') || 
            COALESCE(CAST(BEDROOMS AS STRING), '') || 
            COALESCE(CAST(BATHROOMS AS STRING), '') || 
            COALESCE(CAST(SQUARE_FOOTAGE AS STRING), '') || 
            COALESCE(CAST(LOT_SIZE AS STRING), '') || 
            COALESCE(CAST(YEAR_BUILT AS STRING), '') ||
            COALESCE(CAST(PRICE AS STRING), ''), -- Include PRICE in the hash
            256
        ) AS PROPERTY_DIM_ID,
        ID,
        'RENT'AS LISTING_TYPE, -- Include LISTING_TYPE
        COALESCE(FORMATTED_ADDRESS, '') AS FORMATTED_ADDRESS,
        COALESCE(ADDRESS_LINE1, '') AS ADDRESS_LINE1,
        COALESCE(ADDRESS_LINE2, '') AS ADDRESS_LINE2,
        COALESCE(CITY, '') AS CITY,
        COALESCE(STATE, '') AS STATE,
        COALESCE(ZIP_CODE, '') AS ZIP_CODE,
        CASE 
            WHEN UPPER(COUNTY) IN ('DALLAS', 'DALLAS COUNTY', 'TARRANT COUNTY', 'KAUFMAN COUNTY', 'ELLIS COUNTY') THEN 'DALLAS'
            WHEN UPPER(COUNTY) IN ('DENTON', 'DENTON COUNTY', 'COOKE COUNTY', 'GRAYSON COUNTY') THEN 'DENTON'
            WHEN UPPER(COUNTY) IN ('COLLIN', 'COLLIN COUNTY', 'ROCKWALL COUNTY', 'HUNT COUNTY') THEN 'COLLIN'
        -- Exclude all other counti
        END AS COUNTY,
        COALESCE(LATITUDE, 0) AS LATITUDE,
        COALESCE(LONGITUDE, 0) AS LONGITUDE,
        COALESCE(PROPERTY_TYPE, 'UNKNOWN') AS PROPERTY_TYPE,
        COALESCE(BEDROOMS, 0) AS BEDROOMS,
        COALESCE(BATHROOMS, 0.0) AS BATHROOMS,
        COALESCE(SQUARE_FOOTAGE, 0) AS SQUARE_FOOTAGE,
        COALESCE(LOT_SIZE, 0) AS LOT_SIZE,
        COALESCE(YEAR_BUILT, 0) AS YEAR_BUILT,
        COALESCE(PRICE, 0.0) AS PRICE, -- Include PRICE with a default value of 0.0
        CURRENT_TIMESTAMP() AS EFFECTIVE_START_DATE,
        '9999-12-31 23:59:59'::TIMESTAMP_NTZ(9) AS EFFECTIVE_END_DATE,
        TRUE AS IS_CURRENT
    FROM DEDUPLICATED_DATA
    WHERE rn = 1 -- Select only the first occurrence of each duplicate
) AS source
ON target.ID = source.ID 
   AND target.LISTING_TYPE = source.LISTING_TYPE -- Match on ID and LISTING_TYPE
   AND target.LATITUDE = source.LATITUDE -- Match on latitude
   AND target.LONGITUDE = source.LONGITUDE -- Match on longitude
   AND target.IS_CURRENT = TRUE
WHEN MATCHED AND (
    target.PROPERTY_TYPE <> source.PROPERTY_TYPE OR
    target.BEDROOMS <> source.BEDROOMS OR
    target.BATHROOMS <> source.BATHROOMS OR
    target.SQUARE_FOOTAGE <> source.SQUARE_FOOTAGE OR
    target.LOT_SIZE <> source.LOT_SIZE OR
    target.YEAR_BUILT <> source.YEAR_BUILT OR
    target.PRICE <> source.PRICE -- Include PRICE in the comparison
) THEN UPDATE SET
    target.EFFECTIVE_END_DATE = CURRENT_TIMESTAMP(),
    target.IS_CURRENT = FALSE
WHEN NOT MATCHED THEN
    INSERT (PROPERTY_DIM_ID, ID, LISTING_TYPE, FORMATTED_ADDRESS, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE, ZIP_CODE, COUNTY, LATITUDE, LONGITUDE, PROPERTY_TYPE, BEDROOMS, BATHROOMS, SQUARE_FOOTAGE, LOT_SIZE, YEAR_BUILT, PRICE, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, IS_CURRENT)
    VALUES (source.PROPERTY_DIM_ID, source.ID, source.LISTING_TYPE, source.FORMATTED_ADDRESS, source.ADDRESS_LINE1, source.ADDRESS_LINE2, source.CITY, source.STATE, source.ZIP_CODE, source.COUNTY, source.LATITUDE, source.LONGITUDE, source.PROPERTY_TYPE, source.BEDROOMS, source.BATHROOMS, source.SQUARE_FOOTAGE, source.LOT_SIZE, source.YEAR_BUILT, source.PRICE, source.EFFECTIVE_START_DATE, source.EFFECTIVE_END_DATE, source.IS_CURRENT);        
	''',
        conn_id='real_estate',  # Replace with your Snowflake connection ID
    )

    # Task to load FACT_PROPERTY_LISTINGS
    load_fact_property_listings = SQLExecuteQueryOperator(
        task_id='load_fact_property_listings',
        sql='''
		CREATE OR REPLACE TEMP TABLE TEMP_MAPPED_DATA AS 
WITH COMBINED_DATA AS (
    SELECT
        ID,
        'FOR SALE' AS LISTING_TYPE,
        PRICE,
        DAYS_ON_MARKET,
        CASE 
            WHEN UPPER(COUNTY) IN ('DALLAS', 'DALLAS COUNTY', 'TARRANT COUNTY', 'KAUFMAN COUNTY', 'ELLIS COUNTY') THEN 1
            WHEN UPPER(COUNTY) IN ('DENTON', 'DENTON COUNTY', 'COOKE COUNTY', 'GRAYSON COUNTY') THEN 2
            WHEN UPPER(COUNTY) IN ('COLLIN', 'COLLIN COUNTY', 'ROCKWALL COUNTY', 'HUNT COUNTY') THEN 3
           else 4  -- Exclude all other counties
        END AS COUNTY_ID,
        STATUS,
        LISTED_DATE,
        REMOVED_DATE,
        LAST_SEEN_DATE,
        MLS_NUMBER,
        LISTING_AGENT_NAME,
        LISTING_OFFICE_NAME,
        LATITUDE,
        LONGITUDE
    FROM DATAEXPERT_STUDENT.VARUN_PROJECT.RAW_PROPERTY_LISTINGS
    UNION ALL
    SELECT
        ID,
        'RENT' AS LISTING_TYPE,
        PRICE,
        DAYS_ON_MARKET,
         CASE 
            WHEN UPPER(COUNTY) IN ('DALLAS', 'DALLAS COUNTY', 'TARRANT COUNTY', 'KAUFMAN COUNTY', 'ELLIS COUNTY') THEN 1
            WHEN UPPER(COUNTY) IN ('DENTON', 'DENTON COUNTY', 'COOKE COUNTY', 'GRAYSON COUNTY') THEN 2
            WHEN UPPER(COUNTY) IN ('COLLIN', 'COLLIN COUNTY', 'ROCKWALL COUNTY', 'HUNT COUNTY') THEN 3
            else 4 -- Exclude all other counties
        END AS COUNTY_ID,
        STATUS,
        LISTED_DATE,
        REMOVED_DATE,
        LAST_SEEN_DATE,
        MLS_NUMBER,
        LISTING_AGENT_NAME,
        LISTING_OFFICE_NAME,
        LATITUDE,
        LONGITUDE
    FROM DATAEXPERT_STUDENT.VARUN_PROJECT.RENTAL_PROPERTY_LISTINGS
),
DEDUPLICATED_DATA AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ID, LISTING_TYPE, LATITUDE, LONGITUDE 
            ORDER BY LAST_SEEN_DATE DESC
        ) AS rn
    FROM COMBINED_DATA
)
SELECT
    CD.*,
    COALESCE(DIM.PROPERTY_DIM_ID, DIM_RENT.PROPERTY_DIM_ID) AS PROPERTY_DIM_ID
FROM DEDUPLICATED_DATA CD
LEFT JOIN DATAEXPERT_STUDENT.VARUN_PROJECT.DIM_PROPERTY_WITH_LOCATION DIM
    ON CD.ID = DIM.ID
    AND CD.LATITUDE = DIM.LATITUDE
    AND CD.LONGITUDE = DIM.LONGITUDE
LEFT JOIN DATAEXPERT_STUDENT.VARUN_PROJECT.DIM_PROPERTY_WITH_LOCATION_RENT DIM_RENT
    ON CD.ID = DIM_RENT.ID
    AND CD.LATITUDE = DIM_RENT.LATITUDE
    AND CD.LONGITUDE = DIM_RENT.LONGITUDE
WHERE CD.rn = 1;

      MERGE INTO DATAEXPERT_STUDENT.VARUN_PROJECT.FACT_PROPERTY_LISTINGS AS FPL
USING TEMP_MAPPED_DATA AS TMD
ON FPL.FACT_ID = SHA2(
        COALESCE(TMD.ID, '') || 
        COALESCE(TMD.LISTING_TYPE, '') || 
        COALESCE(CAST(TMD.LATITUDE AS VARCHAR), '') || 
        COALESCE(CAST(TMD.LONGITUDE AS VARCHAR), '') || 
        COALESCE(CAST(TMD.PRICE AS VARCHAR), '') || 
        COALESCE(CAST(TMD.DAYS_ON_MARKET AS VARCHAR), '')
    )
WHEN MATCHED AND (
    FPL.PROPERTY_DIM_ID <> TMD.PROPERTY_DIM_ID OR
    FPL.LISTING_TYPE <> TMD.LISTING_TYPE OR
    FPL.PRICE <> TMD.PRICE OR
    FPL.STATUS <> TMD.STATUS OR
    FPL.COUNTY_ID <> TMD.COUNTY_ID OR
    FPL.DAYS_ON_MARKET <> TMD.DAYS_ON_MARKET OR
    FPL.LISTED_DATE <> TMD.LISTED_DATE OR
    FPL.REMOVED_DATE <> TMD.REMOVED_DATE OR
    FPL.LAST_SEEN_DATE <> TMD.LAST_SEEN_DATE OR
    FPL.MLS_NUMBER <> TMD.MLS_NUMBER OR
    FPL.LISTING_AGENT_NAME <> TMD.LISTING_AGENT_NAME OR
    FPL.LISTING_OFFICE_NAME <> TMD.LISTING_OFFICE_NAME
) THEN
    UPDATE SET
        FPL.PROPERTY_DIM_ID = TMD.PROPERTY_DIM_ID,
        FPL.LISTING_TYPE = TMD.LISTING_TYPE,
        FPL.PRICE = TMD.PRICE,
        FPL.STATUS = TMD.STATUS,
        FPL.COUNTY_ID = TMD.COUNTY_ID,
        FPL.DAYS_ON_MARKET = TMD.DAYS_ON_MARKET,
        FPL.LISTED_DATE = TMD.LISTED_DATE,
        FPL.REMOVED_DATE = TMD.REMOVED_DATE,
        FPL.LAST_SEEN_DATE = TMD.LAST_SEEN_DATE,
        FPL.MLS_NUMBER = TMD.MLS_NUMBER,
        FPL.LISTING_AGENT_NAME = TMD.LISTING_AGENT_NAME,
        FPL.LISTING_OFFICE_NAME = TMD.LISTING_OFFICE_NAME,
        FPL.INGESTION_TIMESTAMP = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        FACT_ID,
        PROPERTY_DIM_ID,
        LISTING_TYPE,
        PRICE,
        STATUS,
        COUNTY_ID,
        DAYS_ON_MARKET,
        LISTED_DATE,
        REMOVED_DATE,
        LAST_SEEN_DATE,
        MLS_NUMBER,
        LISTING_AGENT_NAME,
        LISTING_OFFICE_NAME,
        INGESTION_TIMESTAMP
    )
    VALUES (
        SHA2(
            COALESCE(TMD.ID, '') || 
            COALESCE(TMD.LISTING_TYPE, '') || 
            COALESCE(CAST(TMD.LATITUDE AS VARCHAR), '') || 
            COALESCE(CAST(TMD.LONGITUDE AS VARCHAR), '') || 
            COALESCE(CAST(TMD.PRICE AS VARCHAR), '') || 
            COALESCE(CAST(TMD.DAYS_ON_MARKET AS VARCHAR), '')
        ),
        TMD.PROPERTY_DIM_ID,
        TMD.LISTING_TYPE,
        TMD.PRICE,
        TMD.STATUS,
        TMD.COUNTY_ID,
        TMD.DAYS_ON_MARKET,
        TMD.LISTED_DATE,
        TMD.REMOVED_DATE,
        TMD.LAST_SEEN_DATE,
        TMD.MLS_NUMBER,
        TMD.LISTING_AGENT_NAME,
        TMD.LISTING_OFFICE_NAME,
        CURRENT_TIMESTAMP()
    );
        ''',
        conn_id='real_estate',  # Replace with your Snowflake connection ID
    )

    # End task
    end_task = EmptyOperator(task_id='end_task')

    # Define task dependencies

    start_task >> [run_main_script, run_listings_inactive, run_rent,
              run_rent_inactive] >> load_dim_property_with_location >> load_dim_property_with_location_rent >> load_fact_property_listings >> end_task


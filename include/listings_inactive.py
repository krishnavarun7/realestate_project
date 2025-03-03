import hashlib
import snowflake.connector
import requests
import json
import uuid
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas





def snowflake_data_insert(data):
    connection_params = {
        "account": 'aab46027',
        "user": 'dataexpert_student',
        "password": 'DataExpert123!',
        "role": "all_users_role",
        'warehouse': 'COMPUTE_WH',
        'database': 'dataexpert_student',
        'schema': 'varun_project'
    }
    conn = snowflake.connector.connect(**connection_params)
    cursor = conn.cursor()
    staging_table_ddl = """
    CREATE OR REPLACE TABLE RAW_PROPERTY_LISTINGS_STAGE (
        ID VARCHAR(16777216),
        FORMATTED_ADDRESS VARCHAR(16777216),
        ADDRESS_LINE1 VARCHAR(16777216),
        ADDRESS_LINE2 VARCHAR(16777216),
        CITY VARCHAR(16777216),
        STATE VARCHAR(16777216),
        ZIP_CODE VARCHAR(16777216),
        COUNTY VARCHAR(16777216),
        LATITUDE FLOAT,
        LONGITUDE FLOAT,
        PROPERTY_TYPE VARCHAR(16777216),
        BEDROOMS NUMBER(38,0),
        BATHROOMS NUMBER(38,0),
        SQUARE_FOOTAGE NUMBER(38,0),
        LOT_SIZE NUMBER(38,0),
        YEAR_BUILT NUMBER(38,0),
        HOA_FEE FLOAT,
        STATUS VARCHAR(16777216),
        PRICE FLOAT,
        LISTING_TYPE VARCHAR(16777216),
        LISTED_DATE TIMESTAMP_NTZ(9),
        REMOVED_DATE TIMESTAMP_NTZ(9),
        CREATED_DATE TIMESTAMP_NTZ(9),
        LAST_SEEN_DATE TIMESTAMP_NTZ(9),
        DAYS_ON_MARKET NUMBER(38,0),
        MLS_NAME VARCHAR(16777216),
        MLS_NUMBER VARCHAR(16777216),
        LISTING_AGENT_NAME VARCHAR(16777216),
        LISTING_AGENT_PHONE VARCHAR(16777216),
        LISTING_AGENT_EMAIL VARCHAR(16777216),
        LISTING_AGENT_WEBSITE VARCHAR(16777216),
        LISTING_OFFICE_NAME VARCHAR(16777216),
        LISTING_OFFICE_PHONE VARCHAR(16777216),
        LISTING_OFFICE_EMAIL VARCHAR(16777216),
        HISTORY VARIANT,
        HASH_VALUE VARCHAR(64),
        INGESTION_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
    );
    """

    cursor.execute(staging_table_ddl)

    ingestion_id = str(uuid.uuid4())

    # Establish connection to Snowflake
    def compute_md5_hash(record):
        hash_string = f"{record.get('listing_agent_name', '')}|{record.get('status', '')}|{record.get('price', '')}|{record.get('daysOnMarket', '')}|{record.get('lastSeenDate', '')}"
        return hashlib.md5(hash_string.encode()).hexdigest()

    records = []
    try:

        for d in data:
            md5_hash = compute_md5_hash(d)
            row = {
                "ID": d.get("id", ""),
                "FORMATTED_ADDRESS": d.get("formattedAddress", ""),
                "ADDRESS_LINE1": d.get("addressLine1", ""),
                "ADDRESS_LINE2": None if d.get("addressLine2") in ["Null", None] else d.get("addressLine2"),
                "CITY": d.get("city", ""),
                "STATE": d.get("state", ""),
                "ZIP_CODE": d.get("zipCode", ""),
                "COUNTY": d.get("county", ""),
                "LATITUDE": float(d.get("latitude", 0.0)),
                "LONGITUDE": float(d.get("longitude", 0.0)),
                "PROPERTY_TYPE": d.get("propertyType", ""),
                "BEDROOMS": int(d.get("bedrooms", 0)),
                "BATHROOMS": int(d.get("bathrooms", 0)),
                "SQUARE_FOOTAGE": int(d.get("squareFootage", 0)),
                "LOT_SIZE": int(d.get("lotSize", 0)),
                "YEAR_BUILT": int(d.get("yearBuilt", 0)),
                "HOA_FEE": int(d.get("hoa", {}).get("fee", 0)),
                "STATUS": d.get("status", ""),
                "PRICE": int(d.get("price", 0)),
                "LISTING_TYPE": d.get("listingType", ""),
                "LISTED_DATE": d.get("listedDate", None) if d.get("listedDate") not in ["Null", None] else None,
                "REMOVED_DATE": d.get("removedDate", None) if d.get("removedDate") not in ["Null", None] else None,
                "CREATED_DATE": d.get("createdDate", None) if d.get("createdDate") not in ["Null", None] else None,
                "LAST_SEEN_DATE": d.get("lastSeenDate", None) if d.get("lastSeenDate") not in ["Null", None] else None,
                "DAYS_ON_MARKET": int(d.get("daysOnMarket", 0)),
                "MLS_NAME": d.get("mlsName", ""),
                "MLS_NUMBER": d.get("mlsNumber", ""),
                "LISTING_AGENT_NAME": d.get("listingAgent", {}).get("name", ""),
                "LISTING_AGENT_PHONE": d.get("listingAgent", {}).get("phone", ""),
                "LISTING_AGENT_EMAIL": d.get("listingAgent", {}).get("email", ""),
                "LISTING_AGENT_WEBSITE": d.get("listingAgent", {}).get("website", ""),
                "LISTING_OFFICE_NAME": d.get("listingOffice", {}).get("name", ""),
                "LISTING_OFFICE_PHONE": d.get("listingOffice", {}).get("phone", ""),
                "LISTING_OFFICE_EMAIL": d.get("listingOffice", {}).get("email", ""),
                "HISTORY": json.dumps(d.get("history", {})),
                "HASH_VALUE": md5_hash,  # Convert dict to JSON string
                "INGESTION_TIMESTAMP": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),  # Format for Snowflake
            }
            city = row["CITY"]
            records.append(row)
        df = pd.DataFrame(records)
        df.columns = df.columns.str.upper()

        staging_table = "RAW_PROPERTY_LISTINGS_STAGE"
        write_pandas(conn, df, "RAW_PROPERTY_LISTINGS_STAGE", auto_create_table=False)

        # Step 2: Merge Data from Staging Table into Main Table
        merge_query = f"""
        MERGE INTO RAW_PROPERTY_LISTINGS AS target
        USING RAW_PROPERTY_LISTINGS_STAGE AS source
        ON target.ID = source.ID
        WHEN MATCHED THEN
            UPDATE SET 
                target.FORMATTED_ADDRESS = source.FORMATTED_ADDRESS,
                target.ADDRESS_LINE1 = source.ADDRESS_LINE1,
                target.ADDRESS_LINE2 = source.ADDRESS_LINE2,
                target.CITY = source.CITY,
                target.STATE = source.STATE,
                target.ZIP_CODE = source.ZIP_CODE,
                target.COUNTY = source.COUNTY,
                target.LATITUDE = source.LATITUDE,
                target.LONGITUDE = source.LONGITUDE,
                target.PROPERTY_TYPE = source.PROPERTY_TYPE,
                target.BEDROOMS = source.BEDROOMS,
                target.BATHROOMS = source.BATHROOMS,
                target.SQUARE_FOOTAGE = source.SQUARE_FOOTAGE,
                target.LOT_SIZE = source.LOT_SIZE,
                target.YEAR_BUILT = source.YEAR_BUILT,
                target.HOA_FEE = source.HOA_FEE,
                target.STATUS = source.STATUS,
                target.PRICE = source.PRICE,
                target.LISTING_TYPE = source.LISTING_TYPE,
                target.LISTED_DATE = source.LISTED_DATE,
                target.REMOVED_DATE = source.REMOVED_DATE,
                target.CREATED_DATE = source.CREATED_DATE,
                target.LAST_SEEN_DATE = source.LAST_SEEN_DATE,
                target.DAYS_ON_MARKET = source.DAYS_ON_MARKET,
                target.MLS_NAME = source.MLS_NAME,
                target.MLS_NUMBER = source.MLS_NUMBER,
                target.LISTING_AGENT_NAME = source.LISTING_AGENT_NAME,
                target.LISTING_AGENT_PHONE = source.LISTING_AGENT_PHONE,
                target.LISTING_AGENT_EMAIL = source.LISTING_AGENT_EMAIL,
                target.LISTING_AGENT_WEBSITE = source.LISTING_AGENT_WEBSITE,
                target.LISTING_OFFICE_NAME = source.LISTING_OFFICE_NAME,
                target.LISTING_OFFICE_PHONE = source.LISTING_OFFICE_PHONE,
                target.LISTING_OFFICE_EMAIL = source.LISTING_OFFICE_EMAIL,
                target.HISTORY = source.HISTORY,
                target.HASH_VALUE = source.HASH_VALUE,
                target.INGESTION_TIMESTAMP = source.INGESTION_TIMESTAMP
        WHEN NOT MATCHED THEN
              INSERT (
                ID, FORMATTED_ADDRESS, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE, ZIP_CODE, COUNTY, LATITUDE, LONGITUDE,
                PROPERTY_TYPE, BEDROOMS, BATHROOMS, SQUARE_FOOTAGE, LOT_SIZE, YEAR_BUILT, HOA_FEE, STATUS, PRICE,
                LISTING_TYPE, LISTED_DATE, REMOVED_DATE, CREATED_DATE, LAST_SEEN_DATE, DAYS_ON_MARKET, MLS_NAME,
                MLS_NUMBER, LISTING_AGENT_NAME, LISTING_AGENT_PHONE, LISTING_AGENT_EMAIL, LISTING_AGENT_WEBSITE,
                LISTING_OFFICE_NAME, LISTING_OFFICE_PHONE, LISTING_OFFICE_EMAIL, HISTORY,HASH_VALUE, INGESTION_TIMESTAMP
            ) VALUES (
                source.ID, source.FORMATTED_ADDRESS, source.ADDRESS_LINE1, source.ADDRESS_LINE2, source.CITY, source.STATE, source.ZIP_CODE, source.COUNTY, source.LATITUDE, source.LONGITUDE,
                source.PROPERTY_TYPE, source.BEDROOMS, source.BATHROOMS, source.SQUARE_FOOTAGE, source.LOT_SIZE, source.YEAR_BUILT, source.HOA_FEE, source.STATUS, source.PRICE,
                source.LISTING_TYPE, source.LISTED_DATE, source.REMOVED_DATE, source.CREATED_DATE, source.LAST_SEEN_DATE, source.DAYS_ON_MARKET, source.MLS_NAME,
                source.MLS_NUMBER, source.LISTING_AGENT_NAME, source.LISTING_AGENT_PHONE, source.LISTING_AGENT_EMAIL, source.LISTING_AGENT_WEBSITE,
                source.LISTING_OFFICE_NAME, source.LISTING_OFFICE_PHONE, source.LISTING_OFFICE_EMAIL, source.HISTORY, source.HASH_VALUE, source.INGESTION_TIMESTAMP

            )
        """

        cursor = conn.cursor()
        cursor.execute(merge_query)

        print("✅ Data successfully upserted into RAW_PROPERTY_LISTINGS!")

        # Convert to Pandas DataFrame

        # Append to Snowflake table using Arrow
        #     write_pandas(conn, df, "RAW_PROPERTY_LISTINGS", auto_create_table=False)

        # Log audit entry
        audit_query = """
                INSERT INTO audit_logs_inactive (ingestion_id, source, ingestion_timestamp, record_count, status,city)
                VALUES (%s, 'API', CURRENT_TIMESTAMP, %s, 'Success',%s)
            """
        cursor.execute(audit_query, (ingestion_id, len(records),city))

        conn.commit()
        print("Data ingestion completed successfully.")

    except Exception as e:
        # Log failure
        audit_query = """
            INSERT INTO audit_logs_inactive (ingestion_id, source, ingestion_timestamp, record_count, status, error_message,city)
            VALUES (%s, 'API', CURRENT_TIMESTAMP, %s, 'Failed', %s, %s)
        """
        record_count = len(records) if records else 0

        conn.rollback()
        cursor.execute(audit_query, (ingestion_id, len(records), str(e),city))
        conn.rollback()
        print(f"Error during ingestion: {e}")

    finally:
        cursor.close()
        conn.close()


def listings_inactive_main():
        cities = [
                  "Addison", "Balch Springs", "Carrollton", "Cedar Hill", "Cockrell Hill",
                  "Combine", "Coppell", "Dallas", "DeSoto", "Duncanville", "Farmers Branch",
                  "Ferris", "Garland", "Glenn Heights", "Grand Prairie", "Grapevine",
                  "Highland Park", "Hutchins", "Irving", "Lancaster", "Lewisville",
                  "Mesquite", "Ovilla", "Richardson", "Rowlett", "Sachse", "Seagoville",
                  "Sunnyvale", "University Park", "Wilmer", "Wylie", "Argyle", "Aubrey",
                  "Bartonville", "Carrollton", "Celina", "Coppell", "Copper Canyon",
                  "Corinth", "Cross Roads", "Dallas", "Denton", "DISH", "Double Oak",
                  "Flower Mound", "Fort Worth", "Frisco", "Grapevine", "Hackberry",
                  "Haslet", "Hebron", "Hickory Creek", "Highland Village", "Justin",
                  "Krugerville", "Krum", "Lake Dallas", "Lakewood Village", "Lewisville",
                  "Little Elm", "Northlake", "Oak Point", "Paloma Creek", "Paloma Creek South",
                  "Pilot Point", "Plano", "Ponder", "Prosper", "Providence Village",
                  "Roanoke", "Sanger", "Savannah", "Shady Shores", "Southlake", "The Colony",
                  "Trophy Club", "Westlake", "Allen", "Anna", "Blue Ridge", "Carrollton",
                  "Celina", "Copeville", "Dallas", "Fairview", "Farmersville", "Frisco",
                  "Garland", "Josephine", "Lavon", "Lowry Crossing", "Lucas", "McKinney",
                  "Melissa", "Murphy", "Nevada", "New Hope", "Parker", "Plano", "Princeton",
                  "Prosper", "Richardson", "Royse City", "Sachse", "Saint Paul",
                  "Van Alstyne", "Weston", "Wylie"
                ]

        base_url = "https://api.rentcast.io/v1/listings/sale"
        # url = "https://api.rentcast.io/v1/listings/sale?city=Austin&state=TX&status=Active&limit=500&offset=500"
        # url  = "https://api.rentcast.io/v1/listings/sale?state=TX&status=Active&limit=500&offset=500"


        headers = {
            "accept": "application/json",
            "X-Api-Key": "e91305a9175044a2b1254f9caf88c728"
        }


        connection_params = {
                "account": 'aab46027',
                "user": 'dataexpert_student',
                "password": 'DataExpert123!',
                "role": "all_users_role",
                'warehouse': 'COMPUTE_WH',
                'database': 'dataexpert_student',
                'schema': 'varun_project'
            }

        audit_logs_inactive = """
                SELECT city, SUM(record_count) AS total_records
                FROM audit_logs_inactive
                WHERE city = %s
                GROUP BY city
                ORDER BY total_records DESC;
            """
        check_records = """
                        select city, record_count from (
                        SELECT city, record_count,row_number() over(partition by city order by ingestion_timestamp desc) as r
                        FROM audit_logs_inactive) t 
                        where city = %s and r = 1
        """
        audit_query = """
                      INSERT INTO audit_logs_inactive (ingestion_id, source, ingestion_timestamp, record_count, status, error_message,city)
                      VALUES (%s, 'API', CURRENT_TIMESTAMP, '0', 'Failed', 'No Data available in this city',%s )
                  """
         # Pass city as a tuple

        conn = snowflake.connector.connect(**connection_params)
        cursor = conn.cursor()


        # Convert result to DataFrame

        results=[]
        for city in cities:
            cursor.execute(audit_logs_inactive, (city,))
            result = cursor.fetchone()
            print("I am here 1")
            print(result)
            if result is None or result:
                if result is None:
                    url = f"{base_url}?city={city}&state=TX&status=Inactive&limit=500"
                    print("I am here 2")
                    response = requests.get(url, headers=headers)  # API request
                    print(response.status_code)
                    if response.status_code == 200 and response.json() != []:
                        data = response.json()
                        print(data)
                        snowflake_data_insert(data)

                        # Process or save data as needed

                    else:
                        print(f"❌ Failed to fetch data for {city}: {response.status_code}, {response.text}")

                else:
                    check = []
                    cursor.execute(check_records, (city,))
                    check =  cursor.fetchone()
                    print(check[1])
                    if check[1] < 5:
                        continue

                    else:
                        results.append({"City": result[0], "Total Records": result[1]})
                        offset = result[1]

                        url = f"{base_url}?city={city}&state=TX&status=Inactive&limit=500&offset={offset}"
                        print("I am here 3")
                        response = requests.get(url, headers=headers)  # API request
                        print(response.status_code)

                        if response.status_code == 200 and response.json() != []:
                            data = response.json()
                            print(data)
                            snowflake_data_insert(data)

                            # Process or save data as needed
                        elif(response.status_code == 200 and response.json() == []):
                            ingestion_id = str(uuid.uuid4())
                            print("I am here 4")

                            cursor.execute(audit_query, (ingestion_id, city))


                        else:
                            print(f"❌ Failed to fetch data for {city}: {response.status_code}, {response.text}")








            else:
                    results.append({"City": city, "Total Records": 0})


if __name__ == '__main__':
    listings_inactive_main()
      # Dynamic URL


# print(data)


# print(response.text)


# INSERT (
        #     ID, FORMATTED_ADDRESS, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE, ZIP_CODE, COUNTY, LATITUDE, LONGITUDE,
        #     PROPERTY_TYPE, BEDROOMS, BATHROOMS, SQUARE_FOOTAGE, LOT_SIZE, YEAR_BUILT, HOA_FEE, STATUS, PRICE,
        #     LISTING_TYPE, LISTED_DATE, REMOVED_DATE, CREATED_DATE, LAST_SEEN_DATE, DAYS_ON_MARKET, MLS_NAME,
        #     MLS_NUMBER, LISTING_AGENT_NAME, LISTING_AGENT_PHONE, LISTING_AGENT_EMAIL, LISTING_AGENT_WEBSITE,
        #     LISTING_OFFICE_NAME, LISTING_OFFICE_PHONE, LISTING_OFFICE_EMAIL, HISTORY, INGESTION_TIMESTAMP
        # ) VALUES (
        #     source.ID, source.FORMATTED_ADDRESS, source.ADDRESS_LINE1, source.ADDRESS_LINE2, source.CITY, source.STATE, source.ZIP_CODE, source.COUNTY, source.LATITUDE, source.LONGITUDE,
        #     source.PROPERTY_TYPE, source.BEDROOMS, source.BATHROOMS, source.SQUARE_FOOTAGE, source.LOT_SIZE, source.YEAR_BUILT, source.HOA_FEE, source.STATUS, source.PRICE,
        #     source.LISTING_TYPE, source.LISTED_DATE, source.REMOVED_DATE, source.CREATED_DATE, source.LAST_SEEN_DATE, source.DAYS_ON_MARKET, source.MLS_NAME,
        #     source.MLS_NUMBER, source.LISTING_AGENT_NAME, source.LISTING_AGENT_PHONE, source.LISTING_AGENT_EMAIL, source.LISTING_AGENT_WEBSITE,
        #     source.LISTING_OFFICE_NAME, source.LISTING_OFFICE_PHONE, source.LISTING_OFFICE_EMAIL, source.HISTORY, source.INGESTION_TIMESTAMP
        #
import json

import row
import snowflake.connector
import requests
import json
import uuid
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


connection_params = {
    "account": 'aab46027',
    "user": 'dataexpert_student',
    "password": 'DataExpert123!',
    "role": "all_users_role",
    'warehouse': 'COMPUTE_WH',
    'database': 'dataexpert_student',
    'schema':'varun_project'
}



url = "https://api.rentcast.io/v1/listings/sale?zipCode=75070&status=Active&limit=500"

headers = {
    "accept": "application/json",
    "X-Api-Key": "e40326c88f224fca9b40042f2fc834a3"
}

response = requests.get(url, headers=headers)
data = response.json()
print(response.text)




ingestion_id = str(uuid.uuid4())

# Establish connection to Snowflake
conn = snowflake.connector.connect(**connection_params)
cursor = conn.cursor()


records = []
try:
    for d in data:
        row = {
            "id": d.get("id", ""),
            "formatted_address": d.get("formattedAddress", ""),
            "address_line1": d.get("addressLine1", ""),
            "address_line2": None if d.get("addressLine2") == "Null" else d.get("addressLine2"),
            "city": d.get("city", ""),
            "state": d.get("state", ""),
            "zip_code": d.get("zipCode", ""),
            "county": d.get("county", ""),
            "latitude": float(d.get("latitude", 0.0)),
            "longitude": float(d.get("longitude", 0.0)),
            "property_type": d.get("propertyType", ""),
            "bedrooms": int(d.get("bedrooms", 0)),
            "bathrooms": int(d.get("bathrooms", 0)),
            "square_footage": int(d.get("squareFootage", 0)),
            "lot_size": int(d.get("lotSize", 0)),
            "year_built": int(d.get("yearBuilt", 0)),
            "hoa_fee": int(d.get("hoa", {}).get("fee", 0)),
            "status": d.get("status", ""),
            "price": int(d.get("price", 0)),
            "listing_type": d.get("listingType", ""),
            "listed_date": d.get("listedDate", ""),
            "removed_date": None if d.get("removedDate") == "Null" else d.get("removedDate"),
            "created_date": d.get("createdDate", ""),
            "last_seen_date": d.get("lastSeenDate", ""),
            "days_on_market": int(d.get("daysOnMarket", 0)),
            "mls_name": d.get("mlsName", ""),
            "mls_number": d.get("mlsNumber", ""),
            "listing_agent_name": d.get("listingAgent", {}).get("name", ""),
            "listing_agent_phone": d.get("listingAgent", {}).get("phone", ""),
            "listing_agent_email": d.get("listingAgent", {}).get("email", ""),
            "listing_agent_website": d.get("listingAgent", {}).get("website", ""),
            "listing_office_name": d.get("listingOffice", {}).get("name", ""),
            "listing_office_phone": d.get("listingOffice", {}).get("phone", ""),
            "listing_office_email": d.get("listingOffice", {}).get("email", ""),
            "history": json.dumps(d.get("history", {})),  # Convert dict to JSON string
            "INGESTION_TIMESTAMP": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),  # Convert to readable timestamp        # Auto timestamp
        }
        print(d.get("id", ""))
        records.append(row)

# Convert to Pandas DataFrame
    df = pd.DataFrame(records)
    df.columns = df.columns.str.upper()

# Append to Snowflake table using Arrow
    write_pandas(conn, df, "RAW_PROPERTY_LISTINGS", auto_create_table=False)

     # Log audit entry
    audit_query = """
            INSERT INTO audit_logs (ingestion_id, source, ingestion_timestamp, record_count, status)
            VALUES (%s, 'API', CURRENT_TIMESTAMP, %s, 'Success')
        """
    cursor.execute(audit_query, (ingestion_id, len(records)))

    conn.commit()
    print("Data ingestion completed successfully.")

except Exception as e:
    # Log failure
    audit_query = """
        INSERT INTO audit_logs (ingestion_id, source, ingestion_timestamp, record_count, status, error_message)
        VALUES (%s, 'API', CURRENT_TIMESTAMP, %s, 'Failed', %s)
    """
    record_count = len(records) if records else 0

    print(f"Error during ingestion: {e}")
    conn.rollback()
    cursor.execute(audit_query, (ingestion_id, len(records), str(e)))
    conn.rollback()
    print(f"Error during ingestion: {e}")

finally:
    cursor.close()
    conn.close()
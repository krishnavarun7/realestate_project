import hashlib
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
    'schema': 'varun_project'
}
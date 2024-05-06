import logging
import azure.functions as func
from deltalake import DeltaTable
import duckdb
import pyodbc
import struct
from azure.identity import ClientSecretCredential

import time

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    start_time = time.perf_counter()
    TENANT_ID = "<<your tenant id>>"
    # Your Service Principal App ID
    CLIENT_ID = "<<your spn app id>>" # c1dfact-d-spn
    # Your Service Principal Password
    CLIENT_SECRET = "<<your spn secret>>"
    solution_type = req.params.get('solution_type')
    sql_query = req.params.get('sql_query')

    if solution_type == "direct":
        storage_options={'tenant_id': TENANT_ID, 'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET }
        dt = DeltaTable("abfss://<<container name>>@<<storage account name>>.dfs.core.windows.net/<<delta folder name>>", storage_options=storage_options)
        parquet_read_options = {
            'coerce_int96_timestamp_unit': 'ms',  # Coerce int96 timestamps to a particular unit
        }
        pyarrow_dataset = dt.to_pyarrow_dataset(parquet_read_options=parquet_read_options)
        silver_fact_sale = duckdb.arrow(pyarrow_dataset)
        results = duckdb.query(sql_query).fetchall()

        end_time = time.perf_counter()
        execution_time = end_time - start_time

        return func.HttpResponse(str(execution_time) + "/n" + f"{results}")
    else:
        if solution_type == "synapse_serverless":
            server="<<synapse serverless name>>-ondemand.sql.azuresynapse.net"
            database="<<database name>>"
        elif solution_type == "data_copy_sql":
            server="<<SQL server name>>.database.windows.net"
            database="<database name>>"
        else:
            return func.HttpResponse("No solution selected")
            
        credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
        connstr = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database
        conn = pyodbc.connect(connstr, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

        cursor = conn.cursor()
        cursor.execute(sql_query)
        row = cursor.fetchall()

        end_time = time.perf_counter()
        execution_time = end_time - start_time

        return func.HttpResponse(str(execution_time) + "/n" + str(row))
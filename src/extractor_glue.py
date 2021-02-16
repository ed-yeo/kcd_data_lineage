import boto3
import pandas as pd
import re

def extract_glue_schema():
    client = boto3.client("glue")
    responseGetDatabases = client.get_databases()
    databaseList = responseGetDatabases['DatabaseList']

    tableMeta = list()
    for databaseDict in databaseList:
        databaseName = databaseDict['Name']
        responseGetTables = client.get_tables( DatabaseName = databaseName )
        tableList = responseGetTables['TableList']

        for tableDict in tableList:
            tableName = tableDict['Name']
            location = tableDict['StorageDescriptor']['Location']
            location = location[:-1] if location[-1] == '/' else location
            location = location.replace('/latest','')
            tableMeta.append({
                "db_name":databaseName,
                "table_name":tableName,
                "location":location})
    return tableMeta

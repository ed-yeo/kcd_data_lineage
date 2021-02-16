import boto3
import pandas as pd
import re

client = boto3.client("glue")
DATABASE_NAME = ""
dbs = client.get_databases()

# for databaseDict in dbs['DatabaseList']:
#     databaseName = databaseDict['Name']
#     print('\ndatabaseName: ' + databaseName)

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


        # location = re.findall('[a-z0-9_=/]+/?$', location)[0]

        location = location[:-1] if location[-1] == '/' else location
        location = location.replace('/latest','')
        # location_suffix = location.split('/')[-1]
        tableMeta.append({
            "db_name":databaseName,
            "table_name":tableName,
            "location":location})

df_tbs = pd.DataFrame(tableMeta)

df_tbs.to_csv("../../resources/glue_meta.csv", index=False)
# for table in tableMeta:
#     print(table)
# tbs = client.get_tables(DatabaseName="bi")
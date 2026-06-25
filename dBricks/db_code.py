## This is a code for transforming de data from full_load.raw into a unique table for the next layer of data: the bronze layer for the CLIENTS data.


import delta

def table_exists(catalog,database,table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
        .filter(f"database ='{database}' AND tableName ='{table}'")
        .count())
    return count == 1


     

catalog = "bronze"
schema = "point_system"
table_name = dbutils.widgets.get('tablename') # bronze database is in english
table_name_p = dbutils.widgets.get('table_name_p') #raw database is in portuguese
Id_field = dbutils.widgets.get('Id_field')
timestamp_field = dbutils.widgets.get('timestamp_field')
     

if not table_exists(catalog,schema,table_name):
    print('Table does not exist, creating...')
    df_full = spark.read.format("parquet").load(f"/Volumes/raw/sistema_pontos/full_load/{table_name_p}/")
    (df_full.coalesce(1)
            .write
            .format("delta")
            .saveAsTable(f"{catalog}.{schema}.{table_name}"))
else:
    print('Table already exists, ignoring full load!')
     

df_cdc = spark.read.format("parquet").load(f"/Volumes/raw/sistema_pontos/cdc/{table_name_p}/")
df_cdc.createOrReplaceTempView(f"{table_name}")

query = (f"SELECT * FROM {table_name} QUALIFY ROW_NUMBER() OVER (PARTITION BY {Id_field} ORDER BY {timestamp_field} DESC) = 1")

df_cdc_unique = spark.sql(query)
     

import delta

bronze = delta.DeltaTable.forName(spark,f'{catalog}.{schema}.{table_name}')

(bronze.alias("b") 
    .merge(df_cdc_unique.alias("d"),
    (f"b.{Id_field}= d.{Id_field}")) 
    .whenMatchedDelete(condition = "d.op = 'D'")     
    .whenMatchedUpdateAll(condition = "d.op ='U'") 
    .whenNotMatchedInsertAll(condition = "d.op = 'I' OR d.op = 'U'")
    .execute() 
    )

pip install sql-metadata
pyspark --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar


import pandas
from google.cloud import bigquery
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import  functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import os
os.system("pip install sql-metadata")

import sql_metadata
from sql_metadata import Parser
from sql_metadata import parser


spark.conf.set("viewsEnabled","true")
spark.conf.set("materializationDataset","<dataset>")
def getsql(s):
    x=[]
    x=Parser(s).tables
    return x

bqclient = bigquery.Client()
query_string="select * from `databse_karuna.INFORMATION_SCHEMA.VIEWS`"
dataframe = (bqclient.query(query_string).result().to_dataframe(create_bqstorage_client=True))

q=dataframe['view_definition'].to_list()
c=[]
for i in q:
    x=getsql(i)
    c.append(x)

dataframe['new']=c



mynewSchema = StructType([ StructField("table_catalog", StringType(), True)\
                       ,StructField("table_schema", StringType(), True)\
					   ,StructField("table_name", StringType(), True)\
					   ,StructField("view_definition", StringType(), True)\
					   ,StructField("check_option", StringType(), True)\
                       ,StructField("use_standard_sql", StringType(), True)
					   ,StructField("new", ArrayType(StringType()), True)])
					   
sparkDF2 = spark.createDataFrame(dataframe,schema=mynewSchema)

newdf=sparkDF2.select(sparkDF2['table_catalog'], sparkDF2['table_schema'],sparkDF2['table_name'],F.explode(sparkDF2['new']).alias('source_tables'))


query_string="select * from `databse_karuna.INFORMATION_SCHEMA.TABLES`"
dataframe_2 = (bqclient.query(query_string).result().to_dataframe(create_bqstorage_client=True))

df_pd=dataframe_2[['table_catalog','table_schema','table_name','table_type']]

mySchema = StructType([ StructField("table_catalog", StringType(), True)\
                       ,StructField("table_schema", StringType(), True)\
					   ,StructField("table_name", StringType(), True)\
					   ,StructField("table_type", StringType(), True)])
					   
df_table = spark.createDataFrame(df_pd,schema=mySchema)
df_tables=df_table.filter(df_table.table_type.like("%TABLE%"))



df_depend = newdf.withColumn('parent_proj_nm', split(newdf['source_tables'], '\.').getItem(0)) \
                .withColumn('parent_db_nm', split(newdf['source_tables'], '\.').getItem(1)) \
                .withColumn('parent_table_nm', split(newdf['source_tables'], '\.').getItem(2))

df_depend = df_depend.select(df_depend['table_catalog'].alias('child_proj_nm'),
df_depend['table_schema'].alias('child_db_nm'),
df_depend['table_name'].alias('child_table_nm'),
df_depend['parent_proj_nm'],
df_depend['parent_db_nm'], 
df_depend['parent_table_nm'],
F.lit(1).alias('lvl'))


final_df1=df_depend.join(df_tables,(df_tables.table_catalog == df_depend.parent_proj_nm) & \
         (df_tables.table_schema==df_depend.parent_db_nm) & \
         (df_tables.table_name==df_depend.parent_table_nm),"inner")

final_df2=final_df1.select(
df_tables['table_schema'].alias('search_db'),
df_tables['table_name'].alias('search_tbl'),
df_depend['parent_proj_nm'],
df_depend['parent_db_nm'],
df_depend['parent_table_nm'],
df_depend['child_proj_nm'],
df_depend['child_db_nm'],
df_depend['child_table_nm'],
df_depend['lvl'])

lvl=2
final_df=final_df2

while(True):
    df_loop=final_df.filter(final_df['child_table_nm'].isNotNull()).select(
    final_df['search_db'],
    final_df['search_tbl'],
    final_df['child_proj_nm'].alias('parent_proj_nm'),
    final_df['child_db_nm'].alias('parent_db_nm'),
    final_df['child_table_nm'].alias('parent_table_nm'))
    
    if not df_loop.head(1):
        break
    df_lo=df_loop.join(df_depend,(df_depend.parent_proj_nm==df_loop.parent_proj_nm) & \
              (df_depend.parent_db_nm==df_loop.parent_db_nm) & \
              (df_depend.parent_table_nm==df_loop.parent_table_nm),"left").select(
    df_loop['search_db'],
    df_loop['search_tbl'],
    df_loop['parent_proj_nm'],
    df_loop['parent_db_nm'],
    df_loop['parent_table_nm'],
    df_depend['child_proj_nm'],
    df_depend['child_db_nm'],
    df_depend['child_table_nm'],
    F.lit(lvl).alias('lvl'))
  
    df_lo=df_lo.filter(df_lo['child_table_nm'].isNotNull())
    df_lo=df_lo.distinct()
    final_df2=final_df2.unionAll(df_lo)
    final_df2.registerTempTable("impacts_table")
    final_df=spark.sql("""
    select
    search_db,
    search_tbl,
    parent_proj_nm,
    parent_db_nm,
    parent_table_nm,
    child_proj_nm,
    child_db_nm,
    child_table_nm,
    lvl from impacts_table where lvl={lvl}""".format(lvl=lvl))
    lvl+=1




bucket = "[bucket]"
spark.conf.set("temporaryGcsBucket", bucket)
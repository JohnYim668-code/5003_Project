# Databricks notebook source
folderName="Fact_Usersession_Cal"
dbSchema  = 'tempdb'
partitionField = "EVENT_DATE"

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1002_CONNECTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1003_FUNCTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1004_BATCH_NUM

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1005_MAP_TABLE_LIST

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1006_TABLE_CREATION

# COMMAND ----------

text = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
item = text.replace(dbwPath+'3_CURATED/','')

# COMMAND ----------

batchID = getBatchIdForOutput(item)
batchID = int(batchID)
# batchID = 1

# COMMAND ----------

# Define the table name and database
invalidSummaryTableName = 'tmpinvalidsummary_fact_usersession_cal'
database_name = 'tempdb'

# Set the current database context
spark.sql(f"USE {database_name}")

# Check if the table exists using SQL
table_exists = spark.sql(f"SHOW TABLES").filter(f"tableName = '{invalidSummaryTableName}'").count() > 0

# Define the known external location
external_location = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/998_temp/Fact_Usersession_Cal'

# Check if the table exists
if table_exists:
    # Drop the table safely
    spark.sql(f'DROP TABLE IF EXISTS {invalidSummaryTableName}')
    print(f"Dropped table: {invalidSummaryTableName}")

    # Delete the external data directly using the known location
    if external_location:
        dbutils.fs.rm(external_location, recurse=True)
        print(f"Deleted external data at: {external_location}")
else:
    print(f"Table does not exist: {invalidSummaryTableName}")

# COMMAND ----------

# Input
# Get Spark Table
delta_table_path = curPath_tran + 'Fact_Behaviour_Log/'
usersessionTbl = spark.read.format("delta").load(delta_table_path)\
                           .select("EVENT_DATE", "EVENT_DATETIME", "USER_SESSION")

usersessionTbl_max_min = usersessionTbl.groupBy("USER_SESSION").agg(max("EVENT_DATE").alias("EVENT_DATE"),\
                                                                    max("EVENT_DATETIME").alias("MAX_EVENT_DATETIME"),\
                                                                    min("EVENT_DATETIME").alias("MIN_EVENT_DATETIME"))

# Calculate the duration in minutes
usersessionTbl_duration = usersessionTbl_max_min.withColumn("USER_SESSION_DURATION", (expr("unix_timestamp(MAX_EVENT_DATETIME) - unix_timestamp(MIN_EVENT_DATETIME)")) / 60)

# COMMAND ----------

# behaviour Transformation
resultTbl = usersessionTbl_duration.withColumn('DATE_KEY', col('EVENT_DATE').cast(TimestampType()))\
                                   .withColumn('ETLUPDATEDDATETIME', lit(currentTimeUtc8).cast(TimestampType()))

# Create Partition Fields
resultTbl = resultTbl.withColumn('PARTITIONYEAR', year(partitionField)).withColumn('PARTITIONMONTH', month(partitionField)).withColumn('PARTITIONDAY', dayofmonth(partitionField))

# Select Columns
resultTbl = resultTbl.select('DATE_KEY', 'USER_SESSION', 'MAX_EVENT_DATETIME', 'MIN_EVENT_DATETIME', \
                             'USER_SESSION_DURATION', 'PARTITIONYEAR', 'PARTITIONMONTH', 'PARTITIONDAY', 'ETLUPDATEDDATETIME')

# COMMAND ----------

# Generate Log file in 999_log
logGen()

# COMMAND ----------

spark.catalog.dropTempView("temporary_table")
resultTbl.createTempView('temporary_table')

if spark._jsparkSession.catalog().tableExists("curated." + folderName):
    print("Exist")
else:
    print("Not Exist")
    resultTbl.filter("1!=1").write.mode("overwrite").partitionBy('PARTITIONYEAR','PARTITIONMONTH','PARTITIONDAY')\
                                  .save(curPath_tran + folderName)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO curated.fact_usersession_cal AS target
# MAGIC     USING temporary_table source
# MAGIC   ON target.DATE_KEY = source.DATE_KEY AND target.USER_SESSION = source.USER_SESSION
# MAGIC   WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# # Delta Record
# delta_table_path = curPath_raw + '/Transaction/Fact_Behaviour_Log/'
# behaviourTbl = spark.read.format("delta").load(delta_table_path)
# outputDeltaTran(behaviourTbl, curPath_raw + "/Transaction/Fact_Behaviour_Log/") 

# COMMAND ----------

completeBatch(item, batchID)

# COMMAND ----------

# %sql
# select * from tempdb.tmpbatchfilesummary

# COMMAND ----------

# # Load your Delta table
# delta_table_path = "abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/0_config/batch_summary/batch_summary_temp"
# spark.sql("CREATE TABLE IF NOT EXISTS tempdb.tmpbatchfilesummary USING DELTA LOCATION '{}'".format(delta_table_path))

# # Update records in the Delta table
# spark.sql("""
#     DELETE FROM tempdb.tmpbatchfilesummary
#     WHERE item = 'NB_3002_CUR_Fact_behaviour_log'
# """)

# COMMAND ----------

# # Load your Delta table
# delta_table_path = "abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Usersession_Cal"
# usersessionTbl = spark.read.format("delta").load(delta_table_path)
# # usersessionTbl.display()
# result_df = usersessionTbl.groupBy("PartitionYear", "PartitionMonth").agg(avg("USER_SESSION_DURATION").alias("AverageSessionDuration"))
# result_df.display()

# COMMAND ----------

# # Load your Delta table
# delta_table_path = "abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Usersession_Cal"
# usersessionTbl = spark.read.format("delta").load(delta_table_path)

# usersessionTbl.agg(avg("USER_SESSION_DURATION").alias("AverageSessionDuration")).collect()

# COMMAND ----------



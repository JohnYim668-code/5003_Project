# Databricks notebook source
folderName="Fact_Behaviour_Log"
dbSchema  = 'tempdb'
partitionField = "EVENT_DATETIME"

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

# COMMAND ----------

# Define the table name and database
invalidSummaryTableName = 'tmpinvalidsummary_fact_behaviour_log'
database_name = 'tempdb'

# Set the current database context
spark.sql(f"USE {database_name}")

# Check if the table exists using SQL
table_exists = spark.sql(f"SHOW TABLES").filter(f"tableName = '{invalidSummaryTableName}'").count() > 0

# Define the known external location
external_location = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/998_temp/Fact_Behaviour_Log'

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
delta_table_path = historyPath + 'behaviour_log/'
behaviourTbl = spark.read.format("delta").load(delta_table_path)\
                         .drop("CATEGORY_ID", "CATEGORY_CODE", "BRAND", "PARTITIONYEAR", "PARTITIONMONTH", "PARTITIONDAY")\
                         .withColumn('ETLUPDATEDDATETIME', lit(currentTimeUtc8).cast(TimestampType()))  

# Get Current BatchId
MaxId = behaviourTbl.select([max("BATCHNUMBER")]).collect()[0][0]
behaviourTbl = behaviourTbl.filter(col("BATCHNUMBER") == MaxId)

# Null checking for behaviourTbl
data = checkNull(dateStr, 'history.behaviour_log', 'EVENT_TIME,USER_ID,USER_SESSION', 'EVENT_TIME,USER_ID,USER_SESSION')

if MaxId < int(batchID):
    completeBatch(item, batchID)
    dbutils.notebook.exit("no existing record")

if data is not None:
    # Update/Create Temp Table
    validUpdate(dbSchema, data, folderName, storagePath)

    # Filter by BatchId
    behaviourTbl = behaviourTbl.filter("EVENT_TIME is not null and USER_SESSION is not null and USER_ID is not null")

# Add action item column
# Convert event_time to timestamp
behaviourTbl = behaviourTbl.withColumn("EVENT_TIME", col("EVENT_TIME").cast("timestamp"))

# Define a Window specification for identifying duplicates
window_spec = Window.partitionBy("USER_ID", "USER_SESSION", "EVENT_TIME", "EVENT_TYPE", "PRODUCT_ID").orderBy("EVENT_TIME")

# Add a row number to identify duplicates
behaviourTbl = behaviourTbl.withColumn("ROW_NUM", row_number().over(window_spec))

# Keep only one record for each duplicate set where EVENT_TYPE is "view"
behaviourTbl = behaviourTbl.filter((col("ROW_NUM") == 1) | (col("EVENT_TYPE") != "view")).drop("ROW_NUM") 

# Continue with adding ACTION_ITEMS for all events
window_spec_action = Window.partitionBy("USER_ID", "USER_SESSION").orderBy("EVENT_TIME")

# Add ACTION_ITEMS based on ranking for all events
behaviourTbl = behaviourTbl.withColumn(
    "ACTION_ITEMS",
    row_number().over(window_spec_action)
)

# Duplicate Checking for behaviourTbl
data, dupKey = checkDuplicate(dateStr,'Fact','EVENT_TIME','history.behaviour_log',behaviourTbl,'EVENT_TIME,EVENT_TYPE,USER_ID,ACTION_ITEMS', 'EVENT_TIME,EVENT_TYPE,USER_ID,ACTION_ITEMS', ([('EVENT_TIME'), ('EVENT_TYPE'),('USER_ID'),('ACTION_ITEMS')]))
if data is not None:
    validUpdate(dbSchema, data, folderName, storagePath)
if len(dupKey.head(1)) > 0:
     behaviourTbl = dupKey 

# COMMAND ----------

# behaviour Transformation
behaviour_Trans = behaviourTbl.withColumn('ETLUPDATEDDATETIME', lit(currentTimeUtc8).cast(TimestampType()))\
                              .withColumn('BEHAVIOUR_KEY', xxhash64(concat(col('EVENT_TIME'), lit('|'), col('EVENT_TYPE'), lit('|'), col('PRODUCT_ID'), lit('|'), col('USER_ID'), lit('|'), col('USER_SESSION'), lit('|'), col('ACTION_ITEMS'))))\
                              .withColumn('PRODUCT_ID_FK', xxhash64(col('PRODUCT_ID')))

# Extract date and time from EVENT_TIME
behaviour_Trans = behaviour_Trans.withColumnRenamed('EVENT_TIME', 'EVENT_DATETIME')\
                                 .withColumn("EVENT_DATETIME", to_timestamp(col("EVENT_DATETIME")))
behaviour_Trans = behaviour_Trans.withColumn("EVENT_DATE", date_format(col("EVENT_DATETIME"), "yyyy-MM-dd")) \
                                 .withColumn("EVENT_TIME", date_format(col("EVENT_DATETIME"), "HH:mm:ss")) \
                                 .withColumn("EVENT_WEEKDAY", dayofweek(col("EVENT_DATETIME")))\
                                 .withColumn("PRICE", col('PRICE').cast(DoubleType()))\
                                 .withColumn("SALES_BRANCH", lit('America'))\
                                 .withColumn('ACTION_ITEMS', col('ACTION_ITEMS').cast(IntegerType()))

# Define the list of event types to filter out?
event_types_to_remove = ['purchase']
# Filter out rows where event_type is in the list and price <= 0
behaviour_Trans = behaviour_Trans.filter(~((behaviour_Trans.EVENT_TYPE.isin(event_types_to_remove)) & (behaviour_Trans.PRICE <= 0)))

# Create Partition Fields
behaviour_Trans = behaviour_Trans.withColumn('PARTITIONYEAR', year((partitionField))).withColumn('PARTITIONMONTH', month((partitionField))).withColumn('PARTITIONDAY', dayofmonth((partitionField)))

# Select Columns
behaviour_Trans = behaviour_Trans.select('BEHAVIOUR_KEY', 'PRODUCT_ID_FK', 'EVENT_DATETIME', 'EVENT_DATE', 'EVENT_TIME', 'EVENT_WEEKDAY', \
                                         'EVENT_TYPE', 'PRODUCT_ID', 'PRICE', 'USER_ID', 'USER_SESSION', 'ACTION_ITEMS', 'SALES_BRANCH', 'PARTITIONYEAR', 'PARTITIONMONTH', 'PARTITIONDAY', 'ETLUPDATEDDATETIME')

print(behaviour_Trans.count())

# COMMAND ----------

# Generate Log file in 999_log
logGen()

# COMMAND ----------

spark.catalog.dropTempView("temporary_table")
behaviour_Trans.createTempView('temporary_table')

if spark._jsparkSession.catalog().tableExists("curated." + folderName):
    print("Exist")
else:
    print("Not Exist")
    behaviour_Trans.filter("1!=1").write.mode("overwrite").partitionBy('PARTITIONYEAR','PARTITIONMONTH','PARTITIONDAY')\
                                  .save(curPath_tran + folderName)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO curated.Fact_behaviour_log AS target
# MAGIC     USING temporary_table source
# MAGIC   ON target.EVENT_DATETIME = source.EVENT_DATETIME AND target.EVENT_TYPE = source.EVENT_TYPE AND target.USER_ID = source.USER_ID AND target.USER_SESSION = source.USER_SESSION AND target.ACTION_ITEMS = source.ACTION_ITEMS
# MAGIC   WHEN MATCHED AND target.EVENT_DATETIME <= source.EVENT_DATETIME THEN
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

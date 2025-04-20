# Databricks notebook source
folderName="Dim_Product"
dbSchema  = 'tempdb'

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1002_CONNECTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1003_FUNCTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1004_BATCH_NUM

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1005_TABLE_CREATION

# COMMAND ----------

text = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
item = text.replace(dbwPath+'3_CURATED/','')

# COMMAND ----------

batchID = getBatchIdForOutput(item)
batchID = int(batchID)

# COMMAND ----------

# Check if Notebook Completed its Latest Batch/Dependency
CheckRun(['Behaviour_Log'])

# COMMAND ----------

# Define the table name and database
invalidSummaryTableName = 'tmpinvalidsummary_dim_product'
database_name = 'tempdb'

# Set the current database context
spark.sql(f"USE {database_name}")

# Check if the table exists using SQL
table_exists = spark.sql(f"SHOW TABLES").filter(f"tableName = '{invalidSummaryTableName}'").count() > 0

# Define the known external location
external_location = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/998_temp/Dim_Product'

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
productTbl = spark.read.format("delta").load(delta_table_path)\
                         .select("PRODUCT_ID", "CATEGORY_ID", "CATEGORY_CODE", "BRAND", "BATCHNUMBER")\
                         .withColumn('ETLUPDATEDDATETIME', lit(currentTimeUtc8).cast(TimestampType()))  

# Get Current BatchId
MaxId = productTbl.select([max("BATCHNUMBER")]).collect()[0][0]
productTbl = productTbl.filter(col("BATCHNUMBER") == MaxId)

# Null checking for productTbl
data = checkNull(dateStr, 'history.behaviour_log', 'PRODUCT_ID', 'PRODUCT_ID')

if MaxId < int(batchID):
    completeBatch(item, batchID)
    dbutils.notebook.exit("no existing record")

if data is not None:
    # Update/Create Temp Table
    validUpdate(dbSchema, data, folderName, storagePath)

    # Filter by BatchId
    productTbl = productTbl.filter("PRODUCT_ID is not null")

# Create distinct rows based on column "Product_ID"
productTbl = productTbl.dropDuplicates(["PRODUCT_ID"])

# Duplicate Checking for productTbl
data, dupKey = checkDuplicate(dateStr,'Dimension','ETLUPDATEDDATETIME','history.behaviour_log',productTbl,'PRODUCT_ID', 'PRODUCT_ID', ([('PRODUCT_ID')]))
if data is not None:
    validUpdate(dbSchema, data, folderName, storagePath)
if len(dupKey.head(1)) > 0:
    productTbl = dupKey

# COMMAND ----------

# product Transformation
# Split CATEGORY_CODE beased on delimiter
split_columns = productTbl.select(
    "*",
    split(col("CATEGORY_CODE"), r'\.').alias("split_CATEGORY_CODE")
)

splitted_columns = split_columns.select(
    col("PRODUCT_ID"), col("CATEGORY_ID"), col("BRAND"), col("ETLUPDATEDDATETIME"),
    col("split_CATEGORY_CODE").getItem(0).alias("CATEGORY"),
    col("split_CATEGORY_CODE").getItem(1).alias("SUBCATEGORY1"),
    col("split_CATEGORY_CODE").getItem(2).alias("SUBCATEGORY2"),
    col("split_CATEGORY_CODE").getItem(3).alias("SUBCATEGORY3")
)
## *If only one record contain SUBCATEGORY3 still do?*
# Fill "N/A" for values that are null
fillNa_splitted_columns = splitted_columns.fillna("N/A", subset=['CATEGORY', 'SUBCATEGORY1', 'SUBCATEGORY2', 'SUBCATEGORY3', 'BRAND'])

# Primary Key Transformation
product_Trans = fillNa_splitted_columns.withColumn('PRODUCT_ID_KEY', xxhash64(col('PRODUCT_ID')))

# Select Columns 
product_Trans = product_Trans.select('PRODUCT_ID_KEY', 'PRODUCT_ID', 'CATEGORY_ID', 'CATEGORY', 'SUBCATEGORY1', 'SUBCATEGORY2', 'SUBCATEGORY3', 'BRAND', 'ETLUPDATEDDATETIME')

# COMMAND ----------

# Generate Log file in 999_log
logGen()

# COMMAND ----------

spark.catalog.dropTempView("temporary_table")
product_Trans.createTempView('temporary_table')

if spark._jsparkSession.catalog().tableExists("curated." + folderName):
    print("Exist")
    # product_Trans.filter("1!=1").write.mode("append").option("mergeSchema", "true").save(curPath_dim + folderName)
else:
    print("Not Exist")
    product_Trans.filter("1!=1").write.mode("overwrite").save(curPath_dim + folderName)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO curated.dim_product AS target
# MAGIC     USING temporary_table source
# MAGIC   ON target.PRODUCT_ID = source.PRODUCT_ID
# MAGIC   WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# Delta Record
delta_table_path = curPath_raw + '/Dimension/Dim_Product/'
productTbl = spark.read.format("delta").load(delta_table_path)
outputDeltaDim(productTbl, curPath_raw + "/Dimension/Dim_Product/") 
spark.sql("OPTIMIZE curated.dim_product")

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
#     DELETE from  tempdb.tmpbatchfilesummary
#     WHERE item = 'NB_3001_CUR_Dim_Product'
# """)

# COMMAND ----------



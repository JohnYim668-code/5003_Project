# Databricks notebook source
folderName="Fact_Target"
dbSchema  = 'tempdb'
partitionField = 'ETLUPDATEDDATETIME'

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1002_CONNECTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1003_FUNCTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1004_BATCH_NUM

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1006_TABLE_CREATION

# COMMAND ----------

text = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
item = text.replace(dbwPath+'3_CURATED/','')

# COMMAND ----------

batchID = getBatchIdForOutput(item)
# id = str(id)

# COMMAND ----------

# Define the table name and database
invalidSummaryTableName = 'tmpinvalidsummary_fact_target'
database_name = 'tempdb'

# Set the current database context
spark.sql(f"USE {database_name}")

# Check if the table exists using SQL
table_exists = spark.sql(f"SHOW TABLES").filter(f"tableName = '{invalidSummaryTableName}'").count() > 0

# Define the known external location
external_location = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/998_temp/Fact_Target'

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

# Get Spark Table
delta_table_path = historyPath + 'sales_target/'
targetTbl = spark.read.format("delta").load(delta_table_path)\
                      .withColumn('ETLUPDATEDDATETIME', lit(currentTimeUtc8).cast(TimestampType()))  
                         
# Target File get Current Batch ID
MaxId = targetTbl.select([max("BATCHNUMBER")]).collect()[0][0]
targetTbl = targetTbl.filter(col('BATCHNUMBER') == MaxId)

# Trim all columns
targetTbl = targetTbl.select([trim(col(column)).alias(column) for column in targetTbl.columns]) 

# Null checking for targetTbl
data = checkNull(dateStr, 'history.sales_target', 'COUNTRY,YEARMONTH', 'COUNTRY,YEARMONTH')

if MaxId < int(batchID):
    completeBatch(item, batchID)
    dbutils.notebook.exit("no existing record")

if data is not None:
    # Update/Create Temp Table
    validUpdate(dbSchema, data, folderName, storagePath)
    
    # Filter by BatchId
    targetTbl = targetTbl.filter("COUNTRY is not null and YEARMONTH is not null")

# Duplicate Checking for targetTbl
data, dupKey = checkDuplicate(dateStr,'Fact','ETLUPDATEDDATETIME','history.sales_target',targetTbl,'COUNTRY,YEARMONTH', 'COUNTRY,YEARMONTH', ([('COUNTRY'), ('YEARMONTH')]))
if data is not None:
    validUpdate(dbSchema, data, folderName, storagePath)
if len(dupKey.head(1)) > 0:
     behaviourTbl = dupKey 

# COMMAND ----------

# Add column
TargetETLTbl = targetTbl.withColumn('MONTHNUMBER', substring(col('YEARMONTH'), 5, 2))\
                        .withColumn('YEAR', substring(col('YEARMONTH'), 1, 4))\
                        .withColumn('TARGET', col('TARGET').cast(DoubleType()))

# Convert the "Year" and "MonthNumber" columns to appropriate data types  
TargetETLTbl = TargetETLTbl.withColumn("YEAR", TargetETLTbl["YEAR"].cast("integer"))
TargetETLTbl = TargetETLTbl.withColumn("MONTHNUMBER", TargetETLTbl["MONTHNUMBER"].cast("integer"))

# Calculate the number of days in each month  
TargetETLTbl = TargetETLTbl.withColumn("LASTDAYOFMONTH", last_day(expr("to_date(concat(YEAR, '-', MONTHNUMBER, '-01'))")))

# Calculate the number of days in each month  
TargetETLTbl = TargetETLTbl.withColumn("DAYSINMONTH", dayofmonth(col("LASTDAYOFMONTH")))
  
# Calculate the daily target by dividing the "Target" column by the number of days in each month  
TargetETLTbl = TargetETLTbl.withColumn("DAILYTARGET", col('TARGET')/col('DAYSINMONTH'))
  
# Generate a sequence of dates based on the number of days in each month  
TargetETLTbl = TargetETLTbl.withColumn("DAY", expr(f"sequence(to_date(concat(YEAR, '-', MONTHNUMBER, '-01')), last_day(to_date(concat(YEAR, '-', MONTHNUMBER, '-01'))))"))
  
# Explode the "Day" column to duplicate the records  
TargetETLTbl = TargetETLTbl.withColumn("DAY", explode(TargetETLTbl["DAY"]))
  
# Extract the Year, Month, and Day from the exploded "Day" column  
TargetETLTbl = TargetETLTbl.withColumn("YEAR", year(TargetETLTbl["DAY"]))
TargetETLTbl = TargetETLTbl.withColumn("MONTH", month(TargetETLTbl["DAY"]))
TargetETLTbl = TargetETLTbl.withColumn("DAY", dayofmonth(TargetETLTbl["DAY"]))

# Concatenate the Year, Month, and Day columns to create the "Date" column  
TargetETLTbl = TargetETLTbl.withColumn("DATE", expr("concat(YEAR,'-', lpad(MONTH, 2, '0'),'-', lpad(DAY, 2, '0'))").cast(DateType()))

# COMMAND ----------

# Transformation
resultTbl = TargetETLTbl.withColumn('TARGET_KEY', xxhash64(concat_ws('|', 'COUNTRY', 'DATE')))\
                        .withColumn('YEARMONTH', col('YEARMONTH').cast(IntegerType()))\
                        .withColumn('ETLUPDATEDDATETIME', lit(currentTimeUtc8).cast(TimestampType()))\
                        .withColumn('PARTITIONYEAR',year((partitionField)))\
                        .withColumn('PARTITIONMONTH',month((partitionField)))\
                        .withColumn('PARTITIONDAY',dayofmonth((partitionField)))

resultTbl = resultTbl.select('TARGET_KEY','COUNTRY','DATE','YEARMONTH','YEAR','MONTH','DAY','DAILYTARGET','TARGET','PARTITIONYEAR', 'PARTITIONMONTH', 'PARTITIONDAY', 'ETLUPDATEDDATETIME')

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

year = resultTbl.select(col('Year')).distinct()
year_list=year.select(collect_list('year')).collect()[0][0]
year_list_str = ', '.join(f"'{year}'" for year in year_list)
print(year_list_str)

# COMMAND ----------

# Load your Delta table
delta_table_path = "abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Target"
spark.sql("CREATE TABLE IF NOT EXISTS curated.fact_target USING DELTA LOCATION '{}'".format(delta_table_path))

# Update records in the Delta table
query = f"DELETE FROM curated.fact_target WHERE Year IN ({year_list_str})"
# Execute the query
result = spark.sql(query)

# COMMAND ----------

 # Repartition to increase parallelism
resultTbl = resultTbl.repartition(128)  # Adjust based on cluster capacity

# Configure Spark settings
spark.conf.set("spark.sql.shuffle.partitions", "128")  # Increase shuffle partitions

resultTbl.write \
         .partitionBy('PARTITIONYEAR','PARTITIONMONTH','PARTITIONDAY')\
         .format("delta")\
         .mode("append")\
         .save(delta_table_path)

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
#     WHERE item = '/Users/kkjyim@connect.ust.hk/BDC/3_CURATED/NB_3003_CUR_Fact_Target'
# """)

# COMMAND ----------



# Databricks notebook source
# dbutils.widgets.text("tableName", "sales_target") 
# folderName = dbutils.widgets.get("tableName")
folderName = 'sales_target'
fileType = "csv"
fileName = folderName + '.' + fileType

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
item = text.replace(dbwPath+'2_HISTORY/','')

# COMMAND ----------

# BatchID Indication
batchID = getBatchIdForOutput(item)
# id = str(id)

# COMMAND ----------

#input
df = readfile(rawPath_user, fileType, fileName)

# COMMAND ----------

# Add BatchNumber Column
df = df.withColumn('BatchNumber', lit(batchID))
df = df.withColumn("BatchNumber", df.BatchNumber.cast(IntegerType()))\
       .withColumn("YearMonth", df.YearMonth.cast(IntegerType()))\
       .withColumn("Target", df.Target.cast(DoubleType()))\
       .withColumn('PartitionYear',year(lit(log_datetimeUTC)))\
       .withColumn('PartitionMonth',month(lit(log_datetimeUTC)))\
       .withColumn('PartitionDay',dayofmonth(lit(log_datetimeUTC)))

# COMMAND ----------

# Changing to upper for all columns
df = columnToUpperCase(df)

# COMMAND ----------

# output
outputDelta(df, historyPath + '/' + folderName + '/')
# Optimize the Delta table after writing
spark.sql("OPTIMIZE history.sales_target")
# if ((folderName != 'rfidpcl_machine') and (folderName != 'rfidpcl_user')):
#     outputParquet(df, historyPath + '/' + folderName + '/', folderName + '_' + region)
# else:
#     outputParquetOverwrite(df, historyPath + '/' + folderName + '/', folderName + '_' + region)

# COMMAND ----------

# Create or Append to table hist.<folderName>
# createHistTable(df, folderName)

# COMMAND ----------

completeBatch(item, batchID)

# COMMAND ----------

# %sql
# select * from tempdb.tmpbatchfilesummary;

# COMMAND ----------

# # Load your Delta table
# delta_table_path = "abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/0_config/batch_summary/batch_summary_temp"
# spark.sql("CREATE TABLE IF NOT EXISTS tempdb.tmpbatchfilesummary USING DELTA LOCATION '{}'".format(delta_table_path))

# # Update records in the Delta table
# spark.sql("""
#     DELETE FROM tempdb.tmpbatchfilesummary
#     WHERE item = 'NB_2004_HIST_Sales_Target'
# """)

# COMMAND ----------



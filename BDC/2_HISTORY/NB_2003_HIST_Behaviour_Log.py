# Databricks notebook source
# dbutils.widgets.text("tableName", "behaviour_log") 
# folderName = dbutils.widgets.get("tableName")
folderName = 'behaviour_log'
partitionField = "EVENT_TIME"
fileType = "csv"
fileName = folderName + '.' + fileType

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
item = text.replace(dbwPath+'2_HISTORY/','')

# COMMAND ----------

# BatchID Indication
batchID = getBatchIdForOutput(item)
id = str(id)

# COMMAND ----------

#input
df = readfile(rawPath + '1_landing/' + dateStr + '/', fileType, fileName)

# COMMAND ----------

# Add BatchNumber Column
df = df.withColumn('BatchNumber', lit(batchID))
df = df.withColumn("BatchNumber", df.BatchNumber.cast(IntegerType()))
df = df.withColumn("EVENT_TIME", col("EVENT_TIME").cast("timestamp"))\
       .withColumn('PARTITIONYEAR', year((partitionField))).withColumn('PARTITIONMONTH', month((partitionField))).withColumn('PARTITIONDAY', dayofmonth((partitionField)))

# COMMAND ----------

# Changing to upper for all columns
df = columnToUpperCase(df)

# COMMAND ----------

# output
outputDelta(df, historyPath + '/' + folderName + '/')
# Optimize the Delta table after writing
spark.sql("OPTIMIZE history.behaviour_log")
# if ((folderName != 'rfidpcl_machine') and (folderName != 'rfidpcl_user')):
#     outputParquet(df, historyPath + '/' + folderName + '/', folderName + '_' + region)
# else:
#     outputParquetOverwrite(df, historyPath + '/' + folderName + '/', folderName + '_' + region)

# COMMAND ----------

# Create or Append to table hist.<folderName>
# createHistTable(df, folderName)

# COMMAND ----------

# # insert row count to temp table
# batchId = int(batchID)
# checkRowCount(df, folderName, batchId, region, 'history')

# COMMAND ----------

# batchId = int(id)
# print(id)
# print(type(batchid))

# COMMAND ----------

# tableName = "tempdb.tmpBatchFileSummary" + 'hongkong'
# batchTemp = sqlContext.sql('SELECT * FROM {0}'.format(tableName))

# COMMAND ----------

completeBatch(item, batchID)

# COMMAND ----------

# %sql
# select * from tempdb.tmpsourcefileexistsummary;

# COMMAND ----------

# %sql
# select * from tempdb.tmpbatchfilesummary;

# COMMAND ----------



# Databricks notebook source
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

# MAGIC %run ../1_COMMON/NB_1005_TABLE_CREATION

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

# COMMAND ----------

completeBatch(item, batchID)

# COMMAND ----------



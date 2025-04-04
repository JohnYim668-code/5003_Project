# Databricks notebook source
dbutils.widgets.text("tableName", "behaviour_log") 
folderName = dbutils.widgets.get("tableName")

fileType = "csv"
layer = "history"
fileName = folderName + '.' + fileType
reportName=''
newFolderName=''

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1002_CONNECTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1003_FUNCTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1004_BATCH_NUM

# COMMAND ----------

text = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
item = text.replace(dbwPath+'2_HIST/','')

# COMMAND ----------

fileName

# COMMAND ----------

# BatchID Indication
batchID = getBatchIdForOutput(folderName)
# id = str(id)

# COMMAND ----------

#input
df = readfile(rawPath + '/', fileType, fileName, layer)
display(df)

# COMMAND ----------

# Add BatchNumber Column
df = df.withColumn('BatchNumber', lit(batchID))
df = df.withColumn("BatchNumber", df.BatchNumber.cast(IntegerType()))\
       .withColumn('PartitionYear', year(lit(today_Datetime))).withColumn('PartitionMonth', month(lit(today_Datetime))).withColumn('PartitionDay', dayofmonth(lit(today_Datetime)))\

display(df)

# COMMAND ----------

# Changing to lowercase for all columns
df = columnToUpperCase(df)

# COMMAND ----------

# output
if ((folderName != 'rfidpcl_machine') and (folderName != 'rfidpcl_user')):
    outputParquet(df, historyPath + '/' + folderName + '/', folderName + '_' + region)
else:
    outputParquetOverwrite(df, historyPath + '/' + folderName + '/', folderName + '_' + region)

# COMMAND ----------

# Create or Append to table hist.<folderName>
# createHistTable(df, folderName)

# COMMAND ----------

# insert row count to temp table
batchId = int(batchID)
checkRowCount(df, folderName, batchId, region, 'history')

# COMMAND ----------

# batchId = int(id)
# print(id)
# print(type(batchid))

# COMMAND ----------

# tableName = "tempdb.tmpBatchFileSummary" + 'hongkong'
# batchTemp = sqlContext.sql('SELECT * FROM {0}'.format(tableName))

# COMMAND ----------

completeBatch(folderName, batchId)

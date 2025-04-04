# Databricks notebook source
fileName = 'tempdb.tmpInvalidSummary'
folderName = ''

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1002_CONNECTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1004_BATCH_NUM

# COMMAND ----------

item = 'Start'

#get current batch id
id = int(getBatchId(item))
id = str(id)
print(id)

delta_table_path = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/0_config/batch_summary/batch_summary_temp'
BatchFileSummary = spark.read.format("delta").load(delta_table_path)
BatchFileSummary = BatchFileSummary.orderBy(BatchFileSummary.startTime)

# COMMAND ----------

#get current batch still running
currentBatch_running = BatchFileSummary.filter((BatchFileSummary.batchID==id) & (BatchFileSummary.status=='running') & (BatchFileSummary.item!=item))

if currentBatch_running.count()==0:
  # complete batch
  completeBatch(item, id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tempdb.tmpBatchFileSummary

# COMMAND ----------



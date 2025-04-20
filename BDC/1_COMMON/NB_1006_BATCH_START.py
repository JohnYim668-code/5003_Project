# Databricks notebook source
fileName = 'tempdb.tmpInvalidSummary'
folderName = ''

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1001_CONFIG

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

# start batch
id = getBatchIdForOutput('Start')

# COMMAND ----------

# %sql
# select * from tempdb.tmpBatchFileSummary order by batchID DESC, startTime DESC;

# COMMAND ----------



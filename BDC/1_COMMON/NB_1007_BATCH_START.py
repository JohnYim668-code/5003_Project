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

# MAGIC %sql
# MAGIC --INSERT INTO tempdb.tmpBatchFileSummary VALUES ('25', '20220627', '2022-07-27 21:18:24.714053', 'Start', 'running')

# COMMAND ----------

# %sql
# delete from tempdb.tmpBatchFileSummary 
# where batchID=2 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tempdb.tmpBatchFileSummary order by batchID DESC, startTime DESC;

# COMMAND ----------

# %sql 
# drop table IF EXISTS tempdb.tmpbatchfilesummary;
# drop table IF EXISTS tempdb.tmpsourcefileexistSummary;

# --tableName = 'tempdb.tmpsourcefileexistSummary'
# --sqlContext.sql('Drop table if exists {0}'.format(tableName))

# COMMAND ----------

# BatchFileSummary = spark.table('tempdb.tmpBatchFileSummary')
# BatchFileSummary = BatchFileSummary.orderBy(BatchFileSummary.startTime)

# data_location = logPath + 'DBWBatchFileSummary'

# #output & combine files into 1
# BatchFileSummary.filter(BatchFileSummary.batchID==id).coalesce(1).write.mode("overwrite").csv(data_location,header=True)
# renamePartitionFile(data_location,'csv')

# COMMAND ----------



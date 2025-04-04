# Databricks notebook source
# MAGIC %run ../1_COMMON/NB_1001_CONFIG

# COMMAND ----------

#File path setting
blobStorage = storageAccount + '.dfs.core.windows.net'
storagePath = 'abfss://' + storageContainer +'@' + storageAccount + '.dfs.core.windows.net/'
rawPath = 'abfss://' + storageContainer +'@' + storageAccount + '.dfs.core.windows.net/'
lastYearMonthPath = '/' + lastMonth_Year + '/' + lastMonth_Month + '/'
lastYearMonthDayPath = lastYearMonthPath + lastMonth_Day + '/'
yearMonthPath = '/' + year + '/' + month + '/'
yearMonthDayPath = yearMonthPath + day + '/'
logPath = storagePath+'999_log/batch_summary/' + log_datetime+'/'
historyPath_raw = storagePath + '2_history/'
historyPath = historyPath_raw #+ folderName 
curPath_raw = rawPath + '3_curated/'
curPath_tran = curPath_raw + 'Transaction/' #+ folderName + '/'
curPath_dim = curPath_raw + 'Dimension/' #+ folderName + '/'
rawPath_user = storagePath + '4_user/'

# COMMAND ----------



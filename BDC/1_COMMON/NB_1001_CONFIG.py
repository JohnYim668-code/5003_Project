# Databricks notebook source
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

# # para for ADF
# # date
# date = dbutils.widgets.get("date") #for program run
# log_datetime = dbutils.widgets.get("datetime") #for program run
# dateTime = dbutils.widgets.get("datetime") #for program run

# # ADLS
# storageAccount = dbutils.widgets.get("storageAccount") #for program run
# storageContainer = dbutils.widgets.get("storageContainer") #for program run
# secretScope = dbutils.widgets.get("secretScope") #for program run

# COMMAND ----------

# for manual run
dbutils.widgets.text('date', '20250405')
dbutils.widgets.text('dateTime', '2025-04-05-00:00:00')

date = dbutils.widgets.get("date")
log_datetime = dbutils.widgets.get("dateTime")
dateTime = dbutils.widgets.get("dateTime")

# COMMAND ----------

# Data Lake setting
storageAccount = "bigdatacompute01" #for manual run
storageContainer = "edp-bdc" #for manual run
secretScope = 'databricks-secret-scope-01' #for manual

keyvaultSecret = 'DataLake-secret'
# secretScope = 'DataLake-KeyVault-scope'

blobStorage = storageAccount + '.dfs.core.windows.net'
storageAccountAccessKey = dbutils.secrets.get(scope=secretScope, key=keyvaultSecret)

# Config setting
spark.conf.set('fs.azure.account.key.'+storageAccount+'.dfs.core.windows.net', storageAccountAccessKey)
spark.conf.set('parquet.enable.summary-metadata', 'true')
spark.conf.set('spark.sql.sources.commitProtocolClass', 'org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol')
spark.conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs', 'false')


# Create the blob client
connectionString = 'DefaultEndpointsProtocol=https;AccountName='+storageAccount+';AccountKey='+storageAccountAccessKey+';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connectionString)
# Create the container client
container_client = blob_service_client.get_container_client(storageContainer)

# COMMAND ----------

year = date[0:4]
month = date[4:6]
day = date[6:8]
yearMonth = year + month
today_Datetime = datetime.strptime(date,'%Y%m%d')
timeStr = dateTime[11:]
print(today_Datetime)
dateStr = date
yearStr = year
monthStr = month

# COMMAND ----------

delta = timedelta(days=0,seconds=0,microseconds=0,milliseconds=0,minutes=0,hours=8,weeks=0)
log_datetimeUTC = datetime.strptime(dateTime,'%Y-%m-%d-%H:%M:%S').date()
log_datetimeHKT = (datetime.strptime(dateTime,'%Y-%m-%d-%H:%M:%S') + delta).date()

# COMMAND ----------

dbwPath = '/Users/kkjyim@connect.ust.hk/BDC/'

# COMMAND ----------

log_datetime = log_datetime.replace(" ", "-")
print(log_datetime)

# COMMAND ----------

#last month transformation
lastMonth_Datetime = today_Datetime - relativedelta(months=1)
lastMonth_Date = lastMonth_Datetime.strftime('%Y%m%d')
lastMonth_Year = lastMonth_Date[0:4]
lastMonth_Month = lastMonth_Date[4:6]
lastMonth_Day = lastMonth_Date[6:8]
lastYearMonth = int(lastMonth_Year + lastMonth_Month)

# COMMAND ----------

#month end
last_day_of_prev_month = today_Datetime.replace(day=1) - timedelta(days=1)
last_two_prev_month = today_Datetime.replace(day=1) - timedelta(days=1) - relativedelta(months=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS tempdb;
# MAGIC CREATE DATABASE IF NOT EXISTS landing;
# MAGIC CREATE DATABASE IF NOT EXISTS history;
# MAGIC CREATE DATABASE IF NOT EXISTS curated;

# COMMAND ----------

# %sql
# DROP DATABASE IF EXISTS tempdb CASCADE;
# DROP DATABASE IF EXISTS landing CASCADE;
# DROP DATABASE IF EXISTS history CASCADE;
# DROP DATABASE IF EXISTS curated CASCADE;
# DROP DATABASE IF EXISTS user CASCADE;

# -- DROP DATABASE IF EXISTS default CASCADE;
# --drop table if exists tempdb.tmpbatchfileSummary;
# --drop table if exists tempdb.tmpsourcefileexistSummary;
# --drop table if exists tempdb.tmpCheckHistRowSummary;

# COMMAND ----------



# Databricks notebook source
dbutils.widgets.get("date") #for program run
date = getArgument("date")
# dbutils.widgets.get("dateTime")
# log_datetime = getArgument("dateTime")#for program run
dateTime = dbutils.widgets.get("dateTime") #for program run
dateTime = getArgument("dateTime")
#ADLS
storageAccount = dbutils.widgets.get("storageAccount") #for program run
storageAccount = getArgument("storageAccount")
storageContainer = dbutils.widgets.get("storageContainer") #for program run
storageContainer = getArgument("storageContainer")

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1005_MAP_TABLE_LIST

# COMMAND ----------

# loop 2_history table
for tableName in historyTableList:
#     NB_2002_HIST_Details
    dbutils.notebook.run("NB_2002_HIST_Details", 600000, {"tableName": tableName, "date": date, "dateTime": dateTime, "storageAccount": storageAccount, "storageContainer": storageContainer, "region": region, "loadLevel": loadLevel, "system": system})


# COMMAND ----------

fileName = '' 
folderName = ''

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1002_CONNECTION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1003_FUNCTION

# COMMAND ----------

# call output vaildation function
outputCheckRowCount('history')

# COMMAND ----------

# spark.stop()

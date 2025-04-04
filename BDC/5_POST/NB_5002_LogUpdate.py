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

# COMMAND ----------

delta_table_path = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/0_config/batch_summary/batch_summary_temp'
BatchFileSummary = spark.read.format("delta").load(delta_table_path)
BatchFileSummary = BatchFileSummary.orderBy(BatchFileSummary.startTime)
BatchFileSummary.display()

# COMMAND ----------

data_location = logPath + 'DBWBatchFileSummary'

#output & combine files into 1
BatchFileSummary.filter(BatchFileSummary.batchID==id).coalesce(1).write.mode("overwrite").csv(data_location,header=True)
renamePartitionFile(data_location,'csv')

# COMMAND ----------

# For ADF
list = [
    ('NB_2003_HIST_Behaviour_Log','behaviour_log'),
    ('NB_2004_HIST_Sales_Target','sales_target'),
    ('NB_3000_CUR_Dim_Date','self'),
    ('NB_3001_CUR_Dim_Product','behaviour_log'),
    ('NB_3002_CUR_Fact_Behaviour_Log','behaviour_log'),
    ('NB_3003_CUR_Fact_Target','sales_target')
]

schema = StructType([
    StructField('notebook', StringType(), True),
    StructField('source', StringType(), True)
])

NotebookMap = spark.createDataFrame(data = list, schema = schema)
display(NotebookMap)

# COMMAND ----------

rpaths = dbutils.fs.ls(storagePath+"999_log/batch_summary/")
#rdf = spark.read.load(rpaths[0].path, 'csv').withColumn('name',lit(rpaths[0].name))
a = 0
for j in range(0, len(rpaths)):
  ipaths = dbutils.fs.ls(rpaths[j].path)
#  ipaths = [ipaths for ipaths in ipaths if ipaths.path.endswith(('ADFBatchFileSummary.csv'))]
  for i in range(0, len(ipaths)):
    if ipaths[i].name=='ADFBatchFileSummary.csv':
      temp = spark.read.option('header','true').load(ipaths[i].path, 'csv')
      if temp.count()>0:
        a+=1
        if a==1:
          ADFbatchfile = temp
        else:
          ADFbatchfile = ADFbatchfile.union(temp)

display(ADFbatchfile)

# COMMAND ----------

latestSuccessADFBatch = ADFbatchfile.filter("status = 'complete'")\
                                    .groupBy('item').agg(max(ADFbatchfile.endTime).alias('endTime'))
display(latestSuccessADFBatch)

# COMMAND ----------

latestSuccessBatch = BatchFileSummary.filter('status = "complete"').filter(BatchFileSummary.item.startswith('NB_30'))\
                                     .groupBy('item').agg(max(BatchFileSummary.startTime).alias('startTime'),max(BatchFileSummary.batchID).alias('batchID'))
latestSuccessBatch = latestSuccessBatch.join(NotebookMap, latestSuccessBatch.item==NotebookMap.notebook, how='left')

latestSuccessBatch = latestSuccessBatch.groupBy('source').agg(min(latestSuccessBatch.startTime).alias('DBWstartTime'),min(latestSuccessBatch.batchID).alias('batchID'))
latestSuccessBatch = latestSuccessBatch.join(latestSuccessADFBatch, (latestSuccessBatch.source==latestSuccessADFBatch.item) & (latestSuccessBatch.DBWstartTime>=latestSuccessADFBatch.endTime), 'left').drop('item')
latestSuccessBatch = latestSuccessBatch.withColumn('endTime', when(col('endTime').isNull(), col('DBWstartTime')).otherwise(col('endTime')))
latestSuccessBatch = latestSuccessBatch.filter("source is not null")\
                                       .withColumn('endTime', concat(substring(regexp_replace('endTime',' ','T'),1,19),lit('.00Z')))
#                                       .withColumn('startTime', concat(substring(regexp_replace('startTime',' ','T'),1,22),lit('Z')))
display(latestSuccessBatch)
latestSuccessBatch = latestSuccessBatch.select('source','endTime','batchID')
display(latestSuccessBatch)

# COMMAND ----------

#Output DBWSuccessBatchFileSummary
data_location = logPath + 'DBWSuccessBatchFileSummary'

#output & combine files into 1
latestSuccessBatch.coalesce(1).write.mode("overwrite").csv(data_location,header=True)
renamePartitionFile(data_location,'csv')

display(latestSuccessBatch)

# COMMAND ----------

#Output DBWSourceFileExistSummary
data_location = logPath + 'DBWSourceFileExistSummary'

fileExist = spark.table('tempdb.tmpSourceFileExistSummary')
fileExist = fileExist.orderBy(fileExist.startTime)
display(fileExist)

#output & combine files into 1
fileExist.filter(fileExist.batchID==id).coalesce(1).write.mode("overwrite").csv(data_location,header=True)
renamePartitionFile(data_location,'csv')

# COMMAND ----------



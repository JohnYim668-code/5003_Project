# Databricks notebook source
# MAGIC %run ../1_COMMON/NB_1001_CONFIG

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1003_FUNCTION

# COMMAND ----------

runDate = dateStr

# COMMAND ----------

# datetime object containing current date and time

startTime = datetime.utcnow()
#utc+8 time
delta = timedelta(days=0,seconds=0,microseconds=0,milliseconds=0,minutes=0,hours=8,weeks=0)
startTime = str(startTime+delta)

ddlSchema = StructType([
    StructField('batchID',IntegerType()),
    StructField('date',StringType()),
    StructField('startTime',StringType()),
    StructField('item',StringType()),
    StructField('status',StringType())
])

# COMMAND ----------

def getBatchId(item):
    tableName = "tempdb.tmpBatchFileSummary"
    df = sqlContext.sql('SELECT * FROM {0} WHERE item = "{1}" order by batchID desc limit 1'.format(tableName, item))
    if df.count()>0:
        #return the latest batch id
        id = df.collect()[0][0]
    else:
        #first batch
        id = 0
    return id

# COMMAND ----------

def getBatchIdForOutput(item):
    tableName = "tempdb.tmpBatchFileSummary"
    if spark._jsparkSession.catalog().tableExists(tableName):
        df = sqlContext.sql('SELECT * FROM {0} WHERE item = "Start" or item = "{1}"'.format(tableName, item))
        if df.count()==0: #first batch 
            ApList = ([(1, runDate, startTime, item, 'running')])
            df_Ap = spark.createDataFrame(ApList,ddlSchema)
            batchId = 1
        else:
            df1 = sqlContext.sql('SELECT * FROM {0} WHERE item = "{1}" order by batchID desc limit 1'.format(tableName, item))
            if df1.count()==0:
                a = 0
            else:
                a = df1.collect()[0][0]
            
            df2 = sqlContext.sql('SELECT * FROM {0} WHERE item = "Start" order by batchID desc limit 1'.format(tableName)) #current batch
            if df2.count()==0:
                b = 0
            else:
                b = df2.collect()[0][0]
            
            if a >= b:
                c = a + 1
            else:
                c = b
            
            ApList = ([(c, runDate, startTime, item, 'running')]) 
            df_Ap = spark.createDataFrame(ApList,ddlSchema)
            batchId = df_Ap.collect()[df_Ap.count()-1][0]
#         df_Ap.write.mode("append").option("path",storagePath+'0_config/rfidepc/'+ region + '/batch_summary/batch_summary_temp').saveAsTable(tableName)
        df_Ap.write.mode("append").partitionBy('item').format("delta").save(storagePath+'0_config/batch_summary/batch_summary_temp')
    else:
        list=([(1,runDate,startTime,item,'running')]) #'running'
        df = spark.createDataFrame(list,ddlSchema)
#         df.write.mode("append").option("path",storagePath+'0_config/rfidepc/'+ region + '/batch_summary/batch_summary_temp').saveAsTable(tableName)
        df.write.mode("append").partitionBy('item').format("delta").save(storagePath+'0_config/batch_summary/batch_summary_temp')
        batchId = df.collect()[0][0]
    print(batchId)
    return str(batchId)

# COMMAND ----------

def completeBatch(item, batchId):
    tableName = "tempdb.tmpBatchFileSummary"
    batchTemp = sqlContext.sql('SELECT * FROM {0}'.format(tableName))
    batchId = int(batchId)
    compleBatch = batchTemp.withColumn('status',when((col('batchID')==batchId)&(col('item')==item),'complete').otherwise(col('status')))\
                           .filter((col('batchID')==batchId) & (col('item')==item))
#     compleBatch = compleBatch.write.mode("overwrite").option("path",storagePath+'0_config/rfidepc/' + region + '/batch_summary/batch_summary_temp/').saveAsTable(tableName)
    compleBatch = compleBatch.write.mode("overwrite").option("replaceWhere", "item == '{0}' AND batchID == '{1}'".format(item, batchId))\
                             .option("path",storagePath+'0_config/batch_summary/batch_summary_temp/')\
                             .saveAsTable(tableName)

# COMMAND ----------

# %sql
# select * from tempdb.tmpBatchFileSummary --order by startTime:

# COMMAND ----------



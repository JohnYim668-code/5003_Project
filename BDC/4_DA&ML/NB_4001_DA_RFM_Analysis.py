# Databricks notebook source
folderName="DA_RFM_Analysis"
dbSchema  = 'tempdb'
partitionField = "ETLUPDATEDDATETIME"

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
item = text.replace(dbwPath+'4_DA&ML/','')

# COMMAND ----------

batchID = getBatchIdForOutput(item)
batchID = int(batchID)
# batchID = 1

# COMMAND ----------

# Define the table name and database
invalidSummaryTableName = 'tmpinvalidsummary_da_rfm_analysis'
database_name = 'tempdb'

# Set the current database context
spark.sql(f"USE {database_name}")

# Check if the table exists using SQL
table_exists = spark.sql(f"SHOW TABLES").filter(f"tableName = '{invalidSummaryTableName}'").count() > 0

# Define the known external location
external_location = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/998_temp/DA_RFM_Analysis'

# Check if the table exists
if table_exists:
    # Drop the table safely
    spark.sql(f'DROP TABLE IF EXISTS {invalidSummaryTableName}')
    print(f"Dropped table: {invalidSummaryTableName}")

    # Delete the external data directly using the known location
    if external_location:
        dbutils.fs.rm(external_location, recurse=True)
        print(f"Deleted external data at: {external_location}")
else:
    print(f"Table does not exist: {invalidSummaryTableName}")

# COMMAND ----------

# Input
# Get Spark Table
delta_table_path = curPath_tran + 'Fact_Behaviour_Log/'
behaviourTbl = spark.read.format("delta").load(delta_table_path)
behaviourTbl = behaviourTbl.select('EVENT_DATETIME', 'EVENT_TYPE', 'PRICE', 'USER_ID', 'USER_SESSION')
filterbehaviourTbl = behaviourTbl.filter(behaviourTbl.EVENT_TYPE == 'purchase')

aggbehaviourTbl = filterbehaviourTbl.groupBy("USER_SESSION").agg(
    max("EVENT_DATETIME").alias("DATETIME_ORDER"),
    concat_ws(', ', collect_set("USER_ID")).alias("USER_ID"), 
    count("USER_SESSION").alias("QUANTITY"),
    sum("PRICE").alias("SPENT_MONEY")
)

max_date_row = aggbehaviourTbl.agg(max("DATETIME_ORDER").alias('MAX_DATE')).collect()
max_date = max_date_row[0]['MAX_DATE']
study_date = max_date + timedelta(days=1)
study_date_str = datetime.strftime(study_date, '%Y-%m-%d')

# Calculate last purchase in days
aggbehaviourTbl = aggbehaviourTbl.withColumn('LAST_PURCHASE', datediff(to_date(lit(study_date_str)), col('DATETIME_ORDER')))

# COMMAND ----------

# Calculate Recency, Frequency, and Monetary of the data
RFM = aggbehaviourTbl.groupBy('USER_ID').agg(
    min('LAST_PURCHASE').alias('RECENCY'),
    count('USER_ID').alias('FREQUENCY'),
    sum('SPENT_MONEY').alias('MONETARY')
)

quartiles = RFM.approxQuantile(['RECENCY', 'FREQUENCY', 'MONETARY'], [0.25, 0.5, 0.75], 0.01)
quartiles_dict ={
    'RECENCY': quartiles[0],
    'FREQUENCY': quartiles[1],
    'MONETARY': quartiles[2]
}
# print(quartiles_dict)

# COMMAND ----------

# Define the Recency UDF
def recency_udf(value):
    if value <= quartiles_dict['RECENCY'][0]:
        return 1
    elif value <= quartiles_dict['RECENCY'][1]:
        return 2
    elif value <= quartiles_dict['RECENCY'][2]:
        return 3
    else:
        return 4

# Register the Recency UDF
recency_udf_spark = udf(recency_udf, IntegerType())

# Define the Frequency and Monetary UDF
def fm_udf(value, metric):
    if value <= quartiles_dict[metric][0]:
        return 4
    elif value <= quartiles_dict[metric][1]:
        return 3
    elif value <= quartiles_dict[metric][2]:
        return 2
    else:
        return 1

# Register the Frequency and Monetary UDF
fm_udf_spark = udf(lambda x, p: fm_udf(x, p), IntegerType())

# Create R_Quartile using the Recency UDF
RFM = RFM.withColumn('R_QUARTILE', recency_udf_spark(col('RECENCY')))

# Create F_Quartile using the Frequency UDF
RFM = RFM.withColumn('F_QUARTILE', fm_udf_spark(col('FREQUENCY'), lit('FREQUENCY')))

# Create M_Quartile using the Monetary UDF
RFM = RFM.withColumn('M_QUARTILE', fm_udf_spark(col('MONETARY'), lit('MONETARY')))

# Create RFM_segmentation
RFM = RFM.withColumn(
    'RFM_SEGMENTATION',
    concat(
        col('R_QUARTILE').cast('string'),
        col('F_QUARTILE').cast('string'),
        col('M_QUARTILE').cast('string')
    )
)

# Create RFM_score
RFM = RFM.withColumn(
    'RFM_SCORE',
    col('R_QUARTILE') + col('F_QUARTILE') + col('M_QUARTILE')
)

# COMMAND ----------

# Define the RFM_label function
def rfm_label(score):
    if score >= 10:
        return 'Lost'
    elif 9 <= score < 10:
        return 'Hibernating'
    elif 8 <= score < 9:
        return 'Can’t Lose Them'
    elif 7 <= score < 8:
        return 'About To Sleep'
    elif 6 <= score < 7:
        return 'Promising'
    elif 5 <= score < 6:
        return 'Potential Loyalist'
    elif 4 <= score < 5:
        return 'Loyal Customers'
    else:
        return 'Champions'

# Register the RFM_label UDF
rfm_label_udf = udf(rfm_label, StringType())

# Create RFM_label for customers based on the RFM_score
RFM = RFM.withColumn('RFM_LABEL', rfm_label_udf(col('RFM_score')))\
         .withColumn('USER_ID_KEY', xxhash64(concat(col('USER_ID'))))\
         .withColumn('ETLUPDATEDDATETIME', lit(currentTimeUtc8).cast(TimestampType()))\

# Create Partition Fields
RFM = RFM.withColumn('PARTITIONYEAR', year((partitionField))).withColumn('PARTITIONMONTH', month((partitionField))).withColumn('PARTITIONDAY', dayofmonth((partitionField)))

RFM_result = RFM.select('USER_ID_KEY', 'USER_ID', 'RECENCY', 'FREQUENCY', 'MONETARY', 'R_QUARTILE', 'F_QUARTILE', 'M_QUARTILE', 'RFM_SEGMENTATION', 'RFM_SCORE', 'RFM_LABEL', 'PARTITIONYEAR', 'PARTITIONMONTH', 'PARTITIONDAY', 'ETLUPDATEDDATETIME')

# COMMAND ----------

RFM_result.display()

# COMMAND ----------

spark.catalog.dropTempView("temporary_table")
RFM_result.createTempView('temporary_table')

if spark._jsparkSession.catalog().tableExists("curated." + folderName):
    print("Exist")
else:
    print("Not Exist")
    RFM_result.filter("1!=1").write.mode("overwrite").partitionBy('PARTITIONYEAR','PARTITIONMONTH','PARTITIONDAY')\
                                  .save(curPath_tran + folderName)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO curated.da_rfm_analysis AS target
# MAGIC     USING temporary_table source
# MAGIC   ON target.USER_ID = source.USER_ID
# MAGIC   WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# # Delta Record
# delta_table_path = curPath_raw + '/Transaction/DA_RFM_Analysis/'
# RFM_result = spark.read.format("delta").load(delta_table_path)
# outputDeltaTran(RFM_result, curPath_raw + "/Transaction/DA_RFM_Analysis/") 

# COMMAND ----------

completeBatch(item, batchID)

# COMMAND ----------

# %sql
# select * from tempdb.tmpbatchfilesummary

# COMMAND ----------

# # Load your Delta table
# delta_table_path = "abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/0_config/batch_summary/batch_summary_temp"
# spark.sql("CREATE TABLE IF NOT EXISTS tempdb.tmpbatchfilesummary USING DELTA LOCATION '{}'".format(delta_table_path))

# # Update records in the Delta table
# spark.sql("""
#     DELETE FROM tempdb.tmpbatchfilesummary
#     WHERE item = 'NB_4001_DA_RFM_Analysis'
# """)

# COMMAND ----------



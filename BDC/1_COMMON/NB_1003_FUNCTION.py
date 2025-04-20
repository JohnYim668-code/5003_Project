# Databricks notebook source
# %run ../1_COMMON/NB_1001_CONFIG

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import Window
from pyspark.sql import SQLContext
from functools import reduce
from pyspark.sql.functions import lit
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta
from datetime import *
import pytz
import glob
import os
from pyspark.sql import functions as F

# COMMAND ----------

# Generate Log file in 999_log
# datetime object containing current date and time
currentTimeUtc = datetime.utcnow()
delta = timedelta(days=0,seconds=0,microseconds=0,milliseconds=0,minutes=0,hours=8,weeks=0)
currentTimeUtc8 = str(currentTimeUtc + delta)


#formatting file name
validFileName = currentTimeUtc8.replace("-", "").replace(' ', '-').replace(':','')
firstDelPos = validFileName.find(".")
validFileName = '_'+validFileName.replace(validFileName[firstDelPos:firstDelPos+7],'')+'_'

# COMMAND ----------

spark.conf.set("spark.databricks.delta.checkLatestSchemaOnRead","false") 
spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", False)

# COMMAND ----------

#Check file existence
def fileExist(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
           return False
        else:
            raise

# COMMAND ----------

def fileExistsInPath(arg1): 
    try: 
        dbutils.fs.head(arg1,1) 
    except: 
        dbutils.notebook.exit("no existing record")
        return False; 
    else: 
        return True;

# COMMAND ----------

#Check file existence
def fileExistCheckPath(path):
    try:
        if len(dbutils.fs.ls(path)) == 0:
            dbutils.notebook.exit("no existing record")
        else:
            return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            dbutils.notebook.exit("no existing record")
        else:
            raise

# COMMAND ----------

#Check file existence for wildcard match
def fileExistWildcard(path, filename):
  try:
    for f in dbutils.fs.ls(path):
      if filename in f.name:
        return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

#Read parquet
def readParquet(rawPath):
    df = (spark.read.format('parquet')
      .option('header','true')
      .parquet(rawPath)
         )
    return df

# COMMAND ----------

#Read json
def readJson(rawPath, fileName):
    # Check path is valid or not
    fileExistCheckPath(rawPath + dateStr + '/' +timeStr + '/')
    df = spark.read.format("json").option("recursiveFileLookup", "true").option("header", "true").load(rawPath + dateStr + '/' +timeStr + '/') 
    counting = df.count()
    return df, counting

# COMMAND ----------

def readCSV(rawPath):
    fileExistCheckPath(rawPath + '/')
    df = (spark.read.format('csv').option('header', 'true').option('multiline', 'true').option('escape','"').load(rawPath))
    counting = df.count()
    return df, counting

# COMMAND ----------

ddlSchema_source_file = StructType([
    StructField('batchID',StringType()),
    StructField('date',StringType()),
    StructField('startTime',StringType()),
    StructField('notebook',StringType()),
    StructField('count',StringType()),
    StructField('file',StringType()),
    StructField('exist',StringType())
])

existTableName = 'tmpSourceFileExistSummary'

# Read different file type
def readfile(rawPath, fileType, fileName):
    dbSchema = 'tempdb'
    if fileExist(rawPath):
        if fileType == "json":
            df, counting = readJson(rawPath, fileName)
        elif fileType == "csv":
            df, counting = readCSV(rawPath)
        elif fileType == "parquet":
            df = readParquet(rawPath)
        #append the exist record if table exist
        List_FileExist = [(batchID, runDate, startTime, item, counting, folderName, 'Y')]
        df_FileExist = spark.createDataFrame(List_FileExist, ddlSchema_source_file)
        if spark._jsparkSession.catalog().tableExists(dbSchema, existTableName):
            df_FileExist.write.mode("append").option("mergeSchema", "true").format("delta").save(storagePath + '/0_config/batch_summary/source_file_summary_temp/')
        #create table if it does not exist
        else:
            df_FileExist.write.mode("overwrite").option("overwriteSchema", "true").save(storagePath + '/0_config/batch_summary/source_file_summary_temp/')
        if len(df.head(1)) > 0:
            return df
        else:
            completeBatch(item, region, batchID)
            dbutils.notebook.exit("no existing record")
    else:
        #append the invalid record if table not exist
        List_FileExist = [(batchID, runDate, startTime, item, 'N/A', folderName, 'N')] 
        df_FileExist = spark.createDataFrame(List_FileExist, ddlSchema_source_file)
        if spark._jsparkSession.catalog().tableExists(dbSchema, existTableName):
            df_FileExist.write.mode("append").option("mergeSchema", "true").save(storagePath + '/0_config/rfidepc/batch_summary/source_file_summary_temp/')
        #create table if it does not exist
        else:
            df_FileExist.write.mode("overwrite").option("overwriteSchema", "true").save(storagePath + '/0_config/rfidepc/batch_summary/source_file_summary_temp/')
        dbutils.notebook.exit("path not exist")

# COMMAND ----------

def outputDelta(output, path):
  # Repartition to increase parallelism
  output = output.repartition(128)  # Adjust based on cluster capacity

  # Configure Spark settings
  spark.conf.set("spark.sql.shuffle.partitions", "128")  # Increase shuffle partitions

  output.write \
    .partitionBy('PARTITIONYEAR','PARTITIONMONTH','PARTITIONDAY','BATCHNUMBER')\
    .format("delta")\
    .mode("append")\
    .option("overwriteSchema", "true")\
    .save(path)

# COMMAND ----------

def outputParquetOverwrite(output, path, name):  
    output.write \
      .partitionBy('PartitionYear','PartitionMonth','PartitionDay','BatchNumber')\
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true")\
      .option("path", path)\
      .saveAsTable("history." + name)

# COMMAND ----------

#output
def outputParquetDim(output, path, name):
    output.write.format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true")\
      .option("path", path)\
      .saveAsTable("curated." + name)

# COMMAND ----------

#output
def outputDeltaDim(output, path):
    # Repartition to increase parallelism
    output = output.repartition(128)  # Adjust based on cluster capacity

    # Configure Spark settings
    spark.conf.set("spark.sql.shuffle.partitions", "128")  # Increase shuffle partitions

    output.write \
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save(path)

# COMMAND ----------

#output
def outputDeltaTran(output, path):
    # Repartition to increase parallelism
    output = output.repartition(128)  # Adjust based on cluster capacity

    # Configure Spark settings
    spark.conf.set("spark.sql.shuffle.partitions", "128")  # Increase shuffle partitions

    output.write \
          .partitionBy('PARTITIONYEAR','PARTITIONMONTH','PARTITIONDAY')\
          .format("delta")\
          .mode("overwrite")\
          .save(path)

# COMMAND ----------

#Create Hist table
def createHistTable(df, folderName):
    if spark._jsparkSession.catalog().tableExists('history', folderName):
        df.write.mode("append").option("mergeSchema", "true").saveAsTable("history."+folderName)
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("history."+folderName)

# COMMAND ----------

#import datetime
convertdatetime =  udf (lambda x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'), TimestampType())

# COMMAND ----------

#from datetime import datetime
histconvertdatetime =  udf (lambda x: datetime.strptime(x, '%d/%m/%Y %H:%M:%S') if x is not None else None, TimestampType())

# COMMAND ----------

#from datetime import datetime
historicalconvertdatetime = udf (lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f') if x is not None else None, TimestampType())

# COMMAND ----------

# Null Checking & Output Null rows
def checkNull(dataDate,checkTable,checkField,primaryKey,filterCriteria="1=1"):
    checkDf = spark.table(checkTable).filter(filterCriteria)
    
    if len(checkDf.head(1)) > 0:
        checkFieldExpr = checkField.replace(',', ' is null or ') + ' is null'
        checkFieldValueExpr = 'concat_ws(",", col("' + checkField.replace(',','"),col("') + '"))'
        primaryKeyExpr = 'concat_ws(",", col("' + primaryKey.replace(',','"),col("') + '"))'
        
        check = checkDf.filter(checkFieldExpr)
        data = check.withColumn('checkTable',lit(checkTable))\
                    .withColumn('primaryKey', lit(primaryKey))\
                    .withColumn('primaryKeyValue', eval(primaryKeyExpr))\
                    .withColumn('checkField', lit(checkField))\
                    .withColumn('checkFieldValue', eval(checkFieldValueExpr))\
                    .withColumn('dataDate',lit(dataDate))\
                    .withColumn('runDate',current_timestamp())\
                    .withColumn('errorType', lit("Null value"))
        data = data.withColumn('errorMessage', concat(lit("The Column(s) "),col("checkField"),lit(" of the Table "),col("checkTable"),lit(" is null")))
        data = data.select('dataDate','errorType','checkTable','checkField','checkFieldValue','primaryKey','primaryKeyValue','errorMessage','runDate')
        
        return data

# COMMAND ----------

# Check Duplicates
def checkDuplicate(dataDate,tableType,timeColumn,tableName,checkTable,checkField,primaryKey,PK,filterCriteria="1=1"):
    checkDf = checkTable.filter(filterCriteria) 
    #check if the table has record
    if len(checkDf.head(1)) > 0:
        checkFieldExpr = 'concat_ws(",", col("' + checkField.replace(',','"),col("') + '"))' #col(pk_id) checkField = pk_id,fk_id
        primaryKeyExpr = 'concat_ws(",", col("' + primaryKey.replace(',','"),col("') + '"))'
           
        data = checkDf.withColumn('_key', eval(checkFieldExpr))
        data = data.withColumn('dupeCount', count("*").over(Window.partitionBy("_key").orderBy(col(timeColumn).desc()))).filter('dupeCount > 1') # LHS:1123 RHS:11; LHS:123 RHS:1     F.row_number().over(Window.partitionBy("driver").orderBy(col("unit_count").desc()))
        reorderCol = checkDf.columns
        if tableType == 'Dimension':
            dupKey=checkDf.dropDuplicates(primaryKey.split(','))
            #dupKey = checkDf.join(data, PK, 'left_anti').select(reorderCol) # select unqiue
            #remainDistinct = data.select(reorderCol).distinct() # duplicate select one
            #dupKey = dupKey.union(remainDistinct)
        else:
            dupKey = checkDf.join(data, PK, 'left_anti').select(reorderCol) # select unqiue
            remainDistinct = data.withColumn("row_number", row_number().over(Window.partitionBy("_key").orderBy(col(timeColumn).desc()))).filter(col("row_number") == 1).drop("row_number").drop("_key").drop("dupeCount") # duplicate select latest one
            dupKey = dupKey.union(remainDistinct) 

        #dupKey = checkDf.join(data, PK, 'left_anti').select(reorderCol) # 2 3
        
        data = data.withColumn('checkTable',lit(tableName))\
                   .withColumn('primaryKey', lit(primaryKey))\
                   .withColumn('primaryKeyValue', eval(primaryKeyExpr))\
                   .withColumn('checkField', lit(checkField))\
                   .withColumn('checkFieldValue', eval(checkFieldExpr))\
                   .withColumn('dataDate',lit(dataDate))\
                   .withColumn('runDate',current_timestamp())\
                   .withColumn('errorType', lit("Duplicated value"))
        data = data.withColumn('errorMessage', concat(lit("The Column(s) "),col("checkField"),lit(" of the Table "),col("checkTable"),lit(" have duplicated value: ["),col("checkFieldValue"),lit("]")))
        data = data.select('dataDate','errorType','checkTable','checkField','checkFieldValue','primaryKey','primaryKeyValue','errorMessage','runDate')
        
        return data, dupKey

# COMMAND ----------

def checkMappingExp(checkField):
    checkFieldlist = checkField.replace('_Key','').split(',')
    checkFieldExpr1 = ['('+x + '_Key is null and' for x in checkFieldlist]
    checkFieldExpr2 = [x + '_FK is not null)' for x in checkFieldlist]
    checkFieldExpr = []
    i = 0
    for x in checkFieldExpr1:
        checkFieldExpr.append(x)
        checkFieldExpr.append(checkFieldExpr2[i])
        i += 1
    checkFieldExpr=" ".join(checkFieldExpr)
    checkFieldExpr = checkFieldExpr.replace(') (',') or (')
    return checkFieldExpr

# COMMAND ----------

# Check Mapping
def checkMapping(dataDate, tableName, checkTable, checkField, primaryKey, filterCriteria="1=1"):
    checkDf = checkTable.filter(filterCriteria)
    
    if len(checkDf.head(1)) > 0:
        checkFieldExpr = checkMappingExp(checkField)
        primaryKeyExpr = 'concat_ws(",", col("' + primaryKey.replace(',','"),col("') + '"))'
        foreignKeyExpr = 'concat_ws(",", col("' + checkField.replace('_Key','_FK').replace(',','"),col("') + '"))'
        
        check = checkDf.filter(checkFieldExpr)
        data = check.withColumn('checkTable',lit(tableName))\
                    .withColumn('primaryKey', lit(primaryKey))\
                    .withColumn('primaryKeyValue', eval(primaryKeyExpr))\
                    .withColumn('checkField', lit(checkField))\
                    .withColumn('checkFieldValue', eval(foreignKeyExpr))\
                    .withColumn('dataDate',lit(dataDate))\
                    .withColumn('runDate',current_timestamp())\
                    .withColumn('errorType', lit("Field mapping"))
        data = data.withColumn('errorMessage', concat(lit("The Column(s) "),col("checkField"),lit(" of the Table "),col("checkTable"),lit(" is invalid")))
        data = data.select('dataDate','errorType','checkTable','checkField','checkFieldValue','primaryKey','primaryKeyValue','errorMessage','runDate')
        print(checkFieldExpr)
        return data

# COMMAND ----------

# Create Temp Table For Data Validation
def validUpdate(dbSchema, data, folderName, storagePath):
    # Define the valid table name
    validTableName = dbSchema + '.tmpInvalidSummary_' + folderName
    externalTablePath = storagePath + '/998_temp/' + folderName
    
    # Write the data to the specified path
    data.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .format("delta") \
        .save(externalTablePath)  

    # Create the external table using SQL
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {validTableName} 
        USING DELTA 
        LOCATION '{externalTablePath}'
    """)

# COMMAND ----------

#rename file
def renamePartitionFile(dataLocation, fileType):
    files = dbutils.fs.ls(dataLocation)
    file_list = [x.path for x in files if x.path.endswith("."+fileType)][0]
    dbutils.fs.mv(file_list, dataLocation.rstrip('/') + "."+fileType)
    dbutils.fs.rm(dataLocation, recurse = True)

# COMMAND ----------

def CheckRun(reqtablelist):
    tableName = 'tempdb.tmpBatchFileSummary'
    for reqtable in reqtablelist:
        CheckRun = spark.table(tableName).filter(col('item').like(f'%{reqtable}%'))
        CheckRun = CheckRun.filter(col('batchID') == CheckRun.agg({"batchID": "max"}).collect()[0][0])
#        if CheckRun is not None:
        if CheckRun.count()>0:
            if CheckRun.collect()[0][4] == 'complete':
                print('OK')
            else:
                dbutils.notebook.exit("path not exist")
        else:
            dbutils.notebook.exit("path not exist")

# COMMAND ----------

def logGen():
    tableName = 'tempdb.tmpInvalidSummary' + '_' + folderName
    if spark._jsparkSession.catalog().tableExists(tableName):
        #create dataframe from invalid summary
        df_check = spark.table(tableName)
        if len(df_check.head(1)) > 0:
            print('Error Record Exists')
            #create json file in datalake
            df_check.coalesce(1).write.mode("overwrite").format('json').save(storagePath+'/999_log/'+dateStr+'/'+'invalid_data_log'+validFileName+str(batchID)+'_'+folderName+'.json')
            for filepath in dbutils.fs.ls(storagePath + '/998_temp/'):
                file = str(filepath).split('\'')[1]
                dbutils.fs.rm(file, True)
        else:
            print('Error Record Not Exists')

# COMMAND ----------

# All columns name to upper case
def columnToUpperCase(df):
    for col in df.columns:
        df = df.withColumnRenamed(col, col.upper())
    return df

# COMMAND ----------



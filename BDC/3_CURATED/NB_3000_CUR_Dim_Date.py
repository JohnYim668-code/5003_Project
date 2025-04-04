# Databricks notebook source
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

text = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
item = text.replace(dbwPath+'3_CURATED/','')

# COMMAND ----------

id = getBatchIdForOutput(item)
# id = str(id)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# Create Date teble
schemaDate = StructType([
    StructField('Date_Key', TimestampType(), True),
    StructField('Year', IntegerType(), True),
    StructField('HalfYear', IntegerType(), True),
    StructField('Quarter', IntegerType(), True),
    StructField('Month', StringType(), True),
    StructField('MonthName', StringType(), True),
    StructField('YearMonth', StringType(), True),
    StructField('Week', StringType(), True),
    StructField('Day', IntegerType(), True),
    StructField('Weekday', StringType(), True),
    StructField('Date', TimestampType(), True)
])

# COMMAND ----------

start = datetime.strptime("01-01-2019", "%d-%m-%Y")
end = datetime.strptime("01-01-2050", "%d-%m-%Y")
dateGenerated = []
subDateList = []
# date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]

for x in range(0, (end-start).days):
    isHalfYear = 0
    date = (start + timedelta(days=x))
    #Date_PK, Year, HalfYear <6 True; >6 False, Quarter, Month, MonthName, YearMonth, Week, Day, Weekday, Date ; %U week start with sunday; %V week start with monday ISO8601; weekday monday=0 i.e.thusdayr=4
    subDateList = [date, date.year, 1 if date.month < 6 else 2, pd.Timestamp(date).quarter, date.strftime("%m"), date.strftime("%b"), str(date.year)+str(date.strftime("%m")), date.strftime("%U"), date.day, date.strftime("%w"), date] 
    dateGenerated.append(subDateList)

dataDate = pd.DataFrame(dateGenerated)

df = spark.createDataFrame(data=dataDate,schema=schemaDate)

# COMMAND ----------

df = df.withColumn("YearMonth", df.YearMonth.cast(IntegerType()))\
       .withColumn("Month", df.Month.cast(IntegerType()))\
       .withColumn("Week", df.Week.cast(IntegerType()))\
       .withColumn("Weekday", df.Weekday.cast(IntegerType()))

# COMMAND ----------

# Delta Record
outputDeltaDim(df, curPath_raw + "/Dimension/Dim_Date/") 

# COMMAND ----------

completeBatch(item, id)

# COMMAND ----------



# Databricks notebook source
# Log Related External Table Schema
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS tempdb.tmpBatchFileSummary (
    batchID INT, 
    date STRING, 
    startTime STRING,
    item STRING,
    status STRING
)
USING delta
PARTITIONED BY (item)
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/0_config/batch_summary/batch_summary_temp'
""")

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS tempdb.tmpSourceFileExistSummary (
    batchID STRING,
    date STRING,
    startTime STRING,
    notebook STRING,
    count STRING, 
    file STRING,
    exist STRING
)
USING delta
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/0_config/batch_summary/source_file_summary_temp'
""")

# COMMAND ----------

# History Table Related External Table Schema
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS history.behaviour_log (
    EVENT_TIME TIMESTAMP, 
    EVENT_TYPE STRING, 
    PRODUCT_ID STRING,
    CATEGORY_ID STRING,
    CATEGORY_CODE STRING,
    BRAND STRING,
    PRICE STRING,
    USER_ID STRING, 
    USER_SESSION STRING,
    BATCHNUMBER INT,
    PARTITIONYEAR INT,
    PARTITIONMONTH INT,
    PARTITIONDAY INT
)
USING delta
PARTITIONED BY (PARTITIONYEAR, PARTITIONMONTH, PARTITIONDAY, BATCHNUMBER)
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/2_history/behaviour_log/'
""")

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS history.sales_target (
    COUNTRY STRING, 
    YEARMONTH INTEGER, 
    TARGET DOUBLE, 
    BATCHNUMBER INT,
    PARTITIONYEAR INT,
    PARTITIONMONTH INT,
    PARTITIONDAY INT
)
USING delta
PARTITIONED BY (PARTITIONYEAR, PARTITIONMONTH, PARTITIONDAY, BATCHNUMBER)
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/2_history/sales_target/'
""")

# COMMAND ----------

# Curated Table Related External Table Schema
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS curated.Dim_date
 (
    Date_Key TIMESTAMP, 
    Year INT, 
    HalfYear INT,
    Quarter INT,
    Month STRING,
    MonthName STRING,
    YearMonth STRING,
    Week STRING, 
    Day INT,
    WeekDay STRING,
    Date TIMESTAMP
)
USING delta
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Dimension/Dim_Date'
""")

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS curated.Dim_product
 (
    PRODUCT_ID_KEY LONG, 
    PRODUCT_ID STRING, 
    CATEGORY_ID STRING,
    CATEGORY STRING,
    SUBCATEGORY1 STRING,
    SUBCATEGORY2 STRING,
    SUBCATEGORY3 STRING, 
    BRAND STRING,
    ETLUPDATEDDATETIME TIMESTAMP
)
USING delta
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Dimension/Dim_Product'
""")

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS curated.Fact_behaviour_log
 (
    BEHAVIOUR_KEY LONG, 
    PRODUCT_ID_FK LONG, 
    EVENT_DATETIME TIMESTAMP,
    EVENT_DATE STRING,
    EVENT_TIME STRING,
    EVENT_WEEKDAY INT,
    EVENT_TYPE STRING, 
    PRODUCT_ID STRING, 
    PRICE DOUBLE,
    USER_ID STRING,
    USER_SESSION STRING,
    ACTION_ITEMS INT,
    SALES_BRANCH STRING, 
    PARTITIONYEAR INT,
    PARTITIONMONTH INT, 
    PARTITIONDAY INT,
    ETLUPDATEDDATETIME TIMESTAMP
)
USING delta
PARTITIONED BY (PARTITIONYEAR, PARTITIONMONTH, PARTITIONDAY)
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Behaviour_Log'
""")

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS curated.Fact_target
 (
    TARGET_KEY LONG, 
    COUNTRY STRING, 
    DATE DATE,
    YEARMONTH INT,
    YEAR INT,
    MONTH INT,
    DAY INT, 
    DAILYTARGET DOUBLE, 
    TARGET DOUBLE,
    PARTITIONYEAR INT,
    PARTITIONMONTH INT, 
    PARTITIONDAY INT,
    ETLUPDATEDDATETIME TIMESTAMP
)
USING delta
PARTITIONED BY (PARTITIONYEAR, PARTITIONMONTH, PARTITIONDAY)
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Target'
""")

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS curated.Fact_Usersession_Cal
 (
    DATE_KEY TIMESTAMP, 
    USER_SESSION string, 
    MAX_EVENT_DATETIME timestamp, 
    MIN_EVENT_DATETIME timestamp, 
    USER_SESSION_DURATION double, 
    PARTITIONYEAR int, 
    PARTITIONMONTH int, 
    PARTITIONDAY int ,
    ETLUPDATEDDATETIME timestamp
)
USING delta
PARTITIONED BY (PARTITIONYEAR, PARTITIONMONTH, PARTITIONDAY)
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Usersession_Cal'
""")

# COMMAND ----------

# DA & ML Table Related External Table Schema
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS curated.DA_rfm_analysis
 (
    USER_ID_KEY LONG, 
    USER_ID STRING, 
    RECENCY INT,
    FREQUENCY LONG,
    MONETARY decimal(30,2),
    R_QUARTILE INT,
    F_QUARTILE INT, 
    M_QUARTILE INT, 
    RFM_SEGMENTATION STRING,
    RFM_SCORE INT, 
    RFM_LABEL STRING, 
    PARTITIONYEAR INT,
    PARTITIONMONTH INT, 
    PARTITIONDAY INT,
    ETLUPDATEDDATETIME TIMESTAMP
)
USING delta
PARTITIONED BY (PARTITIONYEAR, PARTITIONMONTH, PARTITIONDAY)
LOCATION 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/DA_RFM_Analysis'
""")

# COMMAND ----------



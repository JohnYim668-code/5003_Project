# Databricks notebook source
folderName="ML_Sales_Prediction"
dbSchema  = 'tempdb'
partitionField = "ETLUPDATEDDATETIME"

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

text = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
item = text.replace(dbwPath+'4_DA&ML/','')

# COMMAND ----------

# batchID = getBatchIdForOutput(item)
# batchID = int(batchID)
batchID = int(1)

# COMMAND ----------


behaviourTbl = spark.read.format("delta").load('abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/2_history/behaviour_log/')
behaviourTbl.count()
# Oct 42448764
# Nov 67501979
# Total 109957943

# COMMAND ----------

behaviourTbl = spark.read.format("delta").load('abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Behaviour_Log')
behaviourTbl.count()


# COMMAND ----------

# Define the table name and database
invalidSummaryTableName = 'tmpinvalidsummary_ml_sales_prediction'
database_name = 'tempdb'

# Set the current database context
spark.sql(f"USE {database_name}")

# Check if the table exists using SQL
table_exists = spark.sql(f"SHOW TABLES").filter(f"tableName = '{invalidSummaryTableName}'").count() > 0

# Define the known external location
external_location = 'abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/998_temp/ML_Sales_Prediction'

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
behaviourTbl = behaviourTbl.select('EVENT_DATE', 'EVENT_TYPE', 'PRODUCT_ID', 'PRICE')

# # Create a summary DataFrame that counts the number of occurrences of each product's price
# price_summary = behaviourTbl.select('PRODUCT_ID', 'PRICE').dropDuplicates().groupBy('PRODUCT_ID').count().withColumnRenamed('COUNT', 'PRICE_COUNT').orderBy(col('PRICE_COUNT').desc())

# # Create a list of product IDs that have 100 or more price occurrences
# morethan100p = [row.PRODUCT_ID for row in price_summary.filter(col('PRICE_COUNT') >= 100).select('PRODUCT_ID').collect()]

# # Filter the original DataFrame to include only those product IDs
# behaviourTbl100 = behaviourTbl.filter(col('PRODUCT_ID').isin(morethan100p))

# COMMAND ----------

# # Creating the week number column
# behaviourTbl100 = behaviourTbl100.withColumn("WEEK_NUMBER", weekofyear(to_timestamp(behaviourTbl.EVENT_DATE)))

# # Filter DataFrames based on event_type
# purchaseDf = behaviourTbl100.filter(col('EVENT_TYPE') == 'purchase')
# cartDf = behaviourTbl100.filter(col('EVENT_TYPE') == 'cart')
# viewDf = behaviourTbl100.filter(col('EVENT_TYPE') == 'view')

# Creating the week number column
behaviourTbl = behaviourTbl.withColumn("WEEK_NUMBER", weekofyear(to_timestamp(behaviourTbl.EVENT_DATE)))

# Creating the year column
behaviourTbl = behaviourTbl.withColumn("YEAR", year(to_timestamp(behaviourTbl.EVENT_DATE)))

# Get minimum year
min_year = behaviourTbl.agg(min(year(to_timestamp("EVENT_DATE"))).alias("MIN_YEAR")).collect()[0]["MIN_YEAR"]

# Add seq_of_week_no column
behaviourTbl = behaviourTbl.withColumn(
    "SEQ_OF_WEEK_NUMBER",
    when((col("YEAR") == min_year) & (month(to_timestamp(col("EVENT_DATE"))) == 12) & (col("WEEK_NUMBER") == 1), 2)
    .when(col("YEAR") == min_year, 1)
    .otherwise(2)
)


# Filter DataFrames based on event_type
purchaseDf = behaviourTbl.filter(col('EVENT_TYPE') == 'purchase')
cartDf = behaviourTbl.filter(col('EVENT_TYPE') == 'cart')
viewDf = behaviourTbl.filter(col('EVENT_TYPE') == 'view')

# First aggregation to create demand table
demandTbl = purchaseDf.groupBy("PRODUCT_ID", "EVENT_DATE").agg(
    count("EVENT_TYPE").alias("SALES"),
    mean("PRICE").alias("AVG_PRICE"),
    max("WEEK_NUMBER").alias("WEEK_NUMBER")
)

# Second aggregation
aggDemandTbl = demandTbl.groupBy("PRODUCT_ID", "WEEK_NUMBER").agg(
    count("EVENT_DATE").alias("DAYS_PURCHASED"),
    mean("AVG_PRICE").alias("AVG_PRICE"),
    sum("SALES").alias("SALES")
)

# Create Sales per week
aggDemandTbl = aggDemandTbl.withColumn("SALES", (col("SALES") / col("DAYS_PURCHASED")) * 7)

# Create Previous week number - Map WEEK_NUMBER = 1 to 52
aggDemandTbl = aggDemandTbl.withColumn(
    'PREVIOUS_WEEK_NUMBER',
    when(col("WEEK_NUMBER") == 1, 52).otherwise(col('WEEK_NUMBER') - 1)
)

# Perform a left join with itself
selfDemandTbl = aggDemandTbl.alias('DC1').join(
    aggDemandTbl.select("PRODUCT_ID", "WEEK_NUMBER", "SALES", "AVG_PRICE").alias('DC2'),
    (col('DC1.PRODUCT_ID') == col('DC2.PRODUCT_ID')) & (col('DC1.PREVIOUS_WEEK_NUMBER') == col('DC2.WEEK_NUMBER')),
    "left").select(
    col("DC1.*"), 
    col("DC2.SALES").alias("PREVIOUS_WEEK_SALES"),  
    col("DC2.AVG_PRICE").alias("PREVIOUS_WEEK_AVG_PRICE") 
).na.drop()

selfDemandTbl.cache()

behaviourTbl.filter(col('PRODUCT_ID') == '1004870').display()

# COMMAND ----------

# MAGIC %pip install xgboost
# MAGIC
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC import xgboost as xgb
# MAGIC from pyspark.ml.feature import VectorAssembler
# MAGIC from pyspark.ml.regression import LinearRegression, GBTRegressor
# MAGIC from pyspark.ml.evaluation import RegressionEvaluator
# MAGIC from scipy import stats
# MAGIC
# MAGIC # # Top 10 records with best sales
# MAGIC # top_10_sales = selfDemandTbl.orderBy(col("SALES").desc()).limit(10)
# MAGIC # # Bottom 10 records with worst sales
# MAGIC # bottom_10_sales = selfDemandTbl.orderBy(col("SALES").asc()).limit(10)
# MAGIC # filteredselfDemandTbl = top_10_sales.union(bottom_10_sales)
# MAGIC filteredselfDemandTbl = selfDemandTbl.filter(col('PRODUCT_ID').isin(['1003306', '1004227', '1004230', '1004240', '1004246']))
# MAGIC
# MAGIC # Assuming elasticity_df is a Spark DataFrame already
# MAGIC elasticity_dicts = []
# MAGIC
# MAGIC # Loop through unique product IDs
# MAGIC for product_id in filteredselfDemandTbl.select("PRODUCT_ID").distinct().rdd.flatMap(lambda x: x).collect():
# MAGIC     entry = {}
# MAGIC
# MAGIC     # Filter DataFrame for the current product
# MAGIC     entry_df = filteredselfDemandTbl.filter(col("PRODUCT_ID") == product_id)
# MAGIC
# MAGIC     # Create new columns for y and X
# MAGIC     entry_df = entry_df.withColumn("y", log(col("SALES"))) \
# MAGIC                        .withColumn("X", log(col("AVG_PRICE")))
# MAGIC
# MAGIC     # Assemble features into a vector
# MAGIC     assembler = VectorAssembler(inputCols=["X"], outputCol="features")
# MAGIC     feature_df = assembler.transform(entry_df)
# MAGIC
# MAGIC     # Fit the Linear Regression model
# MAGIC     lr = LinearRegression(featuresCol="features", labelCol="y")
# MAGIC     lr_model = lr.fit(feature_df)
# MAGIC
# MAGIC     # Get model metrics
# MAGIC     rsquared = lr_model.summary.r2
# MAGIC     # coefficient_pvalue = lr_model.summary.pValues[0] if lr_model.summary.pValues else None
# MAGIC     intercept = lr_model.intercept
# MAGIC     slope = lr_model.coefficients[0] if lr_model.coefficients else 0
# MAGIC
# MAGIC     # Collect data for p-value calculation
# MAGIC     predictions = lr_model.transform(feature_df).select("prediction", "y").toPandas()
# MAGIC     X = predictions["prediction"].values
# MAGIC     y = predictions["y"].values
# MAGIC
# MAGIC     # Calculate residuals and the standard error
# MAGIC     residuals = y - X
# MAGIC     residual_std = np.std(residuals)
# MAGIC     n = len(y)  # Number of observations
# MAGIC     p = 1  # Number of predictors
# MAGIC
# MAGIC     # Calculate t-statistic and p-value
# MAGIC     if slope != 0 and residual_std > 0:
# MAGIC         t_stat = slope / lr_model.summary.coefficientStandardErrors[0]
# MAGIC         p_value = 2 * (1 - stats.t.cdf(np.abs(t_stat), df=n-p-1))
# MAGIC         # t_statistic = slope / (residual_std / np.sqrt(n))
# MAGIC         # p_value = 2 * (1 - stats.t.cdf(np.abs(t_statistic), df= n - p - 1)) 
# MAGIC     else:
# MAGIC         t_statistic = np.nan
# MAGIC         p_value = float('nan')  # Ensure it is a float
# MAGIC
# MAGIC     # # Calculate mean values
# MAGIC     # mean_price = entry_df.agg(mean("X")).first()[0]
# MAGIC     # mean_quantity = entry_df.agg(mean("y")).first()[0]
# MAGIC
# MAGIC     # Store results
# MAGIC     print(product_id, ':', 'slope:', slope, 'R2 Score:', rsquared, 'coefficient_pvalue:', p_value)
# MAGIC     entry['PRODUCT'] = product_id
# MAGIC     entry['ELASTICITY'] = float(slope) if slope is not None else float('nan')
# MAGIC     entry['R2_SCORE'] = float(rsquared) if rsquared is not None else float('nan')
# MAGIC     entry['COEFFICIENT_PVALUE'] = float(p_value) if p_value is not None else float('nan')
# MAGIC     elasticity_dicts.append(entry)
# MAGIC
# MAGIC # Convert results to DataFrame
# MAGIC elasticity_chart = spark.createDataFrame(elasticity_dicts)
# MAGIC
# MAGIC # Show results
# MAGIC elasticity_chart.display()

# COMMAND ----------

# Create the fixed elasticity column
elasticity_chart = elasticity_chart.withColumn(
    'ELASTICITY_FIXED',
    when(elasticity_chart.COEFFICIENT_PVALUE <= 0.05, elasticity_chart.ELASTICITY)
    .otherwise(0)
)

# Update elasticity_fixed to be 0 if it is greater than or equal to 0
elasticity_chart = elasticity_chart.withColumn(
    'elasticity_fixed',
    when(elasticity_chart.ELASTICITY_FIXED < 0, elasticity_chart.ELASTICITY_FIXED)
     .otherwise(0)
)

# COMMAND ----------

# Purchase Perspective: Group by product_id and week_id, aggregating the mean price and count of events
purchaseTbl = purchaseDf.groupBy('PRODUCT_ID','SEQ_OF_WEEK_NUMBER', 'WEEK_NUMBER').agg(
    mean('PRICE').alias('MEAN_PRICE'),
    count('EVENT_TYPE').alias('SALES_OCCURRENCE')
)

# Define window specifications for rolling calculations
window_1w = Window.partitionBy('PRODUCT_ID').orderBy('SEQ_OF_WEEK_NUMBER','WEEK_NUMBER').rowsBetween(-1, 0)
window_2w = Window.partitionBy('PRODUCT_ID').orderBy('SEQ_OF_WEEK_NUMBER','WEEK_NUMBER').rowsBetween(-2, 0)
window_4w = Window.partitionBy('PRODUCT_ID').orderBy('SEQ_OF_WEEK_NUMBER','WEEK_NUMBER').rowsBetween(-4, 0)

# Calculate sales for the last 1, 2, and 4 weeks
purchaseTbl = purchaseTbl.withColumn('SALES_OCC_PREV_1W', sum(col('SALES_OCCURRENCE')).over(window_1w) - col('SALES_OCCURRENCE')) \
                         .withColumn('SALES_OCC_PREV_2W', sum(col('SALES_OCCURRENCE')).over(window_2w) - col('SALES_OCCURRENCE')) \
                         .withColumn('SALES_OCC_PREV_4W', sum(col('SALES_OCCURRENCE')).over(window_4w) - col('SALES_OCCURRENCE'))

#purchaseTbl.display()
purchaseTbl.filter(col('PRODUCT_ID') == '1004870').display()

# COMMAND ----------

# View Perspective: Group by product_id and week_id, aggregating the mean price and count of events
viewTbl = viewDf.groupBy('PRODUCT_ID', 'WEEK_NUMBER').agg(
    count('EVENT_TYPE').alias('VIEW_COUNT')
)

# Define window specifications for rolling calculations
window_1w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER').rowsBetween(-1, 0)
window_2w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER').rowsBetween(-2, 0)
window_4w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER').rowsBetween(-4, 0)

# Calculate sales for the last 1, 2, and 4 weeks
viewTbl = viewTbl.withColumn('VIEW_CNT_PREV_1W', sum(col('VIEW_COUNT')).over(window_1w) - col('VIEW_COUNT')) \
                 .withColumn('VIEW_CNT_PREV_2W', sum(col('VIEW_COUNT')).over(window_2w) - col('VIEW_COUNT')) \
                 .withColumn('VIEW_CNT_PREV_4W', sum(col('VIEW_COUNT')).over(window_4w) - col('VIEW_COUNT'))

viewTbl.display()

# COMMAND ----------

# Cart Perspective: Group by product_id and week_id, aggregating the mean price and count of events
cartTbl = cartDf.groupBy('PRODUCT_ID', 'WEEK_NUMBER').agg(
    count('EVENT_TYPE').alias('CART_COUNT')
)

# Define window specifications for rolling calculations
window_1w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER').rowsBetween(-1, 0)
window_2w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER').rowsBetween(-2, 0)
window_4w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER').rowsBetween(-4, 0)

# Calculate sales for the last 1, 2, and 4 weeks
cartTbl = cartTbl.withColumn('CART_CNT_PREV_1W', sum(col('CART_COUNT')).over(window_1w) - col('CART_COUNT')) \
                 .withColumn('CART_CNT_PREV_2W', sum(col('CART_COUNT')).over(window_2w) - col('CART_COUNT')) \
                 .withColumn('CART_CNT_PREV_4W', sum(col('CART_COUNT')).over(window_4w) - col('CART_COUNT'))

cartTbl.display()

# COMMAND ----------

# Merge DataFrames on product_id and week_id
masterTbl = purchaseDf.join(viewDf, on=['PRODUCT_ID', 'WEEK_NUMBER'], how='left') \
                      .join(cartDf, on=['PRODUCT_ID', 'WEEK_NUMBER'], how='left')

# Select relevant columns
masterTbl = masterTbl.select('WEEK_NUMBER', 'PRODUCT_ID', 'SALES', 
                             'SALES_OCC_PREV_1W', 'SALES_OCC_PREV_2W', 'SALES_OCC_PREV_4W',
                             'VIEW_CNT_PREV_1W', 'VIEW_CNT_PREV_2W', 'VIEW_CNT_PREV_4W',
                             'CART_CNT_PREV_1W', 'CART_CNT_PREV_2W', 'CART_CNT_PREV_4W'
                            )

# Calculate ratios
masterTbl = masterTbl.withColumn('SALES_1W2W_RATIO', when(col('SALES_OCC_PREV_2W') != 0, col('SALES_OCC_PREV_1W')/col('SALES_OCC_PREV_2W')).otherwise(None))\
                     .withColumn('SALES_2W2W_RATIO', when(col('SALES_OCC_PREV_4W') != 0, col('SALES_OCC_PREV_2W')/col('SALES_OCC_PREV_4W')).otherwise(None))\
                     .withColumn('SALES_1W4W_RATIO', when(col('SALES_OCC_PREV_4W') != 0, col('SALES_OCC_PREV_1W')/col('SALES_OCC_PREV_4W')).otherwise(None))\
                     .withColumn('VIEWS_1W2W_RATIO', when(col('VIEW_CNT_PREV_2W') != 0, col('VIEW_CNT_PREV_1W')/col('VIEW_CNT_PREV_2W')).otherwise(None))\
                     .withColumn('VIEWS_2W2W_RATIO', when(col('VIEW_CNT_PREV_4W') != 0, col('VIEW_CNT_PREV_2W')/col('VIEW_CNT_PREV_4W')).otherwise(None))\
                     .withColumn('VIEWS_1W4W_RATIO', when(col('VIEW_CNT_PREV_4W') != 0, col('VIEW_CNT_PREV_1W')/col('VIEW_CNT_PREV_4W')).otherwise(None))\
                     .withColumn('CARTS_1W2W_RATIO', when(col('CART_CNT_PREV_2W') != 0, col('CART_CNT_PREV_1W')/col('CART_CNT_PREV_2W')).otherwise(None))\
                     .withColumn('CARTS_2W2W_RATIO', when(col('CART_CNT_PREV_4W') != 0, col('CART_CNT_PREV_2W')/col('CART_CNT_PREV_4W')).otherwise(None))\
                     .withColumn('CARTS_1W4W_RATIO', when(col('CART_CNT_PREV_4W') != 0, col('CART_CNT_PREV_1W')/col('CART_CNT_PREV_4W')).otherwise(None))

for e in ['SALES','SALES_OCC_PREV_1W', 'SALES_OCC_PREV_2W', 'SALES_OCC_PREV_4W','VIEW_CNT_PREV_1W', 'VIEW_CNT_PREV_2W', 'VIEW_CNT_PREV_4W',\
          'CART_CNT_PREV_1W', 'CART_CNT_PREV_2W', 'CART_CNT_PREV_4W']:
    masterTbl = masterTbl.withColumn(e, log(col(e)))

# Get the maximum week number
maxWeekNum = masterTbl.agg(max("WEEK_NUMBER")).collect()[0][0]

# Define the train and test week_ids dynamically
testWeekNum = maxWeekNum
trainWeekNums = [testWeekNum - i for i in range(1, 5)]  # Last 4 weeks

# Create train and test DataFrames
trainTbl = masterTbl.filter(col("WEEK_NUMBER").isin(trainWeekNums))
testTbl = masterTbl.filter(col("WEEK_NUMBER") == testWeekNum)

# COMMAND ----------

# Sales Prediction
# Selecting features
features=['SALES_OCC_PREV_1W', 'SALES_OCC_PREV_2W', 'SALES_OCC_PREV_4W', 'VIEW_CNT_PREV_1W', 'VIEW_CNT_PREV_2W', 'VIEW_CNT_PREV_4W', 'CART_CNT_PREV_1W', 'CART_CNT_PREV_2W', 'CART_CNT_PREV_4W', 'SALES_1W2W_RATIO', 'SALES_2W2W_RATIO', 'SALES_1W4W_RATIO', 'VIEWS_1W2W_RATIO', 'VIEWS_2W2W_RATIO', 'VIEWS_1W4W_RATIO', 'CARTS_1W2W_RATIO', 'CARTS_2W2W_RATIO', 'CARTS_1W4W_RATIO']

# Convert to Pandas DataFrames (assuming data fits in memory)
train_pd = trainTbl.select(features + ['sales']).toPandas()
test_pd = testTbl.select(features + ['sales']).toPandas()

# Assemble the features into a single vector column
vector_assembler = VectorAssembler(inputCols=features, outputCol='features')
train_vector = vector_assembler.transform(trainTbl)
test_vector = vector_assembler.transform(testTbl)

# Your exact XGBoost training setup
clf = xgb.XGBRegressor(n_estimators=1000, learning_rate=0.005, random_state=42)

# Fit with evaluation sets
clf.fit(
    train_pd[features],
    train_pd['sales'],
    eval_set=[(train_pd[features], train_pd['sales']),
               (test_pd[features], test_pd['sales'])],
    eval_metric='rmse',
    verbose=True,
    early_stopping_rounds=20
)




# COMMAND ----------

# Make predictions
test_pd['prediction'] = clf.predict(test_pd[features])
# Convert back to Spark DataFrame if needed
predictions = spark.createDataFrame(test_pd[['sales', 'prediction']])
test['sales_real'] = np.exp(test['sales'])
test['sales_prediction'] = np.exp(test['prediction']).astype('int')

# Select relevant columns from test and purchase_df
estimate = test.select('WEEK_NUMBER', 'PRODUCT_ID', 'sales_prediction') \
               .join(purcahseDf.select('WEEK_NUMBER', 'PRODUCT_ID', 'SALES', 'PRICE'), 
                     on=['PRODUCT_ID', 'WEEK_NUMBER'], how='left')

# Merge with elasticity_chart
estimate = estimate.join(elasticity_chart, 
                         estimate.PRODUCT_ID == elasticity_chart.PRODUCT, 
                         how='left') \
                   .drop(elasticity_chart.PRODUCT)  # Drop duplicate column if needed

estimate.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



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

# Delta Record
delta_table_path = curPath_raw + '/Transaction/DA_RFM_Analysis/'
RFM_result = spark.read.format("delta").load(delta_table_path)
outputDeltaTran(RFM_result, curPath_raw + "/Transaction/DA_RFM_Analysis/") 

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



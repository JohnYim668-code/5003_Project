# Databricks notebook source
# MAGIC %pip install xgboost

# COMMAND ----------

import numpy as np
import pandas as pd
import xgboost as xgb

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col, first

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV
from sklearn.ensemble import GradientBoostingRegressor

from scipy import stats
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import TimeSeriesSplit


# COMMAND ----------

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

# MAGIC %run ../1_COMMON/NB_1005_TABLE_CREATION

# COMMAND ----------

# MAGIC %run ../1_COMMON/NB_1006_BATCH_START

# COMMAND ----------

text = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
item = text.replace(dbwPath+'4_DA&ML/','')

# COMMAND ----------

# batchID = getBatchIdForOutput(item)
# batchID = int(batchID)
batchID = int(1)

# COMMAND ----------


behaviourTbl = spark.read.format("delta").load('abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/2_history/behaviour_log/')

# 4 MONTHS TOTAL 233460662

# COMMAND ----------

# Load behaviourTbl from Delta table
behaviourTbl = spark.read.format("delta").load('abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/2_history/behaviour_log/')

# Select and cast columns to string

selected_df = behaviourTbl.select(
    col("PRODUCT_ID").alias("PRODUCT_ID"),
    col("CATEGORY_CODE").alias("CATEGORY_CODE"),
    col("BRAND").alias("BRAND")
).filter(col("PRODUCT_ID").isin(['1002544', '4804056', '1004258', '1003317', '1002524', '4804718', '1005135', '1004250', '1005116', '1004230']))


# COMMAND ----------

behaviourTbl = spark.read.format("delta").load('abfss://edp-bdc@bigdatacompute01.dfs.core.windows.net/3_curated/Transaction/Fact_Behaviour_Log')
#behaviourTbl.count()


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


# COMMAND ----------

# Creating the week number column
behaviourTbl = behaviourTbl.withColumn("WEEK_NUMBER", weekofyear(to_timestamp(behaviourTbl.EVENT_DATE)))

# Creating the year column
behaviourTbl = behaviourTbl.withColumn("YEAR", year(to_timestamp(behaviourTbl.EVENT_DATE)))

# Get minimum year
min_year = behaviourTbl.agg(min(year(to_timestamp("EVENT_DATE"))).alias("MIN_YEAR")).collect()[0]["MIN_YEAR"]

# Add seq_of_week_no column
behaviourTbl = behaviourTbl.withColumn(
    "WEEK_NUMBER_CYCLE",
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

# COMMAND ----------


filteredselfDemandTbl = selfDemandTbl.filter(col('PRODUCT_ID').isin(['1002544', '4804056', '1004258', '1003317', '1002524', '4804718', '1005135', '1004250', '1005116', '1004230']))

# Assuming elasticity_df is a Spark DataFrame already
elasticity_dicts = []

# Loop through unique product IDs
for product_id in filteredselfDemandTbl.select("PRODUCT_ID").distinct().rdd.flatMap(lambda x: x).collect():
    entry = {}

    # Filter DataFrame for the current product
    entry_df = filteredselfDemandTbl.filter(col("PRODUCT_ID") == product_id)

    # Create new columns for y and X
    entry_df = entry_df.withColumn("y", log(col("SALES"))) \
                       .withColumn("X", log(col("AVG_PRICE")))

    # Assemble features into a vector
    assembler = VectorAssembler(inputCols=["X"], outputCol="features")
    feature_df = assembler.transform(entry_df)

    # Fit the Linear Regression model
    lr = LinearRegression(featuresCol="features", labelCol="y")
    lr_model = lr.fit(feature_df)

    # Get model metrics
    rsquared = lr_model.summary.r2
    intercept = lr_model.intercept
    slope = lr_model.coefficients[0] if lr_model.coefficients else 0

    # Collect data for p-value calculation
    predictions = lr_model.transform(feature_df).select("prediction", "y").toPandas()
    X = predictions["prediction"].values
    y = predictions["y"].values

    # Calculate residuals and the standard error
    residuals = y - X
    residual_std = np.std(residuals)
    n = len(y)  # Number of observations
    p = 1  # Number of predictors

    # Calculate t-statistic and p-value
    if slope != 0 and residual_std > 0:
        t_stat = slope / lr_model.summary.coefficientStandardErrors[0]
        p_value = 2 * (1 - stats.t.cdf(np.abs(t_stat), df=n-p-1))
    else:
        t_statistic = np.nan
        p_value = float('nan')  # Ensure it is a float

    # Store results
    print(product_id, ':', 'slope:', slope, 'R2 Score:', rsquared, 'coefficient_pvalue:', p_value)
    entry['PRODUCT'] = product_id
    entry['ELASTICITY'] = float(slope) if slope is not None else float('nan')
    entry['R2_SCORE'] = float(rsquared) if rsquared is not None else float('nan')
    entry['COEFFICIENT_PVALUE'] = float(p_value) if p_value is not None else float('nan')
    elasticity_dicts.append(entry)

# Convert results to DataFrame
elasticity_chart = spark.createDataFrame(elasticity_dicts)

# Specify the new column order
new_column_order = ['Product',  'Elasticity', 'R2_Score', 'Coefficient_PValue']  # replace with your actual column names

# Reorder the DataFrame
elasticity_chart_lr = elasticity_chart.select(new_column_order)

# Show the reordered DataFrame
elasticity_chart_lr.display()


# COMMAND ----------


filteredselfDemandTbl = selfDemandTbl.filter(col('PRODUCT_ID').isin(['1002544', '4804056', '1004258', '1003317', '1002524', '4804718', '1005135', '1004250', '1005116', '1004230']))

# Assuming elasticity_df is a Spark DataFrame already
elasticity_dicts = []

# Loop through unique product IDs
for product_id in filteredselfDemandTbl.select("PRODUCT_ID").distinct().rdd.flatMap(lambda x: x).collect():
    entry = {}

    # Filter DataFrame for the current product
    entry_df = filteredselfDemandTbl.filter(col("PRODUCT_ID") == product_id)

   # Create new columns for y and features
    entry_df = entry_df.withColumn("y", log(col("SALES"))) \
                       .withColumn("X", log(col("AVG_PRICE"))) \
                       .withColumn("log_previous_sales", log(col("PREVIOUS_WEEK_SALES") + 1)) \
                       .withColumn("log_previous_price", log(col("PREVIOUS_WEEK_AVG_PRICE") + 1))

    # Assemble features into a vector
    assembler = VectorAssembler(
        inputCols=["X", "log_previous_sales", "log_previous_price"], 
        outputCol="features"
    )
    feature_df = assembler.transform(entry_df)

    # --- Linear Regression ---
    # Fit the Linear Regression model
    lr = LinearRegression(featuresCol="features", labelCol="y")
    lr_model = lr.fit(feature_df)

    # Get Linear Regression metrics
    lr_rsquared = lr_model.summary.r2
    lr_intercept = lr_model.intercept
    lr_slope = lr_model.coefficients[0] if lr_model.coefficients else 0

    # Collect data for p-value calculation
    lr_predictions = lr_model.transform(feature_df).select("prediction", "y").toPandas()
    lr_X = lr_predictions["prediction"].values
    lr_y = lr_predictions["y"].values

    # Calculate residuals and p-value for Linear Regression
    lr_residuals = lr_y - lr_X
    lr_residual_std = np.std(lr_residuals)
    n = len(lr_y)
    p = 1

    if lr_slope != 0 and lr_residual_std > 0:
        lr_t_stat = lr_slope / lr_model.summary.coefficientStandardErrors[0]
        lr_p_value = 2 * (1 - stats.t.cdf(np.abs(lr_t_stat), df=n-p-1))
    else:
        lr_t_stat = np.nan
        lr_p_value = float('nan')

    print(f"{product_id} (Linear Regression): slope: {lr_slope}, R2 Score: {lr_rsquared}, coefficient_pvalue: {lr_p_value}")
    entry['PRODUCT'] = product_id
    entry['ELASTICITY'] = float(lr_slope) if lr_slope is not None else float('nan')
    entry['R2_SCORE'] = float(lr_rsquared) if lr_rsquared is not None else float('nan')
    entry['COEFFICIENT_PVALUE'] = float(lr_p_value) if lr_p_value is not None else float('nan')
    elasticity_dicts.append(entry.copy())


# Convert results to DataFrame
elasticity_chart = spark.createDataFrame(elasticity_dicts)

# Specify the new column order
new_column_order = ['Product', 'Elasticity', 'R2_Score', 'Coefficient_PValue']  # replace with your actual column names

# Reorder the DataFrame
elasticity_chart_mlr = elasticity_chart.select(new_column_order)

# Show the reordered DataFrame
elasticity_chart_mlr.display()


# COMMAND ----------

# Purchase Perspective: Group by product_id and week_id, aggregating the mean price and count of events

purchaseDf = purchaseDf.filter(col('PRODUCT_ID').isin(['1002544', '4804056', '1004258', '1003317', '1002524', '4804718', '1005135', '1004250', '1005116', '1004230'
]))
purchaseTbl = purchaseDf.groupBy('PRODUCT_ID','WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').agg(
    mean('PRICE').alias('MEAN_PRICE'),
    count('EVENT_TYPE').alias('SALES_OCCURRENCE')
)

# Define window specifications for rolling calculations
window_1w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE','WEEK_NUMBER').rowsBetween(-1, 0)
window_2w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE','WEEK_NUMBER').rowsBetween(-2, 0)
window_4w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE','WEEK_NUMBER').rowsBetween(-4, 0)

# Calculate sales for the last 1, 2, and 4 weeks
purchaseTbl = purchaseTbl.withColumn('SALES_OCC_PREV_1W', sum(col('SALES_OCCURRENCE')).over(window_1w) - col('SALES_OCCURRENCE')) \
                         .withColumn('SALES_OCC_PREV_2W', sum(col('SALES_OCCURRENCE')).over(window_2w) - col('SALES_OCCURRENCE')) \
                         .withColumn('SALES_OCC_PREV_4W', sum(col('SALES_OCCURRENCE')).over(window_4w) - col('SALES_OCCURRENCE'))
                           

# COMMAND ----------

purchaseTbl.display()

# COMMAND ----------

# View Perspective: Group by product_id and week_id, aggregating the mean price and count of events

viewDf = viewDf.filter(col('PRODUCT_ID').isin(['1002544', '4804056', '1004258', '1003317', '1002524', '4804718', '1005135', '1004250', '1005116', '1004230'
 ]))

viewTbl = viewDf.groupBy('PRODUCT_ID', 'WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').agg(
    count('EVENT_TYPE').alias('VIEW_COUNT')
)

# Define window specifications for rolling calculations
window_1w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').rowsBetween(-1, 0)
window_2w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').rowsBetween(-2, 0)
window_4w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').rowsBetween(-4, 0)

# Calculate sales for the last 1, 2, and 4 weeks
viewTbl = viewTbl.withColumn('VIEW_CNT_PREV_1W', sum(col('VIEW_COUNT')).over(window_1w) - col('VIEW_COUNT')) \
                 .withColumn('VIEW_CNT_PREV_2W', sum(col('VIEW_COUNT')).over(window_2w) - col('VIEW_COUNT')) \
                 .withColumn('VIEW_CNT_PREV_4W', sum(col('VIEW_COUNT')).over(window_4w) - col('VIEW_COUNT'))
                            

#viewTbl.filter(col('PRODUCT_ID') == '1004870').display()

# COMMAND ----------

# Cart Perspective: Group by product_id and week_id, aggregating the mean price and count of events

cartDf = cartDf.filter(col('PRODUCT_ID').isin(['1002544', '4804056', '1004258', '1003317', '1002524', '4804718', '1005135', '1004250', '1005116', '1004230'
]))

cartTbl = cartDf.groupBy('PRODUCT_ID', 'WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').agg(
    count('EVENT_TYPE').alias('CART_COUNT')
)

# Define window specifications for rolling calculations
window_1w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').rowsBetween(-1, 0)
window_2w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').rowsBetween(-2, 0)
window_4w = Window.partitionBy('PRODUCT_ID').orderBy('WEEK_NUMBER_CYCLE', 'WEEK_NUMBER').rowsBetween(-4, 0)

# Calculate sales for the last 1, 2, and 4 weeks
cartTbl = cartTbl.withColumn('CART_CNT_PREV_1W', sum(col('CART_COUNT')).over(window_1w) - col('CART_COUNT')) \
                 .withColumn('CART_CNT_PREV_2W', sum(col('CART_COUNT')).over(window_2w) - col('CART_COUNT')) \
                 .withColumn('CART_CNT_PREV_4W', sum(col('CART_COUNT')).over(window_4w) - col('CART_COUNT')) 
                            

# COMMAND ----------

# Merge DataFrames on product_id and week_id
masterTbl = purchaseTbl.join(viewTbl, on=['PRODUCT_ID', 'WEEK_NUMBER_CYCLE', 'WEEK_NUMBER'], how='left') \
                      .join(cartTbl, on=['PRODUCT_ID', 'WEEK_NUMBER_CYCLE', 'WEEK_NUMBER'], how='left')
# Select relevant columns
masterTbl = masterTbl.select('WEEK_NUMBER_CYCLE', 'WEEK_NUMBER', 'PRODUCT_ID', 'SALES_OCCURRENCE', 
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

for e in ['SALES_OCCURRENCE','SALES_OCC_PREV_1W', 'SALES_OCC_PREV_2W', 'SALES_OCC_PREV_4W','VIEW_CNT_PREV_1W', 'VIEW_CNT_PREV_2W', 'VIEW_CNT_PREV_4W',\
          'CART_CNT_PREV_1W', 'CART_CNT_PREV_2W', 'CART_CNT_PREV_4W']:
    masterTbl = masterTbl.withColumn(e, log(col(e)))

# Get the maximum week cycle
maxCycleNum = masterTbl.agg(max("WEEK_NUMBER_CYCLE")).collect()[0][0]

# Get the maximum week number for the maximum cycle
maxWeekNum = masterTbl.filter(col("WEEK_NUMBER_CYCLE") == maxCycleNum).agg(max("WEEK_NUMBER")).collect()[0][0]

# Define the train and test week_ids dynamically
testWeekNum = maxWeekNum
testCycleNum = maxCycleNum 
trainWeekNums =[4,3,2,1] # Last 4 weeks

# Create train and test DataFrames
trainTbl = masterTbl.filter((col("WEEK_NUMBER").isin(trainWeekNums)) ) 
testTbl = masterTbl.filter((col("WEEK_NUMBER") == testWeekNum)  & (col("WEEK_NUMBER_CYCLE") == testCycleNum))


# COMMAND ----------

#xgboost with CV, Tuning

# Sales Prediction
# Selecting features
features=['SALES_OCC_PREV_1W', 'SALES_OCC_PREV_2W', 'SALES_OCC_PREV_4W', 'VIEW_CNT_PREV_1W', 'VIEW_CNT_PREV_2W', 'VIEW_CNT_PREV_4W', 'CART_CNT_PREV_1W', 'CART_CNT_PREV_2W', 'CART_CNT_PREV_4W', 'SALES_1W2W_RATIO', 'SALES_2W2W_RATIO', 'SALES_1W4W_RATIO', 'VIEWS_1W2W_RATIO', 'VIEWS_2W2W_RATIO', 'VIEWS_1W4W_RATIO', 'CARTS_1W2W_RATIO', 'CARTS_2W2W_RATIO', 'CARTS_1W4W_RATIO']

# Convert to Pandas DataFrames (assuming data fits in memory)
train_pd = trainTbl.select(features + ['SALES_OCCURRENCE']).toPandas()
test_pd = testTbl.select(features + ['SALES_OCCURRENCE']).toPandas()

# Your exact XGBoost training setup
base_clf = xgb.XGBRegressor(random_state=42, eval_metric='rmse', early_stopping_rounds=50)
param_dist = {
    'n_estimators': [500, 1000, 1500, 2000],
    'learning_rate': [0.005, 0.01, 0.05, 0.1],
    'max_depth': [3, 6, 8, 10],
    'min_child_weight': [1, 3, 5],
    'subsample': [0.6, 0.8, 1.0],
    'colsample_bytree': [0.6, 0.8, 1.0],
    'gamma': [0, 0.1, 0.3],
    'reg_lambda': [0, 1, 10]
}
random_search = RandomizedSearchCV(
    base_clf, param_dist, n_iter=20, scoring='neg_root_mean_squared_error', cv=5, n_jobs=-1, random_state=42
)
# Fit with evaluation sets
random_search.fit(
    train_pd[features],
    train_pd['SALES_OCCURRENCE'],
    eval_set=[(train_pd[features], train_pd['SALES_OCCURRENCE']),
              (test_pd[features], test_pd['SALES_OCCURRENCE'])],
    verbose=False
)
 
clf = random_search.best_estimator_
print("Best Parameters:", random_search.best_params_)
print("Best CV RMSE (log scale):", -random_search.best_score_)

# COMMAND ----------

# Make predictions -XGBoost

test_pd['prediction'] = clf.predict(test_pd[features])

predictions = spark.createDataFrame(test_pd[['SALES_OCCURRENCE', 'prediction']])
test_pd['sales_real'] = np.exp(test_pd['SALES_OCCURRENCE'])
test_pd['sales_prediction'] = np.exp(test_pd['prediction']).astype('int')

# Add back 'WEEK_NUMBER', 'PRODUCT_ID' to test_pd
test_pd['WEEK_NUMBER'] = testTbl.select("WEEK_NUMBER").rdd.flatMap(lambda x: x).collect()
test_pd['PRODUCT_ID'] = testTbl.select("PRODUCT_ID").rdd.flatMap(lambda x: x).collect()

# Change purcahseDtl to Panda DF
purchase_test = purchaseTbl.toPandas()

# Select relevant columns from test and purchase_df
estimate_xg = pd.merge( test_pd[['WEEK_NUMBER', 'PRODUCT_ID', 'sales_prediction']] ,  purchase_test [['WEEK_NUMBER', 'PRODUCT_ID', 'SALES_OCCURRENCE', 'MEAN_PRICE']], on=['PRODUCT_ID', 'WEEK_NUMBER'],how='left' )

estimate_xg.display()

# COMMAND ----------

#xgboost

test_rmse_xg = np.sqrt(mean_squared_error(test_pd['SALES_OCCURRENCE'],test_pd['prediction']))

print(f'Test RMSE: {test_rmse_xg }')

# COMMAND ----------


#GradientBoosting with CV, Tuning

# Sales Prediction
# Selecting features
features=['SALES_OCC_PREV_1W', 'SALES_OCC_PREV_2W', 'SALES_OCC_PREV_4W', 'VIEW_CNT_PREV_1W', 'VIEW_CNT_PREV_2W', 'VIEW_CNT_PREV_4W', 'CART_CNT_PREV_1W', 'CART_CNT_PREV_2W', 'CART_CNT_PREV_4W', 'SALES_1W2W_RATIO', 'SALES_2W2W_RATIO', 'SALES_1W4W_RATIO', 'VIEWS_1W2W_RATIO', 'VIEWS_2W2W_RATIO', 'VIEWS_1W4W_RATIO', 'CARTS_1W2W_RATIO', 'CARTS_2W2W_RATIO', 'CARTS_1W4W_RATIO']

# Convert to Pandas DataFrames (assuming data fits in memory)
train_pd = trainTbl.select(features + ['SALES_OCCURRENCE']).toPandas()
test_pd = testTbl.select(features + ['SALES_OCCURRENCE']).toPandas()

# Initialize GradientBoostingRegressor model
base_clf = GradientBoostingRegressor(random_state=42, verbose=0)

# Define parameter grid
param_dist = {
    'n_estimators': [500, 1000, 1500, 2000],
    'learning_rate': [0.005, 0.01, 0.05, 0.1],
    'max_depth': [3, 6, 8, 10],
    'min_samples_split': [2, 5, 10],
    'subsample': [0.6, 0.8, 1.0],
    'max_features': ['sqrt', 'log2', None]
}

# Initialize RandomizedSearchCV with temporal CV
random_search = RandomizedSearchCV(
    estimator=base_clf,
    param_distributions=param_dist,
    n_iter=20,
    scoring='neg_root_mean_squared_error',
    cv=TimeSeriesSplit(n_splits=5),
    verbose=1,
    n_jobs=-1,
    random_state=42
)

# Fit model
random_search.fit(
    train_pd[features],
    train_pd['SALES_OCCURRENCE']
)

# Best model
clf = random_search.best_estimator_
print("GradientBoosting Best Parameters:", random_search.best_params_)
print("GradientBoosting Best CV RMSE (log scale):", -random_search.best_score_)


# COMMAND ----------

# Make predictions - Gradient Boosting

test_pd['prediction'] = clf.predict(test_pd[features])
# Convert back to Spark DataFrame if needed
predictions = spark.createDataFrame(test_pd[['SALES_OCCURRENCE', 'prediction']])
test_pd['sales_real'] = np.exp(test_pd['SALES_OCCURRENCE'])
test_pd['sales_prediction'] = np.exp(test_pd['prediction']).astype('int')

# Add back 'WEEK_NUMBER', 'PRODUCT_ID' to test_pd
test_pd['WEEK_NUMBER'] = testTbl.select("WEEK_NUMBER").rdd.flatMap(lambda x: x).collect()
test_pd['PRODUCT_ID'] = testTbl.select("PRODUCT_ID").rdd.flatMap(lambda x: x).collect()

# Change purcahseDtl to Panda DF
purchase_test = purchaseTbl.toPandas()

# Select relevant columns from test and purchase_df
estimate_gb = pd.merge( test_pd[['WEEK_NUMBER', 'PRODUCT_ID', 'sales_prediction']] ,  purchase_test [['WEEK_NUMBER', 'PRODUCT_ID', 'SALES_OCCURRENCE', 'MEAN_PRICE']], on=['PRODUCT_ID', 'WEEK_NUMBER'],how='left' )

estimate_gb.display()

# COMMAND ----------

#GradientBoosting

#test_rmse_gd = np.sqrt(mean_squared_error(test_pd['sales_real'], test_pd['sales_prediction']))

test_rmse_gb = np.sqrt(mean_squared_error(test_pd['SALES_OCCURRENCE'],test_pd['prediction']))

print(f'Test RMSE: {test_rmse_gb}')

# COMMAND ----------

#Random Forest Regressor


features=['SALES_OCC_PREV_1W', 'SALES_OCC_PREV_2W', 'SALES_OCC_PREV_4W', 'VIEW_CNT_PREV_1W', 'VIEW_CNT_PREV_2W', 'VIEW_CNT_PREV_4W', 'CART_CNT_PREV_1W', 'CART_CNT_PREV_2W', 'CART_CNT_PREV_4W', 'SALES_1W2W_RATIO', 'SALES_2W2W_RATIO', 'SALES_1W4W_RATIO', 'VIEWS_1W2W_RATIO', 'VIEWS_2W2W_RATIO', 'VIEWS_1W4W_RATIO', 'CARTS_1W2W_RATIO', 'CARTS_2W2W_RATIO', 'CARTS_1W4W_RATIO']

# Convert to Pandas DataFrames (assuming data fits in memory)
train_pd = trainTbl.select(features + ['SALES_OCCURRENCE']).toPandas()
test_pd = testTbl.select(features + ['SALES_OCCURRENCE']).toPandas()


# Initialize RandomForestRegressor
base_clf = RandomForestRegressor(random_state=42)

# Define parameter grid
param_dist = {
    'n_estimators': [100, 300, 500, 1000],
    'max_depth': [5, 10, 15, 20],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4],
    'max_features': ['auto', 'sqrt', 0.5],
    'bootstrap': [True, False]
}

# Initialize RandomizedSearchCV
random_search = RandomizedSearchCV(
    estimator=base_clf,
    param_distributions=param_dist,
    n_iter=20,
    scoring='neg_root_mean_squared_error',
    cv=5,
    verbose=1,
    n_jobs=-1,
    random_state=42
)

# Fit with cross-validation
random_search.fit(
    train_pd[features],
    train_pd['SALES_OCCURRENCE']
)

# Best parameters and score
clf = random_search.best_estimator_
print("RandomForest Best Parameters:", random_search.best_params_)
print("RandomForest Best CV RMSE (log scale):", -random_search.best_score_)


# COMMAND ----------

# Make predictions - Random Forest

test_pd['prediction'] = clf.predict(test_pd[features])
# Convert back to Spark DataFrame if needed
predictions = spark.createDataFrame(test_pd[['SALES_OCCURRENCE', 'prediction']])
test_pd['sales_real'] = np.exp(test_pd['SALES_OCCURRENCE'])
test_pd['sales_prediction'] = np.exp(test_pd['prediction']).astype('int')

# Add back 'WEEK_NUMBER', 'PRODUCT_ID' to test_pd
test_pd['WEEK_NUMBER'] = testTbl.select("WEEK_NUMBER").rdd.flatMap(lambda x: x).collect()
test_pd['PRODUCT_ID'] = testTbl.select("PRODUCT_ID").rdd.flatMap(lambda x: x).collect()

# Change purcahseDtl to Panda DF
purchase_test = purchaseTbl.toPandas()

# Select relevant columns from test and purchase_df
estimate_rf = pd.merge( test_pd[['WEEK_NUMBER', 'PRODUCT_ID', 'sales_prediction']] ,  purchase_test [['WEEK_NUMBER', 'PRODUCT_ID', 'SALES_OCCURRENCE', 'MEAN_PRICE']], on=['PRODUCT_ID', 'WEEK_NUMBER'],how='left' )

estimate_rf.display()

# COMMAND ----------

#Random Forest Regressor

test_rmse_rf = np.sqrt(mean_squared_error(test_pd['SALES_OCCURRENCE'],test_pd['prediction']))

print(f'Test RMSE: {test_rmse_rf }')


# COMMAND ----------

estimate_df = estimate_xg.merge(
    estimate_gb,
    on=['WEEK_NUMBER', 'PRODUCT_ID'],
    suffixes=('_xg', '_gb')
).merge(
    estimate_rf,
    on=['WEEK_NUMBER', 'PRODUCT_ID'],
    suffixes=('', '_rf')  # The first DataFrame will retain original names
)
estimate_df = estimate_df.drop(columns=['SALES_OCCURRENCE_xg','SALES_OCCURRENCE_gb','MEAN_PRICE_xg','MEAN_PRICE_gb' ])

estimate_df = estimate_df.rename(columns={'sales_prediction': 'sales_prediction_rf'})

estimate_df['WEEK_NUMBER_CYCLE'] = 2
estimate_df['WEEK_NUMBER_CYCLE'] = estimate_df['WEEK_NUMBER_CYCLE'].astype('long')

# Display the merged DataFrame
desired_order = ['WEEK_NUMBER_CYCLE', 'WEEK_NUMBER', 'PRODUCT_ID','MEAN_PRICE', 'SALES_OCCURRENCE','sales_prediction_xg', 'sales_prediction_gb', 'sales_prediction_rf']
estimate_df = estimate_df[desired_order]

estimate_df = estimate_df.rename(columns={'sales_prediction_xg': 'SALES_PREDICTION_XG', 'sales_prediction_gb': 'SALES_PREDICTION_GB', 'sales_prediction_rf': 'SALES_PREDICTION_RF','SALES_OCCURRENCE':'ACTUAL_SALES' })

display(estimate_df)

# COMMAND ----------

# Merge with elasticity_chart

elasticity_pd = elasticity_chart_mlr.toPandas()
elasticity_pd = elasticity_pd.rename(columns={'Elasticity': 'ELASTICITY', 'R2_Score': 'LR_R2_SCORE', 'Coefficient_PValue': 'COEFFICIENT_PVALUE'})

# Merge with elasticity_chart using left join
estimate_elastic = estimate_df.merge(elasticity_pd, 
                          left_on='PRODUCT_ID', 
                          right_on='Product', 
                          how='left')

# Drop the duplicate 'PRODUCT' column 
estimate_elastic = estimate_elastic.drop(columns=['Product'])

# Union the past sales data
purchaseTbl_output = purchaseTbl.select("WEEK_NUMBER_CYCLE", "WEEK_NUMBER", "PRODUCT_ID","MEAN_PRICE","SALES_OCCURRENCE").toPandas()
purchaseTbl_output = purchaseTbl_output.rename(columns={'SALES_OCCURRENCE': 'ACTUAL_SALES'})  

new_sale_columns = ['SALES_PREDICTION_XG', 'SALES_PREDICTION_GB', 'SALES_PREDICTION_RF', 'ELASTICITY', 'LR_R2_SCORE', 'COEFFICIENT_PVALUE']
for col in new_sale_columns:
    purchaseTbl_output[col] = 'n/a'

# Perform union using pd.concat
estimate_elastic = pd.concat([purchaseTbl_output, estimate_elastic], ignore_index=True)

estimate_elastic = estimate_elastic[ ~( (estimate_elastic['WEEK_NUMBER_CYCLE'] == 2) & (estimate_elastic['WEEK_NUMBER'] == 5) & (estimate_elastic['SALES_PREDICTION_XG'] == 'n/a'))].sort_values(by=['WEEK_NUMBER_CYCLE','WEEK_NUMBER'])

display(estimate_elastic)
#estimate_elastic[estimate_elastic['product'] == '1002544']

# COMMAND ----------


estimate_elastic = spark.createDataFrame(estimate_elastic)
estimate_elastic.cache()


# COMMAND ----------

spark.catalog.dropTempView("temporary_table")
estimate_elastic.createTempView('temporary_table')
tableName = "ml_sales_prediction"
if spark._jsparkSession.catalog().tableExists("curated." + tableName):
    print("Exist")
else:
    print("Not Exist")
    estimate_elastic.filter("1!=1").write.mode("overwrite").partitionBy('PRODUCT_ID')\
                                  .save(curPath_tran + folderName)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO curated.ml_sales_prediction AS target
# MAGIC     USING temporary_table source
# MAGIC   ON target.PRODUCT_ID = source.PRODUCT_ID and target.WEEK_NUMBER = source.WEEK_NUMBER
# MAGIC   WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# # Delta Record
# delta_table_path = curPath_raw + '/Transaction/ML_Sales_Prediction/'
# ML_result = spark.read.format("delta").load(delta_table_path)
# outputDeltaTran(ML_result, curPath_raw + "/Transaction/ML_Sales_Prediction/") 

# COMMAND ----------

completeBatch(item, batchID)

# COMMAND ----------



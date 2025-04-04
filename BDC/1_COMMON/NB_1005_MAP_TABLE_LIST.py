# Databricks notebook source
 from pyspark.sql.types import *

# COMMAND ----------

#update table from landing to history
historyTableList = [
    'behviour_log'
]

# COMMAND ----------

# # deltaDF, targetTB, BK, SK, source, tableName
# # update table from history to curated

# curatedTableList = [
#     ('Customer',['CustomersV3', 'DimAttributeCompanyInfos', 'EDP_LineOfBusiness_Entity']),
#     ('SalesRep', ['EmployeesV2', 'EDP_CommissionSalesRep_Entity', 'DimAttributeCompanyInfos']),
#     ('DirParty', ['EDP_DirPartyTableExportMGT_Entity', 'DimAttributeCompanyInfos']),
#     ('Project', ['Projects', 'DimAttributeCompanyInfos']),
#     ('ItemTable', ['EDP_InventTableV2_Entity', 'DimAttributeCompanyInfos', 'EDP_InventItemBrand_Entity', 'EDP_EcoResProduct_Entity']),#, 'EDP_InventTableV2_Entity_SecondCompany'
#     ('SalesOrderHeader', ['SalesOrderHeadersV2', 'ReturnOrderHeaders', 'DimAttributeCompanyInfos', 'EDP_SalesTableStatus_Entity']),
#     ('SalesOrderLine', ['SalesOrderLines', 'ReturnOrderLines', 'DimAttributeCompanyInfos', 'EDP_SalesTableStatus_Entity', 'EDP_SalesLineStatus_Entity']),
#     ('SalesOrderLineReturn', ['ReturnOrderLines', 'DimAttributeCompanyInfos', 'EDP_SalesLineStatus_Entity', 'ReturnOrderHeaders']),
#     ('BudgetItem',['DimAttributeCompanyInfos']),
#     ('BudgetSalesRep',['DimAttributeCompanyInfos']), 
#     ('ServiceTarget',['DimAttributeCompanyInfos']),
#     ('ItemLocation',['EDP_InventLocation_Entity', 'DimAttributeCompanyInfos', 'EDP_InventLocationGroup_Entity']),
#     ('Vendor',['VendorsV2', 'DimAttributeCompanyInfos', 'EDP_LineOfBusiness_Entity']),
#     ('StockOnHand',['EDP_InventSum_Entity', 'DimAttributeCompanyInfos']),
#     ('StockAging',['EDP_InventAgingTmp_Entity', 'DimAttributeCompanyInfos']), 
#     ('ItemTransLine',['EDP_InventTrans_Entity', 'DimAttributeCompanyInfos','EDP_InventDim_Entity']),
#     ('ItemTransHeader',['DimAttributeCompanyInfos', 'EDP_InventTransOrigin_Entity']), 
#     ('PurchHeader',['EDP_PurchTable_Entity', 'DimAttributeCompanyInfos']),  
#     ('PurchLine',['EDP_PurchLine_Entity', 'DimAttributeCompanyInfos']), 
#     ('Warranty',['EDP_SMAWarrantyTable_Entity', 'DimAttributeCompanyInfos']),
#     ('ServiceOrderHeader',['EDP_SMAServiceOrderTable_Entity', 'DimAttributeCompanyInfos']),
#     ('ServiceOrderLine',['EDP_SMAServiceOrderTableLine_Entity', 'DimAttributeCompanyInfos']),
#     ('SalesInvoice', ['EDP_CustInvoiceJourTrans_Entity', 'DimAttributeCompanyInfos', 'Historical']), # 'EDP_CustInvoiceJourTrans_Entity_SecondCompany'
#     ('ExchangeRate', ['ExchangeRates']),
#     ('Category', ['EDP_EcoResCategory_Entity']),
#     ('ServiceProblem', ['EDP_SMAServiceProblem_Entity', 'DimAttributeCompanyInfos']),
#     ('ServiceSolution', ['EDP_SMAServiceSolution_Entity', 'DimAttributeCompanyInfos']),
#     ('ServiceDefect', ['EDP_SMAServiceDefect_Entity', 'DimAttributeCompanyInfos']),
#     ('PartsConsumption', ['EDP_SMAServiceOrderAddService_Entity', 'DimAttributeCompanyInfos']),
#     ('FiscalDate', ['EDP_FiscalPeriodDate_Entity', 'DimAttributeCompanyInfos']),
#     ('LogisticsPostalAddress', ['EDP_LogisticsPostalAddress_Entity'])
  
# ]

# inv_list = [
#     'Historical'
# ]

# schema = StructType([
#     StructField('TransTable', StringType(), True),
#     StructField('ReqTables', ArrayType(StringType()), True)
# ])

# mapDf = spark.createDataFrame(data = curatedTableList, schema = schema)

# COMMAND ----------

# # deltaDF, targetTB, BK, SK, source, tableName
# # update table from history to curated

# curatedTableList = [
#     ('rfidpcl_mo', ['rfidpcl_mo' + '_' + region]),
#     ('rfidipp_mo', ['rfidipp_mo' + '_' + region]),
#     ('rfidpcl_sku', ['rfidpcl_sku' + '_' + region]),
#     ('rfidipp_sku', ['rfidipp_sku' + '_' + region]),
#     ('rfidpcl_qclog', ['rfidpcl_qclog' + '_' + region]),
#     ('rfidipp_qclog', ['rfidipp_qclog' + '_' + region]),
#     ('rfidpcl_sku_time_log', ['rfidpcl_sku_time_log' + '_' + region]),
#     ('rfidipp_sku_time_log', ['rfidipp_sku_time_log' + '_' + region]),
#     ('rfidpcl_epc', ['rfidpcl_epc' + '_' + region]),
#     ('rfidipp_epc', ['rfidipp_epc' + '_' + region]),
#     ('rfidpcl_machine', ['rfidpcl_machine' + '_' + region]),
#     ('rfidipp_machine', ['rfidipp_machine' + '_' + region]),
#     ('rfidpcl_user', ['rfidpcl_user' + '_' + region]),
#     ('rfidipp_user', ['rfidipp_user' + '_' + region]),
#     ('rfidpcl_epc_ADHOC', ['rfidpcl_epc' + '_' + region, 'rfidpcl_sku' + '_' + region])
# ]

# schema = StructType([
#     StructField('TransTable', StringType(), True),
#     StructField('ReqTables', ArrayType(StringType()), True)
# ])

# mapDf = spark.createDataFrame(data = curatedTableList, schema = schema)

# COMMAND ----------

# def createMultiTbl(TransTable):
#     final_df = {}
#     table_list = mapDf.filter(col("TransTable") == TransTable).collect()[0][1]
#     if ((region != 'dawn') and (region != 'morning') and (region != 'afternoon')):
#         for reqtable in table_list:
#             final_df[reqtable] = spark.table("history." + reqtable)
#             MaxId = final_df[reqtable].select([max("BatchNumber")]).collect()[0][0]
#             final_df[reqtable] = final_df[reqtable].filter(col('BatchNumber')== MaxId)
#     else: 
#         for reqtable in table_list:
#             final_df[reqtable] = spark.table("history." + reqtable)
#     return final_df

# COMMAND ----------



# Databricks notebook source
# DBTITLE 1,Error Logging function
def WriteToLog(dataSourceName,notebookName,notebookCellTitle,ErrorDescription):
  import datetime
  now = datetime.datetime.now()
  df=spark.createDataFrame([
    (dataSourceName,notebookName,notebookCellTitle,ErrorDescription,now)
  ])
  df.write.mode("append").insertInto("default.tbl_ErrorLog")
  del df

# COMMAND ----------

# DBTITLE 1,Cell Number Increment For Error Logging
try:
  var_cellNumber = 2
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)  

# COMMAND ----------

# DBTITLE 1,Spark Cache & PyArrow Configuration
try:
  spark.conf.set("spark.sql.execution.arrow.enabled", "true")
  spark.conf.set("spark.databricks.io.cache.enabled", "true")
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data pre-processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                      

# COMMAND ----------

# DBTITLE 1,Loading libraries
try:
  import time
  import math 
  import datetime
  import pandas as pd
  import numpy as np
  from pyspark.sql import *
  from pyspark.sql.types import *
  from pyspark.sql.functions import *
  from pyspark.sql.types import DateType,FloatType,StringType,LongType,BooleanType,TimestampType,NumericType,IntegerType
  from datetime import datetime as dt
  from pyspark.sql.functions import *
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)  

# COMMAND ----------

# DBTITLE 1,Harcoded values
adls_client_id = dbutils.secrets.get(scope = 'ColumbiaMexico', key = 'csadlsclientid')
adls_credential = dbutils.secrets.get(scope = 'ColumbiaMexico', key = 'csadlscredential')
  
#variables for intermediate storage location when writing to sql dw
temp_blob_storage_account = 'chamastorageaccount1'
temp_blob_storage_container ='latem-container-test'  
temp_blob_dw_load_key = dbutils.secrets.get(scope = 'ColumbiaMexico', key = 'csblobfordwloadkey')
  
#variables related to sql dw
sql_server_pswd = dbutils.secrets.get(scope = 'ColumbiaMexico', key = 'cssqldwpwd')
sql_server='us6chamaanalyticsql0001.database.windows.net'
sql_server_port='1433'
sql_server_db= 'us6chamaanalyticsdw0001'
sql_server_login = 'SQLDBWriter'
  
#DW Tables Related to Columbia
dw_product_dim_table = 'NorthCluster.Colombia_dim_product'
dw_customer_dim_table = 'NorthCluster.Colombia_dim_customer'
dw_pricing_dim_table = 'NorthCluster.Colombia_dim_pricing'

#Fact Table
dw_fact_table = 'NorthCluster.Colombia_Fact_SellOut_Incremental'
partition_location_previous = (datetime.datetime.today() - datetime.timedelta(days=220)).strftime('%Y/%Y-%b/')
partition_location = (datetime.datetime.today() - datetime.timedelta(days=190)).strftime('%Y/%Y-%b/')


# COMMAND ----------

print(partition_location_previous)
print(partition_location)

# COMMAND ----------

# DBTITLE 1,Initialise variables
try:
  #Variables to connect to sql dw
  #sql_server = getArgument("sql_server")
  #sql_server_port = getArgument("sql_server_port")
  #sql_server_db = getArgument("sql_server_db")
  #sql_server_login = getArgument("sql_server_login")
  #sql_server_pswd = getArgument("sql_server_pswd")
  #temp_blob_dw_load_key =getArgument("temp_blob_dw_load_key")
  #dw_fact_table = getArgument("dw_fact_table")
  
  
  #variables for intermediate storage location when writing to sql dw
  #temp_blob_storage_account = getArgument("temp_blob_storage_account")
  #emp_blob_storage_container = getArgument("temp_blob_storage_container")
  #temp_blob_dw_load_key= getArgument("temp_blob_dw_load_key")
  #partition_location = getArgument("partition_location")
  #partition_location = (datetime.datetime.today() - datetime.timedelta(days=148)).strftime('%Y/%Y-%b/')
  
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)  

# COMMAND ----------

# DBTITLE 1,Spark Configuration
try:
    sc._jsc.hadoopConfiguration().set("fs.azure.account.key." + temp_blob_storage_account + ".blob.core.windows.net",temp_blob_dw_load_key)
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc._jsc.hadoopConfiguration().set("csv.enable.summary-metadata", "false")
    var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)    

# COMMAND ----------

# DBTITLE 1,Read Inventory pre-processed data
try:
  
#Current Month
  Colombia_Inventory_Template_Processed=spark.read.format("csv").options(header='true',multiLine='true',ignoreTrailingWhiteSpace='true',inferSchema='true',ignoreLeadingWhiteSpace='true').load("/mnt/Americas/Conformed/LATAM/NorthernCluster/Colombia/Transaction/Inventory/" + partition_location).withColumn("ADLS_Path_Nm",input_file_name()).withColumnRenamed('REPRESENTANTE','Sales_Rep_Nm').withColumnRenamed('CLIENTES','Client_Nm').withColumnRenamed('Rota','Return_Cnt').withColumnRenamed('inv','Inventory_Cnt').withColumnRenamed('DESCRIPCION','UPC_Desc').withColumnRenamed('MARCA','Brand_Nm').withColumnRenamed('EAN','Item_National_Cd')
  
#Previous Month
  Colombia_Inventory_Template_Processed_Previous=spark.read.format("csv").options(header='true',multiLine='true',ignoreTrailingWhiteSpace='true',inferSchema='true',ignoreLeadingWhiteSpace='true').load("/mnt/Americas/Conformed/LATAM/NorthernCluster/Colombia/Transaction/Inventory/" + partition_location_previous).withColumn("ADLS_Path_Nm",input_file_name()).withColumnRenamed('REPRESENTANTE','Sales_Rep_Nm').withColumnRenamed('CLIENTES','Client_Nm').withColumnRenamed('Rota','Return_Cnt').withColumnRenamed('inv','Inventory_Cnt').withColumnRenamed('DESCRIPCION','UPC_Desc').withColumnRenamed('MARCA','Brand_Nm').withColumnRenamed('EAN','Item_National_Cd')
  
#union of previous & current month
  Colombia_Inventory_Template_Processed = Colombia_Inventory_Template_Processed.union(Colombia_Inventory_Template_Processed_Previous)
  
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)      

# COMMAND ----------

# DBTITLE 1,Loading Offer Product Factors 
try: 
  Colombia_Product_Factor_Processed=spark.read.format("csv").options(header='true',multiLine='true',ignoreTrailingWhiteSpace='true',inferSchema='true',ignoreLeadingWhiteSpace='true').load("/mnt/Americas/Conformed/LATAM/NorthernCluster/Colombia/Transaction/Product Factor/").withColumnRenamed('UPC_Desc','Product_Factor_UPC_Desc').where(col('GMN Offer')!='Inactivo').distinct()
  
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e) 

# COMMAND ----------

# DBTITLE 1,  Changing format of File_Template_Dt  to date format of Colombia_Inventory_Template_Processed DF
try:

  Colombia_Inventory_Template_Processed = Colombia_Inventory_Template_Processed.withColumn('File_Template_Dt',col('File_Template_Dt').cast(StringType()))
  Colombia_Inventory_Template_Processed = Colombia_Inventory_Template_Processed.withColumn('File_Template_Dt',lpad(Colombia_Inventory_Template_Processed['File_Template_Dt'], 8, '0'))
  Colombia_Inventory_Template_Processed = Colombia_Inventory_Template_Processed.withColumn('File_Template_Dt',from_unixtime(unix_timestamp('File_Template_Dt', 'MMddyyy')))
  Colombia_Inventory_Template_Processed = Colombia_Inventory_Template_Processed.withColumn('File_Template_Dt',col('File_Template_Dt').cast('date'))
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)        

# COMMAND ----------

# DBTITLE 1,Adding row number column
try:
  from pyspark.sql.functions import monotonically_increasing_id
  DT = Colombia_Inventory_Template_Processed.select('File_Template_Dt')
  DT = DT.distinct()

  DT = DT.orderBy('File_Template_Dt', ascending=False)
  DT = DT.withColumn("Id", monotonically_increasing_id()+1)
  w = Window.orderBy("Id")
  DT = DT.withColumn("ID", row_number().over(w))
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Aggregation to avoid duplicate records
#There are multiple records for a single transaction. Logic has been applied to sum up the values to show one single record for a single transaction
try:
  Colombia_Inventory_Template_Processed = Colombia_Inventory_Template_Processed.withColumnRenamed('COD','UPC')
  Colombia_Inventory_Template_Processed_no_duplicates=Colombia_Inventory_Template_Processed.groupBy('Client_Id','Sales_Rep_Nm','Client_Nm','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','ADLS_Path_Nm','Src_Data_File_Nm','Src_Nm','Template_Region_Nm','File_Template_Dt','Job_Load_Dt').agg(sum('Return_Cnt').alias('Return_Cnt'),sum('Inventory_Cnt').alias('Inventory_Cnt'))
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)     

# COMMAND ----------

# DBTITLE 1,Calculating initial inventory
#Initial inventory for the current month is obtained from the final inventory of the previous month. This is achieved using 2 data frames.
#create two data frames - one for the latest month (A) and one for the previous month (B) (next cell). A is left joined with B to obtain the final inventory #from the previous month and assign that value to initial inventory of the latest month
try:
  df_A=Colombia_Inventory_Template_Processed_no_duplicates.select('Client_Id','Sales_Rep_Nm','Client_Nm','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','Inventory_Cnt','Return_Cnt','ADLS_Path_Nm','Src_Data_File_Nm','Src_Nm','Template_Region_Nm','File_Template_Dt','Job_Load_Dt')

  df_A = df_A.withColumn('Inventory_Cnt',when((df_A['Inventory_Cnt']=='')|(df_A['Inventory_Cnt'].isNull()),0).otherwise((df_A['Inventory_Cnt'])))
  df_A = df_A.withColumn('Return_Cnt',when((df_A['Return_Cnt']=='')|(df_A['Return_Cnt'].isNull()),0).otherwise((df_A['Return_Cnt'])))

  df_A = df_A.where(col('File_Template_Dt')== DT.select('File_Template_Dt').first()[0])
  #Rearranging the Columns Position
  #df_A=Colombia_Inventory_Template_Processed_no_duplicates.select('Client_Id','Sales_Rep_Nm','Client_Nm','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','Inventory_Cnt','Return_Cnt','ADLS_Path_Nm','Src_Data_File_Nm','Src_Nm','Template_Region_Nm','File_Template_Dt','Job_Load_Dt')

  #df_A = df_A.withColumn('Inventory_Cnt',when((df_A['Inventory_Cnt']=='')|(df_A['Inventory_Cnt'].isNull()),0).otherwise((df_A['Inventory_Cnt'])))
  #df_A = df_A.withColumn('Return_Cnt',when((df_A['Return_Cnt']=='')|(df_A['Return_Cnt'].isNull()),0).otherwise((df_A['Return_Cnt'])))

  #df_A = df_A.where(col('File_Template_Dt')== DT.select('File_Template_Dt').first()[0])
  
  #var_cellNumber = var_cellNumber + 1
  
  df_B=Colombia_Inventory_Template_Processed_no_duplicates.select('Client_Id','Sales_Rep_Nm','Client_Nm','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','Inventory_Cnt','Return_Cnt','ADLS_Path_Nm','Src_Data_File_Nm','Src_Nm','Template_Region_Nm','File_Template_Dt','Job_Load_Dt')

  df_B=df_B.withColumnRenamed('Client_Id','Client_Id_B').withColumnRenamed('Sales_Rep_Nm','Sales_Rep_Nm_B').withColumnRenamed('Client_Nm','Client_Nm_B').withColumnRenamed('UPC','UPC_B').withColumnRenamed('Item_National_Cd','Item_National_Cd_B').withColumnRenamed('Brand_Nm','Brand_Nm_B').withColumnRenamed('UPC_Desc','UPC_Desc_B').withColumnRenamed('ADLS_Path_Nm','ADLS_Path_Nm_B').withColumnRenamed('Src_Data_File_Nm','Src_Data_File_Nm_B').withColumnRenamed('Src_Nm','Src_Nm_B').withColumnRenamed('Template_Region_Nm','Template_Region_Nm_B').withColumnRenamed('File_Template_Dt','File_Template_Dt_B').withColumnRenamed('Job_Load_Dt', 'Job_Load_Dt_B').withColumnRenamed('Return_Cnt','Return_Cnt_B').withColumnRenamed('Inventory_Cnt','Inventory_Cnt_B')

  df_B = df_B.withColumn('Inventory_Cnt_B',when((df_B['Inventory_Cnt_B']=='')|(df_B['Inventory_Cnt_B'].isNull()),0).otherwise((df_B['Inventory_Cnt_B'])))
  df_B = df_B.withColumn('Return_Cnt_B',when((df_B['Return_Cnt_B']=='')|(df_B['Return_Cnt_B'].isNull()),0).otherwise((df_B['Return_Cnt_B'])))

  Second_Date = DT.select('File_Template_Dt').where(col('ID')==2)
  df_B = df_B.where(col('File_Template_Dt_B')== Second_Date.select('File_Template_Dt').first()[0])
  df_B = df_B.drop('File_Template_Dt_B')
  
  #dataframe to get records in B but not in A
  df_B_only = df_B.join(df_A,(df_B['Client_Id_B']==df_A['Client_Id']),'left_anti')
  
  df_B_only = df_B_only.withColumn('Inventory_Cnt',lit(0))
  
  df_B_only = df_B_only.drop('File_Template_Dt_B')
  
  df_B_only = df_B_only.withColumn('File_Template_Dt',lit(DT.select('File_Template_Dt').first()[0]))
  
  df_B_only = df_B_only.select('Client_Id_B','Sales_Rep_Nm_B','Client_Nm_B','UPC_B','Item_National_Cd_B','Brand_Nm_B','UPC_Desc_B','Inventory_Cnt','Inventory_Cnt_B','Return_Cnt_B','ADLS_Path_Nm_B','Src_Data_File_Nm_B','Src_Nm_B','Template_Region_Nm_B','File_Template_Dt','Job_Load_Dt_B')
    
  df = df_A.join(df_B,(df_A['Client_Id']==df_B['Client_Id_B'])&(df_A['UPC']==df_B['UPC_B']),'left')
  
  df = df.select('Client_Id','Sales_Rep_Nm','Client_Nm','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','Inventory_Cnt','Inventory_Cnt_B','Return_Cnt','ADLS_Path_Nm','Src_Data_File_Nm','Src_Nm','Template_Region_Nm','File_Template_Dt','Job_Load_Dt')
  
  df = df.union(df_B_only)
   
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)  

# COMMAND ----------

# DBTITLE 1,Read Sell in pre-processed data
try:
  Colombia_Sellin_Processed = spark.read.format("csv").options(header='true',multiLine='true',ignoreTrailingWhiteSpace='true',inferSchema='true',
    ignoreLeadingWhiteSpace='true').load("/mnt/Americas/Conformed/LATAM/NorthernCluster/Colombia/Transaction/Sellin/" + partition_location).withColumn("ADLS_Path_Nm", input_file_name()).withColumnRenamed('COD HIJO','Client_id').withColumnRenamed('GMM OFERTA','UPC').withColumnRenamed('Venta (Und)','Sale_Units').select('Client_id','UPC','Sale_Units','File_Template_Dt').distinct()
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)       

# COMMAND ----------

# DBTITLE 1,Format File Template Date
try:
  Colombia_Sellin_Processed = Colombia_Sellin_Processed.withColumn('File_Template_Dt',col('File_Template_Dt').cast(StringType()))
  Colombia_Sellin_Processed = Colombia_Sellin_Processed.withColumn('File_Template_Dt',lpad(Colombia_Sellin_Processed['File_Template_Dt'], 8, '0'))
  Colombia_Sellin_Processed = Colombia_Sellin_Processed.withColumn('File_Template_Dt',from_unixtime(unix_timestamp('File_Template_Dt', 'MMddyyy')))
  Colombia_Sellin_Processed = Colombia_Sellin_Processed.withColumn('File_Template_Dt',col('File_Template_Dt').cast('date'))
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Filtering On Latest Date
try:
  Colombia_Sellin_Processed = Colombia_Sellin_Processed.where(Colombia_Sellin_Processed['File_Template_Dt']== DT.select('File_Template_Dt').first()[0] )
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Format and aggregate Sales Units Column To Sellin
try:
  Colombia_Sellin_Processed =Colombia_Sellin_Processed.withColumn('Sale_Units',when(col('Sale_Units')=='-',0).when(col('Sale_Units').isNull(),0).otherwise(col('Sale_Units'))).withColumn('Sale_Units',when(col('Sale_Units').contains(','),regexp_replace(col('Sale_Units'),',','')).otherwise(col('Sale_Units'))).withColumn('Sale_Units',translate(col('Sale_Units'),"(",'-')).withColumn('Sale_Units',translate(col('Sale_Units'),")",''))

  Colombia_Sellin_Processed = Colombia_Sellin_Processed.withColumn('Sale_Units',col('Sale_Units').cast(DoubleType()))
  SP = Colombia_Sellin_Processed.groupBy('Client_Id','UPC','File_Template_Dt').agg(sum('Sale_Units').alias('Sellin'))
  SP = SP.withColumnRenamed('Client_Id','Client_Id_SP').withColumnRenamed('File_Template_Dt','File_Template_Dt_SP').withColumnRenamed('UPC','UPC_SP')
  
  SP = SP.withColumn('Client_Id_SP',col('Client_Id_SP').cast(IntegerType()))
  SP = SP.withColumn('UPC_SP',ltrim(rtrim(col('UPC_SP'))))
  
  
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)    

# COMMAND ----------

# DBTITLE 1,Intermediate sell out data frame creation
try:
  RS = df.join(SP,(df['Client_Id']== SP['Client_Id_SP'])&(df['UPC']==SP['UPC_SP']),'left')
  
  RS = RS.withColumn('Sellin',when(col('Sellin').isNull(),0).otherwise(col('Sellin'))).withColumn('Sellin',col('Sellin').cast(DoubleType()))
  
  RS = RS.withColumn('Inventory_Cnt',when(col('Inventory_Cnt').isNull(),0).otherwise(col('Inventory_Cnt'))).withColumn('Inventory_Cnt',col('Inventory_Cnt').cast(DoubleType()))
    
  RS = RS.withColumn('Inventory_Cnt_B',when(col('Inventory_Cnt_B').isNull(),0).otherwise(col('Inventory_Cnt_B'))).withColumn('Inventory_Cnt_B',col('Inventory_Cnt_B').cast(DoubleType()))
  
  RS=RS.groupBy('File_Template_Dt','Sales_Rep_Nm','Client_Id','Client_Nm','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','Return_Cnt').agg(sum('Inventory_Cnt').alias('CurrentMonth_IF'),sum('Sellin').alias('Sellin'),sum('Inventory_Cnt_B').alias('CurrentMonth_II'),(sum('Inventory_Cnt_B')+sum('Sellin')-sum('Inventory_Cnt')).alias('SellOut'))
  
  RS=RS.withColumn('SellOut',when(col('Client_Id').isin(['678991','678934']),col('Return_Cnt')).otherwise(col('SellOut'))).drop('Return_Cnt')

  RS = RS.withColumn('YEAR',year(col('File_Template_Dt')))

  First_Date = DT.select('File_Template_Dt').first()[0]
  RS = RS.withColumn('month_column',date_format(lit(First_Date),'MMMM'))

  RS = RS.withColumn('Feed_Column',lit(' Feed'))
  RS =RS.withColumn('Src_Data_File_Nm',concat(col('month_column'),col('Feed_Column')))

  RS = RS.withColumn('Src_Nm',lit('Colombia Monthly Feed')).withColumn('Job_Created_Dt',current_date())

  RS = RS.withColumnRenamed('File_Template_Dt','Transaction_Dt')

  RS=RS.select('Transaction_Dt','YEAR','Sales_Rep_Nm','Client_Id','Client_Nm','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','CurrentMonth_II','Sellin','CurrentMonth_IF','SellOut','Src_Data_File_Nm','Src_Nm','Job_Created_Dt')
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Loading Pricing Master 
try:
  CP=spark.read.format("csv").options(header='true',multiLine='true',ignoreTrailingWhiteSpace='true',inferSchema='true',ignoreLeadingWhiteSpace='true').load("/mnt/Americas/Conformed/LATAM/NorthernCluster/Colombia/Master/Price/").withColumn("ADLS_Path_Nm", input_file_name())
  
  #.withColumnRenamed('TIPO PRECIO','Price_Type_Cd').withColumnRenamed('Referencia','Reference_Cd').withColumnRenamed('SKU','SKU_Desc').withColumnRenamed('PRECIO','Product_Pricing_Amt').withColumnRenamed('ADLS_Path_Nm','ADLS_Stage_Path_Nm').withColumnRenamed('job_load_dt','Job_Load_Dt')
  #Max_Year = CP.select(max(col('Year')).alias('Year')).collect()
  #Year = [row.Year for row in Max_Year]
  #CP=CP.where(col('Year')==Year[0]).select('Id','Price_Type_Cd','Reference_Cd','SKU_Desc','Product_Pricing_Amt','ADLS_Stage_Path_Nm','Job_Load_Dt')
  CP = CP.distinct()
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)    

# COMMAND ----------

# DBTITLE 1,Product Factor Preprocessing
try:
  Colombia_Product_Factor_Processed = Colombia_Product_Factor_Processed.select('GMN Offer','Factor').distinct()
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Product Factor Preprocessing', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Product Factor Join
try:
  df_Colombia_Fact_SellOut = RS.join(CP,(RS['Client_Id']==CP['Id'])&(RS['UPC']==CP['Offer_Reference_Cd']),'left')
  
  df_Colombia_Fact_SellOut = df_Colombia_Fact_SellOut.join(Colombia_Product_Factor_Processed,df_Colombia_Fact_SellOut['UPC']==Colombia_Product_Factor_Processed['GMN Offer'],'left')
  
  df_Colombia_Fact_SellOut =df_Colombia_Fact_SellOut.withColumn('Factor',when(col('Factor').isNull(),1).otherwise(col('Factor')))
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Duplicating Rows Using Factor Column
try:
  Factor_Array = udf(lambda n : [n] * n, ArrayType(IntegerType()))
  df_Colombia_Fact_SellOut = df_Colombia_Fact_SellOut.withColumn('Array_Factor', Factor_Array(df_Colombia_Fact_SellOut.Factor))
  #df_Colombia_Fact_SellOut.select('Array_Factor').distinct().show()
  df_Colombia_Fact_SellOut = df_Colombia_Fact_SellOut.withColumn('Array_Factor', explode(df_Colombia_Fact_SellOut.Array_Factor))
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Columbia Fact Sellout Table
try:
  #df_Colombia_Fact_SellOut = df_Colombia_Fact_SellOut.withColumn('CurrentMonth_II',col('CurrentMonth_II')*col('Factor')).withColumn('Sellin',col('Sellin')*col('Factor')).withColumn('CurrentMonth_IF',col('CurrentMonth_IF')*col('Factor')).withColumn('SellOut',col('SellOut')*col('Factor'))
  
  df_Colombia_Fact_SellOut = df_Colombia_Fact_SellOut.withColumn('CurrentMonth_II_Values',col('CurrentMonth_II')*col('Product_Pricing_Amt')).withColumn('Sellin_Values',col('Sellin')*col('Product_Pricing_Amt')).withColumn('CurrentMonth_IF_Values',col('CurrentMonth_IF')*col('Product_Pricing_Amt')).withColumn('SellOut_Values',col('SellOut')*col('Product_Pricing_Amt'))

  df_Colombia_Fact_SellOut = df_Colombia_Fact_SellOut.select('Transaction_Dt','YEAR','Sales_Rep_Nm','Client_Id','Client_Nm','Offer_Reference_Cd','UPC','Item_National_Cd','Brand_Nm','UPC_Desc','CurrentMonth_II','Sellin','CurrentMonth_IF','SellOut','CurrentMonth_II_Values','Sellin_Values','CurrentMonth_IF_Values','SellOut_Values','Src_Data_File_Nm','Src_Nm','Job_Created_Dt','Product_Pricing_Amt')
  
  df_Colombia_Fact_SellOut = df_Colombia_Fact_SellOut.withColumn('Sellin_Values',col('Sellin_Values')/1000000).withColumn('SellOut_Values',col('SellOut_Values')/1000000).withColumn('CurrentMonth_II_Values',col('CurrentMonth_II_Values')/1000000).withColumn('CurrentMonth_IF_Values',col('CurrentMonth_IF_Values')/1000000)
  
  df_Colombia_Fact_SellOut.createOrReplaceTempView("TempColumbiaFactSelloutCurrentMonth")
  #Colombia_Fact_SellOut.write.saveAsTable('FactSelloutTest')
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)                                                                                     

# COMMAND ----------

# DBTITLE 1,Insert into Fact Sell out Hive table
# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE NorthCluster.Colombia_Fact_SellOut;
# MAGIC 
# MAGIC DELETE FROM NorthCluster.Colombia_Fact_SellOut
# MAGIC WHERE Transaction_Dt IN (select Transaction_Dt from TempColumbiaFactSelloutCurrentMonth group by Transaction_Dt);
# MAGIC   
# MAGIC INSERT INTO NorthCluster.Colombia_Fact_SellOut
# MAGIC    SELECT * 
# MAGIC      FROM TempColumbiaFactSelloutCurrentMonth
# MAGIC     WHERE Transaction_Dt IS NOT NULL; 

# COMMAND ----------

# DBTITLE 1,Write sell out data into collated layer partition
try:
  aggr_value = df_Colombia_Fact_SellOut.select('Transaction_Dt').distinct().orderBy('Transaction_Dt').collect()
  for date1 in aggr_value:
    df_Fact_Sellout_Semantic = spark.sql("select * from TempColumbiaFactSelloutCurrentMonth where Transaction_Dt = '" + str(date1[0]) + "'")
    df_Fact_Sellout_Semantic.coalesce(1).write.mode("overwrite").option("header", "true").parquet("/mnt/Americas/Semantic/LATAM/NorthernCluster/Colombia/Fact_Sellout/" + "Transaction_Dt=" + str(date1[0]))
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)   

# COMMAND ----------

# DBTITLE 1,Changing Double Datatype to Decimal
df_Fact_Sellout_Semantic= df_Fact_Sellout_Semantic.withColumn('CurrentMonth_II',col('CurrentMonth_II').cast(DecimalType(10,3))).withColumn('Sellin',col('Sellin').cast(DecimalType(10,3))).withColumn('CurrentMonth_IF',col('CurrentMonth_IF').cast(DecimalType(10,3))).withColumn('SellOut',col('SellOut').cast(DecimalType(10,3))).withColumn('CurrentMonth_II_Values',col('CurrentMonth_II_Values').cast(DecimalType(10,3))).withColumn('Sellin_Values',col('Sellin_Values').cast(DecimalType(10,3))).withColumn('CurrentMonth_IF_Values',col('CurrentMonth_IF_Values').cast(DecimalType(10,3))).withColumn('SellOut_Values',col('SellOut_Values').cast(DecimalType(10,3))).withColumn('Product_Pricing_Amt',col('Product_Pricing_Amt').cast(DecimalType(10,3)))

# COMMAND ----------

# DBTITLE 1,Write sell out data to SQL Data Warehouse
try:
  df_Fact_Sellout_Semantic.write \
      .format("com.databricks.spark.sqldw") \
      .option("url",'jdbc:sqlserver://' + sql_server + ':' + sql_server_port + ';database='+ sql_server_db + ';user=' + sql_server_login + ';password=' + sql_server_pswd) \
      .option("forwardSparkAzureStorageCredentials", "true") \
      .option("dbTable", dw_fact_table) \
      .option("tempDir", "wasbs://" + temp_blob_storage_container + "@" + temp_blob_storage_account + ".blob.core.windows.net/") \
      .mode("overwrite") \
      .save()
  var_cellNumber = var_cellNumber + 1
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction data final processing', 'cmd '+str(var_cellNumber), str(e))
  raise(e)   

# COMMAND ----------


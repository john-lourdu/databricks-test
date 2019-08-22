# Databricks notebook source
# DBTITLE 1,Parameters from ADF
try:
  dbutils.widgets.text("prm_file_path", "","")
  dbutils.widgets.get("prm_file_path")
  prm_file_path = getArgument("prm_file_path") 
  
  dbutils.widgets.text("prm_AlmacenesLa14Sa_CM", "","")
  dbutils.widgets.get("prm_AlmacenesLa14Sa_CM")
  prm_AlmacenesLa14Sa_CM = getArgument("prm_AlmacenesLa14Sa_CM")  
  
  dbutils.widgets.text("prm_AlmacenesLa14Sa_PM", "","")
  dbutils.widgets.get("prm_AlmacenesLa14Sa_PM")
  prm_AlmacenesLa14Sa_PM = getArgument("prm_AlmacenesLa14Sa_PM")    
  
  dbutils.widgets.text("prm_CentralRegion_CM", "","")
  dbutils.widgets.get("prm_CentralRegion_CM")
  prm_CentralRegion_CM = getArgument("prm_CentralRegion_CM") 
  
  dbutils.widgets.text("prm_CentralRegion_PM", "","")
  dbutils.widgets.get("prm_CentralRegion_PM")
  prm_CentralRegion_PM = getArgument("prm_CentralRegion_PM") 
  
  dbutils.widgets.text("prm_CentralRetailRegion_CM", "","")
  dbutils.widgets.get("prm_CentralRetailRegion_CM")
  prm_CentralRetailRegion_CM = getArgument("prm_CentralRetailRegion_CM")
  
  dbutils.widgets.text("prm_CruzVerde_CM", "","")
  dbutils.widgets.get("prm_CruzVerde_CM")
  prm_CruzVerde_CM = getArgument("prm_CruzVerde_CM")
  
  dbutils.widgets.text("prm_CruzVerde_PM", "","")
  dbutils.widgets.get("prm_CruzVerde_PM")
  prm_CruzVerde_PM = getArgument("prm_CruzVerde_PM")
  
  dbutils.widgets.text("prm_ExitoAndCencosud_CM", "","")
  dbutils.widgets.get("prm_ExitoAndCencosud_CM")
  prm_ExitoAndCencosud_CM = getArgument("prm_ExitoAndCencosud_CM")
  
  dbutils.widgets.text("prm_Locatel_CM", "","")
  dbutils.widgets.get("prm_Locatel_CM")
  prm_Locatel_CM = getArgument("prm_Locatel_CM")
  
  dbutils.widgets.text("prm_ModernRegion_PM", "","")
  dbutils.widgets.get("prm_ModernRegion_PM")
  prm_ModernRegion_PM = getArgument("prm_ModernRegion_PM")
  
  dbutils.widgets.text("prm_NorthRegion_CM", "","")
  dbutils.widgets.get("prm_NorthRegion_CM")
  prm_NorthRegion_CM = getArgument("prm_NorthRegion_CM")
  
  dbutils.widgets.text("prm_NorthRegion_PM", "","")
  dbutils.widgets.get("prm_NorthRegion_PM")
  prm_NorthRegion_PM = getArgument("prm_NorthRegion_PM")
  
  dbutils.widgets.text("prm_Pacifico_CM", "","")
  dbutils.widgets.get("prm_Pacifico_CM")
  prm_Pacifico_CM = getArgument("prm_Pacifico_CM")
  
  dbutils.widgets.text("prm_WestRegion_PM", "","")
  dbutils.widgets.get("prm_WestRegion_PM")
  prm_WestRegion_PM = getArgument("prm_WestRegion_PM")
  
  dbutils.widgets.text("prm_Copservir_CM", "","")
  dbutils.widgets.get("prm_Copservir_CM")
  prm_Copservir_CM = getArgument("prm_Copservir_CM")
  
  dbutils.widgets.text("prm_Olimpica_CM", "","")
  dbutils.widgets.get("prm_Olimpica_CM")
  prm_Olimpica_CM = getArgument("prm_Olimpica_CM")
  
  dbutils.widgets.text("prm_sellin", "","")
  dbutils.widgets.get("prm_sellin")
  prm_sellin = getArgument("prm_sellin")  
    
  dbutils.widgets.text("prm_productfactor", "","")
  dbutils.widgets.get("prm_productfactor")
  prm_productfactor = getArgument("prm_productfactor") 
  
except Exception as e:
  WriteToLog('Colombia', 'Colombia Transaction', 'cmd '+str(var_cellNumber), str(e))
  raise(e)

# COMMAND ----------

# DBTITLE 1,Error Log Function
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

# DBTITLE 1,Importing Libraries
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

# DBTITLE 1,Hardcoded values
#prm_file_path = '2019/2019-Jan/'
prm_AlmacenesLa14Sa_CM = "Sell Out Base la 14 - Enero.xlsx"
prm_AlmacenesLa14Sa_PM = "Sell Out Base la 14 - Diciembre.xlsx"
prm_CentralRegion_CM = "Formato Cargue Inv Region Centro - Enero.xlsx"
prm_CentralRegion_PM = "Formato Cargue Inv Region Centro - Diciembre.xlsx"
prm_CentralRetailRegion_CM = "Formato Cargue Inv Retail Centro - Enero.xlsx"
prm_CruzVerde_CM = "Sell Out Base Cruz Verde - Febrero.xlsx"
prm_CruzVerde_PM = "Sell Out Base Cruz Verde - Diciembre.xlsx"
prm_ExitoAndCencosud_CM = "Formato Cargue Inv EXITO CENCOSUD - Enero.xlsx"
prm_Locatel_CM = "Formato Cargue Inv Locatel - Enero.xlsx"
prm_ModernRegion_PM = "Formato Cargue Inv Region Moderno - Diciembre.xlsx"
prm_NorthRegion_CM = "Formato Cargue Inv Region Norte - Enero.xlsx"
prm_NorthRegion_PM = "Formato Cargue Inv Region Norte - Diciembre.xlsx"
prm_Pacifico_CM = "Formato Cargue Inv R Pacifico - Enero.xlsx"
prm_WestRegion_PM = "Formato Cargue Inv Region Occidente - Diciembre.xlsx"
prm_Copservir_CM  = "Formato Cargue Inv Copservir -  Enero.xlsx"
prm_Olimpica_CM = "Formato Cargue Inv Olimpica - Enero.xlsx"
prm_sellin="Sell In Enero 2019 GSK CH.xlsx"
prm_productfactor="Base Reporte.xlsx"

# COMMAND ----------

# DBTITLE 1,Get filepath for current & previous month
prm_file_path_previous = (datetime.datetime.today() - datetime.timedelta(days=220)).strftime('%Y/%Y-%b/')
prm_file_path = (datetime.datetime.today() - datetime.timedelta(days=190)).strftime('%Y/%Y-%b/')

# COMMAND ----------

# DBTITLE 1,Get year for current & previous month
CM_file_year = prm_file_path[5:]
CM_year = CM_file_year[:4]
CM_file_year = CM_file_year.replace('/','')
if CM_file_year.find("Jan") != -1:
  PM_year = int(CM_year) - 1
else:
  PM_year = int(CM_year)  

# COMMAND ----------

# DBTITLE 1,Get Monthnumber 
import calendar
from datetime import date, timedelta
from calendar import monthrange
from dateutil.relativedelta import relativedelta

#function to get monthnumber
def to_monthnumber(monthname):
      if monthname == "Jan": return '01'
      elif monthname == "Feb": return '02'
      elif monthname == "Mar": return '03'
      elif monthname == "Apr": return '04'
      elif monthname == "May": return '05'
      elif monthname == "Jun": return '06'
      elif monthname == "Jul": return '07'
      elif monthname == "Aug": return '08'
      elif monthname == "Sep": return '09'
      elif monthname == "Oct": return '10'
      elif monthname == "Nov": return '11'
      elif monthname == "Dec": return '12'
      
#get monthname
monthname = prm_file_path[-4:]
monthname = monthname.replace('/','')

#get monthnumber      
year = prm_file_path[:4]      
monthnumber = to_monthnumber(monthname)

#get lastday of currentmonth
lastday = monthrange(int(year), int(monthnumber))[1]

#get lastday of previousmonth
getlastdate_previousmonth = date(int(year),int(monthnumber),1)+relativedelta(days=-1)
getlastdate_previousmonth = str(getlastdate_previousmonth)[-2:]

#get monthnumber of previous month
getlastdateofpreviousmonth = date(int(year),int(monthnumber),1)+relativedelta(days=-1)
getlastdateofpreviousmonth = str(getlastdateofpreviousmonth)[-5:]
getlastdateofpreviousmonth = getlastdateofpreviousmonth[:2]

#get monthdateyear
CM_monthdateyear = str(monthnumber) + str(lastday) + str(CM_year)
PM_monthdateyear = str(getlastdateofpreviousmonth) + str(getlastdate_previousmonth) + str(PM_year)

# COMMAND ----------

print(prm_file_path)

# COMMAND ----------

# DBTITLE 1,AlmacenesLa14Sa_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

Alma_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_AlmacenesLa14Sa_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

Alma_CM_excel = Alma_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

Alma_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

Alma_CM_Data = spark.createDataFrame(Alma_CM_excel,schema=Alma_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_AlmacenesLa14Sa_' + CM_monthdateyear + '.csv'

Alma_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,AlmacenesLa14Sa_PM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

Alma_PM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path_previous + prm_AlmacenesLa14Sa_PM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

Alma_PM_excel = Alma_PM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

Alma_PM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

Alma_PM_Data = spark.createDataFrame(Alma_PM_excel,schema=Alma_PM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path_previous
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_AlmacenesLa14Sa_' + PM_monthdateyear + '.csv'

Alma_PM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,CentralRegion_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

CentralRegion_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_CentralRegion_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

CentralRegion_CM_excel = CentralRegion_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

CentralRegion_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

CentralRegion_CM_Data = spark.createDataFrame(CentralRegion_CM_excel,schema=CentralRegion_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_CentralRegion_' + CM_monthdateyear + '.csv'

CentralRegion_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,CentralRegion_PM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

CentralRegion_PM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path_previous + prm_CentralRegion_PM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

#CentralRegion_PM_excel=CentralRegion_PM_excel[CentralRegion_PM_excel.columns[0:7]] 

CentralRegion_PM_excel = CentralRegion_PM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

CentralRegion_PM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

CentralRegion_PM_Data = spark.createDataFrame(CentralRegion_PM_excel,schema=CentralRegion_PM_Schema)

CentralRegion_PM_Data = CentralRegion_PM_Data.drop_duplicates()

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path_previous
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_CentralRegion_' + PM_monthdateyear + '.csv'

CentralRegion_PM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,CentralRetailRegion_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

CentralRetail_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_CentralRetailRegion_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

CentralRetail_CM_excel = CentralRetail_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

CentralRetail_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

CentralRetail_CM_Data = spark.createDataFrame(CentralRetail_CM_excel,schema=CentralRetail_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_CentralRetailRegion_' + CM_monthdateyear + '.csv'

CentralRetail_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,CruzVerde_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

CruzVerde_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_CruzVerde_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

CruzVerde_CM_excel = CruzVerde_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

CruzVerde_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

CruzVerde_CM_Data = spark.createDataFrame(CruzVerde_CM_excel,schema=CruzVerde_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_CruzVerde_' + CM_monthdateyear + '.csv'

CruzVerde_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,CruzVerde_PM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

CruzVerde_PM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path_previous + prm_CruzVerde_PM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

CruzVerde_PM_excel = CruzVerde_PM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

CruzVerde_PM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

CruzVerde_PM_Data = spark.createDataFrame(CruzVerde_PM_excel,schema=CruzVerde_PM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path_previous
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_CruzVerde_Consolidated_' + PM_monthdateyear + '.csv'

CruzVerde_PM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,ExitoAndCencosud_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

EC_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_ExitoAndCencosud_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

EC_CM_excel = EC_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

EC_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

EC_CM_Data = spark.createDataFrame(EC_CM_excel,schema=EC_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_ExitoAndCencosud_' + CM_monthdateyear + '.csv'

EC_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Locatel_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

Loc_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_Locatel_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

Loc_CM_excel = Loc_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

Loc_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

Loc_CM_Data = spark.createDataFrame(Loc_CM_excel,schema=Loc_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_Locatel_' + CM_monthdateyear + '.csv'

Loc_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,ModernRegion_PM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

ModernRegion_PM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path_previous + prm_ModernRegion_PM + '',sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

ModernRegion_PM_excel = ModernRegion_PM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

ModernRegion_PM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

ModernRegion_PM_Data = spark.createDataFrame(ModernRegion_PM_excel,schema=ModernRegion_PM_Schema)

ModernRegion_PM_Data = ModernRegion_PM_Data.drop_duplicates()

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path_previous
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_ModernRegion_' + PM_monthdateyear + '.csv'

ModernRegion_PM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)


# COMMAND ----------

# DBTITLE 1,NorthRegion_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

NorthRegion_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_NorthRegion_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

NorthRegion_CM_excel = NorthRegion_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

NorthRegion_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

NorthRegion_CM_Data = spark.createDataFrame(NorthRegion_CM_excel,schema=NorthRegion_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_NorthRegion_' + CM_monthdateyear + '.csv'

NorthRegion_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,NorthRegion_PM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

NorthRegion_PM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path_previous + prm_NorthRegion_PM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

NorthRegion_PM_excel = NorthRegion_PM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

NorthRegion_PM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

NorthRegion_PM_Data = spark.createDataFrame(NorthRegion_PM_excel,schema=NorthRegion_PM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path_previous
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_NorthRegion_' + PM_monthdateyear + '.csv'

NorthRegion_PM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Pacifico_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

Pac_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_Pacifico_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

Pac_CM_excel = Pac_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

Pac_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

Pac_CM_Data = spark.createDataFrame(Pac_CM_excel,schema=Pac_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_RPacifico_' + CM_monthdateyear + '.csv'

Pac_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true", encoding='UTF-8')
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,WestRegion_PM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

WestRegion_PM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path_previous + prm_WestRegion_PM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

WestRegion_PM_excel = WestRegion_PM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

WestRegion_PM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

WestRegion_PM_Data = spark.createDataFrame(WestRegion_PM_excel,schema=WestRegion_PM_Schema)

#WestRegion_PM_Data=WestRegion_PM_Data.drop_duplicates()

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path_previous
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_WestRegion_' + PM_monthdateyear + '.csv'

WestRegion_PM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Copservir_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

Copservir_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_Copservir_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

Copservir_CM_excel = Copservir_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

Copservir_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

Copservir_CM_Data = spark.createDataFrame(Copservir_CM_excel,schema=Copservir_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_Copservir_' + CM_monthdateyear + '.csv'

Copservir_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Olimpica_CM
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

Olimpica_CM_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Inventory/' + prm_file_path + prm_Olimpica_CM,sheetname = 'BD',index_col=None, keep_default_na=False, na_values=['_'])

Olimpica_CM_excel = Olimpica_CM_excel[['REPRESENTANTE','CLIENTES','COD','EAN','MARCA','DESCRIPCION','inv','Rota']]

Olimpica_CM_Schema = StructType([StructField("REPRESENTANTE", StringType(), True),StructField("CLIENTES", StringType(), True),StructField("COD", StringType(), True),StructField("EAN", StringType(), True),StructField("MARCA", StringType(), True),StructField("DESCRIPCION", StringType(),True), StructField("inv", StringType(),True),StructField("Rota", StringType(),True)])

Olimpica_CM_Data = spark.createDataFrame(Olimpica_CM_excel,schema=Olimpica_CM_Schema)

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Inventory/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_Inventory_Template_Olimpica_' + CM_monthdateyear + '.csv'

Olimpica_CM_Data.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Sellin
import pandas as pd
from pyspark.sql.types import *

Sellin_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Sellin/' + prm_file_path + prm_sellin,sheetname = 'Sheet1',index_col=None, keep_default_na=False, na_values=['_'])

Sellin_Schema = StructType([StructField("SHIP TO", StringType(), True),StructField("COD HIJO", StringType(), True),StructField("SOLD TO", StringType(), True),StructField("CLIENTE", StringType(), True),StructField("CANAL", StringType(), True),StructField("SUBCANAL", StringType(), True),StructField("REGION", StringType(), True),StructField("COD ZONA", StringType(), True),StructField("ZONA", StringType(), True),StructField("CIUDAD", StringType(), True),StructField("GMM OFERTA", StringType(), True),StructField("SKU", StringType(), True),StructField("MARCA", StringType(), True),StructField("SUBMARCA", StringType(), True),StructField("TIPO DE MARCA", StringType(), True),StructField("TIPO DE PRODUCTO", StringType(), True),StructField("ACTIVO/INACTIVO", StringType(), True),StructField("Venta (Val)", StringType(), True),StructField("Venta (Und)", StringType(), True)])

RawSellin = spark.createDataFrame(Sellin_excel,schema=Sellin_Schema) 

save_location= '/mnt/Americas/Staging/LATAM/NorthernCluster/Colombia/Transaction/Sellin/' + prm_file_path
csv_location = save_location+'temp'
file_location = save_location+'Colombia_SellIn_' +CM_monthdateyear +'.csv'

RawSellin.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True) 

# COMMAND ----------

# DBTITLE 1,Product Factor
import pandas as pd
from pyspark.sql.types import *

PF_excel = pd.read_excel(io = '/dbfs/mnt/Americas/RAW/LATAM/Northern Cluster/Colombia/Transaction/Product Factor/' + prm_productfactor,sheetname = 'Factor',index_col=None, keep_default_na=False, na_values=['_'])

PF_Schema = StructType([StructField("GMN Offer", StringType(), True),StructField("Offer", StringType(), True),StructField("Factor", StringType(), True),StructField("GMN Regular", StringType(), True),StructField("GMN", StringType(), True)])

RawDataPF = spark.createDataFrame(PF_excel,schema=PF_Schema) 

save_location= '/mnt/Americas/Staging/LATAM/Northern Cluster/Colombia/Transaction/Product Factor/'
csv_location = save_location+'temp'
file_location = save_location+'ProductFactor.csv'

RawDataPF.repartition(1).write.csv(path=csv_location, mode="overwrite", header="true")
file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True) 
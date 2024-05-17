from pyspark.sql.types import *
from pyspark.sql.functions import col, year, month, dayofmonth, current_date, quarter

# data can be downloaded from here: 
#
# https://azuresynapsestorage.blob.core.windows.net/sampledata/WideWorldImportersDW/csv/full/fact_sale_1y_full/ 
#
# it can't be read into spark directly by https. It shall be accessed via abffs from your own storage account or from local storage"

file_location = "abfss://<<your container>>@<<your storage>>.dfs.core.windows.net/<<path to csv files>>"

fact_sale_1y_full_schema = StructType([
    StructField('SaleKey', LongType(), True), 
    StructField('CityKey', IntegerType(), True), 
    StructField('CustomerKey', IntegerType(), True), 
    StructField('BillToCustomerKey', IntegerType(), True), 
    StructField('StockItemKey', IntegerType(), True), 
    StructField('InvoiceDateKey', TimestampType(), True), 
    StructField('DeliveryDateKey', TimestampType(), True), 
    StructField('SalespersonKey', IntegerType(), True), 
    StructField('WWIInvoiceID', IntegerType(), True), 
    StructField('Description', StringType(), True), 
    StructField('Package', StringType(), True), 
    StructField('Quantity', IntegerType(), True), 
    StructField('UnitPrice', DecimalType(18,2), True), 
    StructField('TaxRate', DecimalType(18,3), True), 
    StructField('TotalExcludingTax', DecimalType(29,2), True), 
    StructField('TaxAmount', DecimalType(38,6), True), 
    StructField('Profit', DecimalType(18,2), True), 
    StructField('TotalIncludingTax', DecimalType(38,6), True), 
    StructField('TotalDryItems', IntegerType(), True), 
    StructField('TotalChillerItems', IntegerType(), True), 
    StructField('LineageKey', IntegerType(), True),
    StructField('Year', IntegerType(), True),
    StructField('Quarter', IntegerType(), True),
    StructField('Month', IntegerType(), True)])

df = spark.read.format("csv").schema(fact_sale_1y_full_schema).option("header","true").load(file_location)
df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))

df.repartition(2).write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("abfss://<<your container>>@<<your storage>>.dfs.core.windows.net/<<path to delta table>>")

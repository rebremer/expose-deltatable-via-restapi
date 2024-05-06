## This code can be run on Spark to convert a CSV file to a Delta table

file_location = "https://azuresynapsestorage.blob.core.windows.net/sampledata/WideWorldImportersDW/csv/full/WideWorldImportersDW/csv/full/fact_sale_1y_full/"

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
df = df.withColumn('Year', year(current_date()))
df = df.withColumn('Month', month(current_date()))
df = df.withColumn('Day', dayofmonth(current_date()))
df.repartition(2).write.mode("overwrite").format("parquet").partitionBy("Year","Month","Day").save("abfss://<<your container>>@<<your account>>.dfs.core.windows.net/<<your location>>")
USE [Blog-Serverless-API];

CREATE EXTERNAL FILE FORMAT WWIDeltaTableFormat WITH(FORMAT_TYPE = DELTA);

DROP  EXTERNAL DATA SOURCE [taxidata_testextsynapistor_dfs_core_windows_net]


IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'fabricdemo_extsynapistor_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [taxidata_testextsynapistor_dfs_core_windows_net] 
	WITH (
		LOCATION   = 'https://<<account_name>>.dfs.core.windows.net/<<container>>',
	)
DROP EXTERNAL TABLE dbo.silver_fact_sale

CREATE EXTERNAL TABLE silver_fact_sale (
SaleKey           INT,
CityKey           INT,
CustomerKey       INT,
BillToCustomerKey INT,
StockItemKey      INT,
InvoiceDateKey    datetime2(7),
DeliveryDateKey   datetime2(7),
SalespersonKey    INT,
WWIInvoiceID      INT,
Description       VARCHAR(MAX),
Package           VARCHAR(100),
Quantity          smallint,
UnitPrice         DECIMAL(38,6),
TaxRate           DECIMAL(38,6),
TotalExcludingTax DECIMAL(38,6),
TaxAmount         DECIMAL(38,6),
Profit            DECIMAL(38,6),
TotalIncludingTax DECIMAL(38,6),
TotalDryItems     INT,
TotalChillerItems INT,
LineageKey        INT,
Quarter           INT
)
WITH (
        LOCATION = '/silver/silver_fact_sale',
        FILE_FORMAT = WWIDeltaTableFormat,
        DATA_SOURCE = fabricdemo_extsynapistor_dfs_core_windows_net
);
GO

CREATE USER [your_spn_name] FROM EXTERNAL PROVIDER;
ALTER ROLE [db_datareader] ADD MEMBER [your_spn_name];

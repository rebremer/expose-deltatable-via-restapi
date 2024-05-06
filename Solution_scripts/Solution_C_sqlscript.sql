/****** Object:  Table [dbo].[silver_fact_sale]    Script Date: 5/6/2024 10:00:45 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[silver_fact_sale](
	[SaleKey] [int] NULL,
	[CityKey] [int] NULL,
	[CustomerKey] [int] NULL,
	[BillToCustomerKey] [int] NULL,
	[StockItemKey] [int] NULL,
	[InvoiceDateKey] [datetime2](7) NULL,
	[DeliveryDateKey] [datetime2](7) NULL,
	[SalespersonKey] [int] NULL,
	[WWIInvoiceID] [int] NULL,
	[Description] [varchar](max) NULL,
	[Package] [varchar](100) NULL,
	[Quantity] [smallint] NULL,
	[UnitPrice] [decimal](38, 6) NULL,
	[TaxRate] [decimal](38, 6) NULL,
	[TotalExcludingTax] [decimal](38, 6) NULL,
	[TaxAmount] [decimal](38, 6) NULL,
	[Profit] [decimal](38, 6) NULL,
	[TotalIncludingTax] [decimal](38, 6) NULL,
	[TotalDryItems] [int] NULL,
	[TotalChillerItems] [int] NULL,
	[LineageKey] [int] NULL,
	[Quarter] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX ix_DesTest ON [dbo].[silver_fact_sale] (CityKey, TaxAmount, DeliveryDateKey DESC)

CREATE USER [your_spn_name] FROM EXTERNAL PROVIDER;
ALTER ROLE [db_datareader] ADD MEMBER [your_spn_name];
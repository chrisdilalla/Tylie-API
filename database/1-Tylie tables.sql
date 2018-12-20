USE [MAS500_Test]
GO

/****** Object:  Table [dbo].[StgSOLine_Tylie]    Script Date: 12/18/2018 5:18:57 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[StgSOLine_Tylie](
	[RowKey] [int] IDENTITY(1,1) NOT NULL,
	[AcctRefCode] [varchar](20) NULL,
	[Action] [smallint] NULL,
	[AmtInvcd] [decimal](15, 3) NULL,
	[CloseDate] [datetime] NULL,
	[CmntOnly] [varchar](3) NULL,
	[CommClassID] [varchar](15) NULL,
	[CommPlanID] [varchar](15) NULL,
	[DeliveryMeth] [varchar](25) NULL,
	[Description] [varchar](40) NULL,
	[ExtAmt] [decimal](15, 3) NULL,
	[ExtCmnt] [varchar](255) NULL,
	[FOBID] [varchar](15) NULL,
	[FreightAmt] [decimal](15, 3) NULL,
	[GLAcctNo] [varchar](100) NULL,
	[Hold] [varchar](3) NULL,
	[HoldReason] [varchar](20) NULL,
	[ItemAliasID] [varchar](30) NULL,
	[ItemID] [varchar](30) NULL,
	[KitComponent] [varchar](3) NULL,
	[MAS90LineIndex] [varchar](5) NULL,
	[OrigOrdered] [decimal](16, 8) NULL,
	[OrigPromiseDate] [datetime] NULL,
	[PONumber] [varchar](13) NULL,
	[PromiseDate] [datetime] NULL,
	[QtyInvcd] [decimal](16, 8) NULL,
	[QtyOnBO] [decimal](16, 8) NULL,
	[QtyOrd] [decimal](16, 8) NULL,
	[QtyRtrnCredit] [decimal](16, 8) NULL,
	[QtyRtrnReplacement] [decimal](16, 8) NULL,
	[QtyShip] [decimal](16, 8) NULL,
	[ReqCert] [varchar](3) NULL,
	[RequestDate] [datetime] NULL,
	[ShipDate] [datetime] NULL,
	[ShipMethID] [varchar](15) NULL,
	[ShipPriority] [smallint] NULL,
	[ShipToAddrLine1] [varchar](40) NULL,
	[ShipToAddrLine2] [varchar](40) NULL,
	[ShipToAddrLine3] [varchar](40) NULL,
	[ShipToAddrLine4] [varchar](40) NULL,
	[ShipToAddrLine5] [varchar](40) NULL,
	[ShipToAddrName] [varchar](40) NULL,
	[ShipToCity] [varchar](20) NULL,
	[ShipToCountryID] [varchar](3) NULL,
	[ShipToPostalCode] [varchar](10) NULL,
	[ShipToStateID] [varchar](3) NULL,
	[SOLineNo] [int] NULL,
	[Status] [varchar](25) NULL,
	[STaxClassID] [varchar](15) NULL,
	[TradeDiscAmt] [decimal](15, 3) NULL,
	[TradeDiscPct] [decimal](5, 2) NULL,
	[TranNo] [varchar](10) NULL,
	[UnitMeasID] [varchar](6) NULL,
	[UnitPrice] [decimal](15, 5) NULL,
	[UserFld1] [varchar](15) NULL,
	[UserFld2] [varchar](15) NULL,
	[VendorID] [varchar](12) NULL,
	[WarehouseID] [varchar](6) NULL,
	[WillCall] [varchar](3) NULL,
	[ProcessStatus] [smallint] NOT NULL,
	[SessionKey] [int] NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[RowKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[stgCustomer_Tylie]    Script Date: 12/18/2018 5:18:57 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[stgCustomer_Tylie](
	[Rowkey] [int] IDENTITY(1,1) NOT NULL,
	[CompanyID] [varchar](3) NULL,
	[Key] [int] NULL,
	[CustID] [varchar](50) NULL,
	[CustClassID] [varchar](50) NULL,
	[CustClassName] [varchar](50) NULL,
	[AddrLine1] [varchar](50) NULL,
	[AddrLine2] [varchar](50) NULL,
	[City] [varchar](50) NULL,
	[StateID] [varchar](50) NULL,
	[CountryID] [varchar](50) NULL,
	[PostalCode] [varchar](50) NULL,
	[ContactName] [varchar](50) NULL,
	[ContactTitle] [varchar](50) NULL,
	[ContactFax] [varchar](50) NULL,
	[ContactPhone] [varchar](50) NULL,
	[ContactEmail] [varchar](256) NULL,
	[PrintAck] [varchar](50) NULL,
	[RequireAck] [varchar](50) NULL,
	[Status] [varchar](50) NULL,
	[BrandKey] [int] NULL,
	[BrandID] [varchar](50) NULL,
	[Brand] [varchar](50) NULL,
	[BrandStatus] [varchar](50) NULL,
	[ImportStatus] [int] NULL
) ON [PRIMARY]
GO



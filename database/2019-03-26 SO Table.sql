USE [MAS500_Test]
GO

/****** Object:  Table [dbo].[StgSOLine_Tylie]    Script Date: 3/26/2019 3:29:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[StgSOLine_Tylie](
	[RowKey] [int] IDENTITY(1,1) NOT NULL,
	[CustID] [varchar](50) NULL,
	[ShiptoCustID] [varchar](50) NULL,
	[Trandate] [datetime] NULL,
	[URL] [varchar](4000) NULL,
	[CustPONo] [varchar](15) NULL,
	[CustJobNo] [varchar](50) NULL,
	[PlatformID] [varchar](50) NULL,
	[ItemID] [varchar](50) NULL,
	[QtyOrd] [decimal](15, 3) NULL,
	[WorkOrderNo] [varchar](50) NULL,
	[Spots] [decimal](15, 3) NULL,
	[Comment] [varchar](255) NULL,
	[Status] [int] NULL,
	[Discount] [decimal](15, 3) NULL,
	[Length] [int] NULL,
	[AddlOrDiscount] [decimal](18, 0) NULL,
	[ProBono] [int] NULL,
	[CostCenter] [varchar](100) NULL,
	[AdditionalInfo] [varchar](max) NULL,
	[Destination] [int] NULL,
 CONSTRAINT [PK__StgSOLin__5008C760EB158B5C] PRIMARY KEY CLUSTERED 
(
	[RowKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO



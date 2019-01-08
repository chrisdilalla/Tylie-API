 /****** Object:  Table [dbo].[StgPOLine_Tylie]    Script Date: 1/5/2019 5:37:22 AM ******/

SET ANSI_NULLS ON

GO

 

SET QUOTED_IDENTIFIER ON
GO

 

CREATE TABLE [dbo].[StgPOLine_Tylie](
                [Rowkey] [int] IDENTITY(1,1) NOT NULL,
                [VendID] [varchar](50) NULL,
                [VendPO] [varchar](50) NULL,
                [TranDate] [datetime] NULL,
                [ItemID] [varchar](50) NULL,
                [Description] [varchar](50) NULL,
                [QtyOrd] [decimal](15, 3) NULL,
                [Comment] [varchar](255) NULL,
                [Workorderno] [varchar](50) NULL,
                [Status] [int] NULL
) ON [PRIMARY]

GO


/****** Object:  Table [dbo].[StgSOLine_Tylie]    Script Date: 1/5/2019 5:37:22 AM ******/

SET ANSI_NULLS ON
GO
 

SET QUOTED_IDENTIFIER ON
GO


drop table StgSOLine_Tylie
go

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
CONSTRAINT [PK__StgSOLin__5008C760EB158B5C] PRIMARY KEY CLUSTERED
(
                [RowKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]

GO
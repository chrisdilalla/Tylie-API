USE [MAS500_Test]
GO

/****** Object:  StoredProcedure [dbo].[spSOapiSalesOrdIns_Tylie]    Script Date: 04.01.19 14:12:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




ALTER procedure [dbo].[spSOapiSalesOrdIns_Tylie]
	(	@_oRetVal			INT		OUTPUT
	)
AS

---------------------------------------------------------------------------------
-- NAME: 		spSOapiSalesOrdIns
--
-- DESCRIPTION:	Migrates Sales Order from staging or temporary tables
--		to the real tables via custom SQL statements contained within this
--		stored procedure.
--
-- MIGRATION SP INTERFACE PARAMETERS:
--	@oContinue		Supported.  0=All rows processed   1=Unprocessed rows still exist.
--	@iCancel		Supported.  0=Normal   1=Cancel any existing processing
--	@iSessionKey 		Supported.  Unique ID for the set of records to process.
--	@iCompanyID		Supported.	Enterprise Company.
--	@iRptOption		Partially Supported.  0=All  1=None  2=Good  3=Bad
--	@iPrintWarnings		Not applicable to this SP returns no warnings.
--	@UseStageTable 		If 1, then data will be obtained from the staging tables, otherwise
--					it is expected that temporary tables exist and have the data in them.
--	@oRecsProcessed		Supported.  Input/Output.  Will be set to incoming value plus
--					the total number of rows processed.
--	@oFailedRecs		Supported.  Input/Output.  The number of records failed.  The value is
--					additive, as is @oRecsProcessed and @oTotalRecs - we add to it each time.
--	@oTotalRecs		Supported.  Input/Output.  Set to total number of rows to be
--					processed, but only when the value is NULL or zero.  This allows
--					this value to remain constant over any number of SP runs.
--	@_oRetVal		Supported.  See declared values below.
--
--	ASSUMPTIONS:	
-- 	If the caller wants to use staging table data, then if the temporary tables already exist
--	they will be truncated.  If the temporary tables don't exist they will be created.
--  	Data from the staging tables will then be placed into the temporary tables.  If the caller
--	doesn't use staging tables, then it is assumed that they have created them and that the
--  	temporary tables contain the desired data.
--	
--	AUTHOR: Best Enterprise Suite Data Migration Team
---------------------------------------------------------------------------------
Declare @oContinue			SMALLINT	,		
	@iCancel			SMALLINT,	
	@iSessionKey		INT=0,			
	@iCompanyID 		VARCHAR(3),
	@iRptOption			INT = 1,				-- Default = Print none.
	@iPrintWarnings		SMALLINT = 1,				-- Default = Print warnings
	@UseStageTable		SMALLINT = 1,				-- Default = Use staging tables.
	@oRecsProcessed		INT	,		
	@oFailedRecs		INT	,
	@oTotalRecs			INT		

BEGIN /* MAIN CODE BLOCK */

SET NOCOUNT ON

	-------------
	-- DECLARATIONS OF VARIABLES AND CONSTANTS
	-------------
	------------------- GENERAL MIGRATION CONSTANTS --------------------------------
	-- Return Constants
	DECLARE @RET_UNKNOWN_ERR	SMALLINT,		@RET_SUCCESS		SMALLINT,
			@RET_FAILURE		SMALLINT,		@RET_PARM_ERR		SMALLINT	
	SELECT	@RET_UNKNOWN_ERR	= 0,			@RET_SUCCESS		= 1,
			@RET_FAILURE		= 2,			@RET_PARM_ERR		= 3

	-- ProcessingStatus constants
	DECLARE	@NOT_PROCESSED		SMALLINT,		@GENRL_PROC_ERR		SMALLINT,
			@PROCESSED			SMALLINT,		@VALIDATION_FAILED	SMALLINT
	SELECT	@NOT_PROCESSED		= 0,			@GENRL_PROC_ERR		= 2,
			@PROCESSED			= 1,			@VALIDATION_FAILED	= 0	

	-- Migration Status constants
	DECLARE @MIG_STAT_SUCCESSFUL SMALLINT,		@MIG_STAT_FAILURE	SMALLINT,
			@MIG_STAT_WARNING	SMALLINT,		@MIG_STAT_INFO		SMALLINT
	SELECT	@MIG_STAT_INFO		= 0,			@MIG_STAT_SUCCESSFUL	= 3,
			@MIG_STAT_FAILURE	= 1,			@MIG_STAT_WARNING	= 2

	-- Reporting Constants
	DECLARE	@RPT_PRINT_ALL		SMALLINT,		@RPT_PRINT_NONE		SMALLINT,
			@RPT_PRINT_SUCCESSFUL	SMALLINT,	@RPT_PRINT_UNSUCCESSFUL	SMALLINT
	SELECT	@RPT_PRINT_ALL		= 0,			@RPT_PRINT_NONE		= 1,
			@RPT_PRINT_SUCCESSFUL	= 2,		@RPT_PRINT_UNSUCCESSFUL	= 3

	-- Behavior constants: Invalid GL Account, DuplicationAction, invalid reference Parameter options
	DECLARE @DO_NOT_MIGRATE		SMALLINT,		@GL_USE_SUSPENSE_ACCT	SMALLINT,
			@REPLACE_DUPLICATE	SMALLINT,		@INVALIDREF_USE_BLANK	SMALLINT
	SELECT	@DO_NOT_MIGRATE		= 0,			@GL_USE_SUSPENSE_ACCT	= 1,
			@REPLACE_DUPLICATE	= 1,			@INVALIDREF_USE_BLANK	= 1

	-- Migration Constants that are based on the migration entity
	-- @MAX_ROWS_PER_RUN is the threshhold to trigger @oContinue to be set to true.
	-- It allows the caller to continuously call this SP so that a status can be reported
	-- back to the user.  The lower the value, the more frequently the user can recieve progress feedback.
	DECLARE	@CREATE_TYPE_MIGRATE	SMALLINT,	@MAX_ROWS_PER_RUN	INTEGER
			,@MIGRATE_SECS		SMALLINT
	SELECT	@CREATE_TYPE_MIGRATE	= 7,		@MAX_ROWS_PER_RUN	= 1
			,@MIGRATE_SECS		= 2

	--Cursor constants
	DECLARE	@FETCH_SUCCESSFUL	INTEGER,		@FETCH_PAST_LAST_ROW	INTEGER,		
			@FETCH_ROW_MISSING	INTEGER
	SELECT	@FETCH_SUCCESSFUL	= 0,			@FETCH_PAST_LAST_ROW	= -1,
			@FETCH_ROW_MISSING	= -2

	--Temp Table Creation Constants
	DECLARE @CREATED_OUTSIDE_THIS_SP	INTEGER,	@CREATED_INSIDE_THIS_SP		INTEGER

	SELECT	@CREATED_OUTSIDE_THIS_SP 	= 1,		@CREATED_INSIDE_THIS_SP 	= 2

	--Cursor Status Constants
	DECLARE @STATUS_CURSOR_OPEN_WITH_DATA	INTEGER,	@STATUS_CURSOR_OPEN_BUT_EMPTY	INTEGER,
			@STATUS_CURSOR_CLOSED		INTEGER,		@STATUS_CURSOR_DOES_NOT_EXIST	INTEGER

	SELECT	@STATUS_CURSOR_OPEN_WITH_DATA	= 1,		@STATUS_CURSOR_OPEN_BUT_EMPTY	= 0,
			@STATUS_CURSOR_CLOSED 		= -1,			@STATUS_CURSOR_DOES_NOT_EXIST 	= -3

	-- Report Constants
	DECLARE	@RPT_STRINGNO_UNSUCCESSFUL	INTEGER, 		@RPT_STRINGNO_CANCELLED		INTEGER,
			@RPT_STRINGNO_DUPLICATE		INTEGER,		@RPT_STRINGNO_INVALID_COMPANY	INTEGER,
			@RPT_STRINGNO_UPDATE_FAILED    	INTEGER, 	@RPT_STRINGNO_DELETE_FAILED    	INTEGER,
			@RPT_STRINGNO_CHILD_FAILURE INTEGER,		@RPT_STRINGNO_HDR_REC_MISSING 	INT 		
	SELECT	@RPT_STRINGNO_UNSUCCESSFUL	= 100447,		@RPT_STRINGNO_CANCELLED		= 100448,
			@RPT_STRINGNO_DUPLICATE		= 110067,		@RPT_STRINGNO_INVALID_COMPANY	= 160228,
			@RPT_STRINGNO_UPDATE_FAILED    	= 430127,	@RPT_STRINGNO_DELETE_FAILED    	= 430128,
			@RPT_STRINGNO_CHILD_FAILURE = 430046,		@RPT_STRINGNO_HDR_REC_MISSING = 211078

	-- Variables
	DECLARE	@SourceRowCount			INT,		@RowsProcThisCall	INT,			
			@UserID				VARCHAR(30),	@Message		VARCHAR(255),	
			@LanguageID			INT,			@Duplicate		SMALLINT,		
			@Status				SMALLINT,		@StringNo		INT,
			@EntryNo			INT,			@StartDate		DATETIME,
			@RetVal				INT,			@ColumnID		VARCHAR(35),
			@ColumnValue			VARCHAR(100),	@TempTableCreation	SMALLINT,
			@lSpid				INT,			@ErrorCount		INT,
			@UpdRecCount		INT

	DECLARE @LIST_DBVALUE			INT,		@LIST_LOCALTEXT	INT
	SELECT	@LIST_DBVALUE			= 0,		@LIST_LOCALTEXT	= 1

	--spDMListValues constants/variables
	DECLARE @RET_DBVALUES			INT,		@RET_LOCALTEXT	INT
	SELECT	@RET_DBVALUES			= 0,		@RET_LOCALTEXT	= 1
	DECLARE @List				VARCHAR(255)

	-- Other Constants
	DECLARE @ItemType_AssemblyKit 	SMALLINT,	@ItemType_BTOKit	SMALLINT
	SELECT	@ItemType_AssemblyKit = 	8,		@ItemType_BTOKit	= 7

	-- Record Action Constants
	DECLARE @REC_INSERT				SMALLINT,	@REC_UPDATE		SMALLINT,
			@REC_DELETE             SMALLINT,	@REC_AUTO           	SMALLINT,
			@ACTION_COL_USED	  	SMALLINT
	
	SELECT  @REC_INSERT 			= 1,		@REC_UPDATE 		= 2,
			@REC_DELETE 			= 3,		@REC_AUTO 			= 0,
			@ACTION_COL_USED		= 1	

	-- Sales Order Status Constans
	DECLARE @SOStatus_NOTACT 		VARCHAR(1),	@SOStatus_OPEN 		VARCHAR(1),
			@SOStatus_INACTIVE 		VARCHAR(1),	@SOStatus_CANCELED	VARCHAR(1),
			@SOStatus_CLOSED 		VARCHAR(1),	@SOStatus_INCOMPLETE 	VARCHAR(1),
			@SOStatus_PENDINGAPP 	VARCHAR(1),	@SOInsertion_OPEN	INT,
			@SOInsertion_CLOSED		INT,		@Str_OPEN			VARCHAR(4),
			@Str_CLOSED				VARCHAR(6)
	
	SELECT 	@SOStatus_NOTACT 		= '0',		@SOStatus_OPEN 			= '1',
			@SOStatus_INACTIVE 		= '2',		@SOStatus_CANCELED		= '3',
			@SOStatus_CLOSED 		= '4',		@SOStatus_INCOMPLETE	= '5',
			@SOStatus_PENDINGAPP 	= '6',		@SOInsertion_OPEN		= 2100600,
			@SOInsertion_CLOSED		= 2400350,	@Str_OPEN				= 'Open',
			@Str_CLOSED				= 'Closed'

	-- Default output parameter values
	SELECT 	@_oRetVal = @RET_UNKNOWN_ERR,
			@oRecsProcessed = COALESCE(@oRecsProcessed,0),
			@oTotalRecs = COALESCE(@oTotalRecs,0),	
			@oFailedRecs = COALESCE(@oFailedRecs,0),
			@oContinue = 0
	
	-- Get the migration user
	EXEC spDMMigrUserIDGet @iSessionKey, @UseStageTable, @UserID OUTPUT

	-- Get LanguageID for migration user (use english if not found)
	SELECT 	@LanguageID =
		COALESCE((SELECT LanguageID FROM tsmUser WITH (NOLOCK) WHERE UserID = @UserID),1033)

	---------------------------------------------------------------------------------
	-- -----------	Entity Specific Constants and Variables		
	--
	-- Be sure to include declaration of any DEFAULT values here
	-- these will be used to COALESCE against incoming values that
	-- are assigned to target columns with default values defined.
	---------------------------------------------------------------------------------
	DECLARE
		@MODULE_NO_CI 			INT,			@CI_MODULE_DESC 		VARCHAR(80),
		@CI_MODULE_STRINGNO 		INT,	
		@MODULE_NO_AR 			INT,			@AR_MODULE_DESC			VARCHAR(80),
		@AR_MODULE_STRINGNO		INT,
		@MODULE_NO_SO			INT,			@SO_MODULE_DESC			VARCHAR(80),
		@SO_MODULE_STRINGNO		INT,			@TranTypeSO			INT,
		@RPT_STRINGNO_MODULE_INACTIVE 	INT,	@RPT_STRINGNO_SEEFORVALID 	INT,
		@RPT_STRINGNO_X_IS_INVALID 	INT,		@RPT_STRINGNO_Col_IS_NULL	INT,
		@RPT_STRINGNO_REF_BLANK 	INT,		@RPT_STRINGNO_ONLYPOSITVE 	INT,	
		@RPT_STRINGNO_INVALID_YESNO 	INT,	@RPT_STRINGNO_AS_LIST 		INT,
		@RPT_STRINGNO_VAL1_FOR_VAL2 	INT,	@RPT_STRINGNO_IGNORE_COMP_LINE	INT,
		@RPT_STRINGNO_SOChngOrdOnNoUpd  INT,	@RPT_STRINGNO_CannotUpdCloseOrd INT,
		@RPT_STRINGNO_CannotCancelwActivity INT,
		@CreateType_Migrate		SMALLINT,		@Migration			SMALLINT,
		@HomeCurrID			VARCHAR(3),			@AllocateMeth_AMT		SMALLINT,
		@lListValues			VARCHAR(256)	

	SELECT 	
		@MODULE_NO_CI = 2,						@CI_MODULE_STRINGNO = 50774,
		@MODULE_NO_AR = 5,						@AR_MODULE_STRINGNO = 50778,	
		@MODULE_NO_SO = 8,						@SO_MODULE_STRINGNO = 50073,	
		@RPT_STRINGNO_MODULE_INACTIVE = 100463,	@RPT_STRINGNO_Col_IS_NULL = 207,
		@RPT_STRINGNO_X_IS_INVALID = 151465,	@RPT_STRINGNO_REF_BLANK = 430008,	
		@RPT_STRINGNO_INVALID_YESNO = 430106,	@RPT_STRINGNO_AS_LIST = 100469,
		@RPT_STRINGNO_VAL1_FOR_VAL2 = 100488,	@RPT_STRINGNO_SEEFORVALID = 430103,
		@RPT_STRINGNO_ONLYPOSITVE = 240074,		@RPT_STRINGNO_SOChngOrdOnNoUpd = 250971,
		@RPT_STRINGNO_CannotUpdCloseOrd = 250970,
		@RPT_STRINGNO_CannotCancelwActivity = 250972,
		@CreateType_Migrate = 7,
		@TranTypeSO = 801,						@Migration = 1,
		@AllocateMeth_AMT = 1,					@RPT_STRINGNO_IGNORE_COMP_LINE = 430125

	-- TaskID
	DECLARE
	@TASKID_CUSTOMER INTEGER,		@TaskString VARCHAR(40)

	SELECT	
	@TASKID_CUSTOMER	= 83952612	
--
	--Variable for SO Header
	DECLARE	
		@AckDate datetime,				@AuthOvrdAmt decimal(15, 3),
		@AcctRefCode VARCHAR(20),										--Added to store input Account Reference code from stage table.
		@BillToCustID VARCHAR(15),		@BillToCustAddrKey int,
		@BillToAddrLine1 varchar (40),	@BillToAddrLine2 varchar (40),
		@BillToAddrLine3 varchar (40),	@BillToAddrLine4 varchar (40),
		@BillToAddrLine5 varchar (40),	@BillToAddrName varchar (40),
		@BillToCity VARCHAR(20),		@BillToCountryID VARCHAR(3),
		@BillToPostalCode VARCHAR(10),	@BillToStateID VARCHAR(3),
		@BillToCustAddrID VARCHAR(15),	@BillToCopyID VARCHAR(15),
		@CloseDate datetime,			@CurrID VARCHAR(3),
		@ContactName VARCHAR(40),			
		@CrHold VARCHAR(3),				@CustID VARCHAR(12),
		@CustKey int,					@CustClassID VARCHAR(15),
		@CustPONo VARCHAR(15),			@CurrExchRate float,	
		@ConfirmNo VARCHAR(10),			
		@DfltAcctRefCode VARCHAR(20),	@DfltCommPlanID VARCHAR(15),
		@DfltFOBID VARCHAR(15),			@DfltPromDate datetime,
		@DfltRequestDate datetime,		@DfltShipDate datetime,
		@DfltShipMethID VARCHAR(15),	@DfltShipPriority smallint,		
		@DfltWarehouseID VARCHAR(6),	@Expiration datetime,
		@FreightAmt decimal(15, 3),		@Hold VARCHAR(3),
		@HoldReason VARCHAR(20) ,		@OpenAmt decimal(15, 3),
		@PmtTermsID VARCHAR(15),		@RecurSOTranNo VARCHAR(10),
		@PrimarySperID VARCHAR(12),		@RequireSOAck VARCHAR(3),
		@SalesAmt decimal(15, 3),		@SalesAmtHC decimal(15, 3),
		@SalesSourceID VARCHAR(15),		@SOAckFormID VARCHAR(15),
		@SOStatus VARCHAR(12),			@lSOStatus smallint,
		@DfltDSTCopyKey int,			@DfltShipToAddrLine1 varchar (40),
		@DfltShipToAddrLine2 varchar (40),	@DfltShipToAddrLine3 varchar (40),
		@DfltShipToAddrLine4 varchar (40),	@DfltShipToAddrLine5 varchar (40),
		@DfltShipToAddrName varchar (40),	@DfltShipToCity VARCHAR(20),
		@DfltShipToCountryID VARCHAR(3),	@DfltShipToPostalCode VARCHAR(10),
		@DfltShipToStateID VARCHAR(3),	@DfltShipToCustAddrID varchar (40),
		@STaxCalc VARCHAR(3),			
		@TradeDiscAmt decimal(15, 3),	@STaxAmt decimal(15, 3),
		@TranAmtHC decimal(15, 3),		@TradeDiscPct decimal(5,4),
		@TranCmnt varchar (50) ,		@TranDate datetime,
		@TranNo VARCHAR(10) ,			@UserFld1 VARCHAR(15) ,
		@UserFld2 VARCHAR(15) ,			@UserFld3 VARCHAR(15) ,			
		@UserFld4 VARCHAR(15) ,			@TranAmt decimal(15,3),
		@CreateDate datetime,			@RowKey	int,
		@lColumnValue varchar(255),		@UniqueID VARCHAR(255),
		@DefaultIfNull smallint,		@SOKey int,	
		@TranID VARCHAR(13),			@lHold smallint,
		@DfltWhseKey int,				@lRequireSOAck smallint,
		@ProcessStatus	smallint,		@Action SMALLINT,	
		@OrgiSOStatus VARCHAR(12),		@Rec_Exist SMALLINT

	--Variable for SO Line
	DECLARE
		@LineRowKey int,				@LineCloseDate datetime ,
		@CmntOnly VARCHAR(3),			@CommClassID VARCHAR(15) ,
		@CommPlanID VARCHAR(15) ,		@CustomBTOKit VARCHAR(3),
		@Description varchar (40) ,		@ExtCmnt varchar (255) ,
		@ItemAliasID VARCHAR(30),		@ItemID VARCHAR(30) ,
		@ReqCert VARCHAR(3),			@STaxClassID VARCHAR(15) ,			
		@UnitMeasID VARCHAR(12) ,
		@UnitPrice decimal(15, 5),		@UnitPriceOvrd VARCHAR(3),
		@LineUserFld1 VARCHAR(15) ,		@LineUserFld2 VARCHAR(15) ,
		@AmtInvcd decimal(15, 3),		@ExtAmt decimal(15, 3),
		@LineFreightAmt decimal(15, 3),	@GLAcctNo VARCHAR(100) ,
		@OrigOrdered decimal(16, 8),	@OrigPromiseDate datetime ,
		@PromiseDate datetime ,			@QtyInvcd decimal(16, 8),
		@QtyOnBO decimal(16, 8),		@QtyOpenToShip decimal(16, 8),
		@QtyOrd decimal(16, 8),			@QtyRtrnCredit decimal(16, 8),
		@QtyRtrnReplacement decimal(16, 8),	@QtyShip decimal(16, 8),
		@RequestDate datetime ,			@ShipToAddrLine1 varchar (40)  ,
		@ShipToAddrLine2 varchar (40),	@ShipToAddrLine3 varchar (40)  ,	
		@ShipToAddrLine4 varchar (40) ,	@ShipToAddrLine5 varchar (40)  ,
		@ShipToAddrName varchar (40),	@ShipToCity VARCHAR(20)  ,
		@ShipToCountryID VARCHAR(3) ,	@ShipToPostalCode VARCHAR(10)  ,
		@ShipToStateID VARCHAR(3) ,		@ShipToCustAddrID varchar (40)  ,
		@ShipDate datetime ,			@ShipMethID VARCHAR(15) ,
		@ShipPriority smallint,			@LineStatus VARCHAR(15),
		@LineTradeDiscAmt decimal(15, 3),	@LineTradeDiscPct decimal(7, 4),
		@WarehouseID VARCHAR(12),		@SOLineNo integer,
		@SOLineKey int,					@SOLineDistKey int,	
		@OrigSOLineNo int,				@KitComponent VARCHAR(3),
		@lCmntOnly smallint,			@FOBID VARCHAR(15),
		@lInclOnPackList smallint,		@lInclOnPickList smallint,	
		@lReqCert smallint,				@lUnitPriceOvrd smallint,
		@lLineStatus smallint,			@CHILD_VALIDATION_FAILED smallint,
		@lEntityID VARCHAR(80),			@lCustomizedKit smallint,
		@KitItemKey int,				@KitQty decimal(16,8),
		@KitWhseKey int,				@lItemType int,
		@VendorID VARCHAR(15),			@DeliveryMeth VARCHAR(25),
		@lDeliveryMeth smallint,		@PONumber VARCHAR(10),
		@DetailAction SMALLINT,			@lActivity int,
		@CurrExchSchdID VARCHAR(15),	@DfltPurchVAddrID VARCHAR(15)

	--Closing Line Variables
	DECLARE
@lMinStatus         smallint

	DECLARE @QtyDecPlaces SMALLINT
	DECLARE @UnitCostDecPlaces SMALLINT
	DECLARE @UnitPriceDecPlaces SMALLINT
	DECLARE @DigitsAfterDecimal SMALLINT

	/*Entity specific constant for dealing with tsmMigrateStepParamValue*/
	-- tsmMigrateStepParam constants (The actual param numbers will differ for each routine)
	-- These are used to retrieve the parameters by a named constant (vs. ordinal ParamNo)
	DECLARE @INVALIDGL_PARM_TYPE	SMALLINT, 	@REF_PARM_TYPE 		SMALLINT,
			@DUPL_PARM_TYPE			SMALLINT,	@DFLT_PARM_TYPE		SMALLINT,
			@DFLT_CUST_PARM_TYPE	SMALLINT,	@DFLT_ITEM_PARM_TYPE	SMALLINT

	SELECT 	@INVALIDGL_PARM_TYPE 	= 1, 		@REF_PARM_TYPE 		= 2,
			@DUPL_PARM_TYPE 	= 3, 		@DFLT_PARM_TYPE 	= 4,
			@DFLT_CUST_PARM_TYPE = 5,		@DFLT_ITEM_PARM_TYPE = 6

	DECLARE @DO_NOT_LOG_ERRS 	SMALLINT, 	@RET_GLACCT_INVALID 	SMALLINT,
			@RET_SUSPENSE_ACCT 	SMALLINT

	SELECT 	@DO_NOT_LOG_ERRS 	= 0,		@RET_GLACCT_INVALID 	= 2,
			@RET_SUSPENSE_ACCT 	= 2

	-- Qty, Amt decimal digits validation constants
	DECLARE	@ColType_AmtNC		SMALLINT,	@ColType_AmtHC		SMALLINT,
			@ColType_PRICE		SMALLINT,	@ColType_COST		SMALLINT,
			@ColType_QTY		SMALLINT	
	
	SELECT	@ColType_AmtNC		= 1,		@ColType_AmtHC		= 2,
			@ColType_PRICE		= 3,		@ColType_COST		= 4,
			@ColType_QTY		= 5	

	/*SetupStepParam cache variables*/
	DECLARE @BlankInvalidReference SMALLINT,	-- used to cache value for Invalid Reference Flag.
	 		@InvalidGLUseSuspense SMALLINT,
	 		@IsGLAcctValid SMALLINT,
	 		@DfltSOAckFormID VARCHAR(15),
	 		@DfltSOAckFormKey_Parm_No SMALLINT,
			@UpdColList VARCHAR(4000),
			@UpdColList1 VARCHAR(4000),	
			@SQLStmt VARCHAR(5000),
			@TrackChngOrders SMALLINT,
			@EntityColName VARCHAR(255),
			@SetupStepKey INT,
			@Status_Str VARCHAR(6),			
			@UseCustomerDefaultIfBlank SMALLINT,
			@UseItemDefaultIfBlank SMALLINT

	SELECT  @DfltSOAckFormKey_Parm_No	= 3
	-- End Entity Specific Constants and Variables

	-----------------------------------------------------------------------------
	-- Get input parameters from tsmMigrateStepParamValue and tsmMigrateStepParam
	-- These will be unique to the specific migration entity
	-- --------------------------------------------------------------------------
	SELECT @BlankInvalidReference = CONVERT(SMALLINT, pv.ParamValue),
		   @SetupStepKey = sp.SetupStepKey
	FROM tsmMigrateStepParam sp  WITH (NOLOCK)
	INNER JOIN tsmMigrateStepParamValue pv ON pv.SetupStepKey = sp.SetupStepKey AND pv.ParamNo = sp.ParamNo
	WHERE pv.MigrateSessionKey = @iSessionKey AND sp.ParamType = @REF_PARM_TYPE

	SELECT @InvalidGLUseSuspense = COALESCE(CONVERT(SMALLINT, pv.ParamValue),0)
	FROM tsmMigrateStepParam sp  WITH (NOLOCK)
	LEFT JOIN tsmMigrateStepParamValue pv ON pv.SetupStepKey = sp.SetupStepKey AND pv.ParamNo = sp.ParamNo
	WHERE pv.MigrateSessionKey = @iSessionKey AND sp.ParamType = @INVALIDGL_PARM_TYPE

	SELECT @UseCustomerDefaultIfBlank = COALESCE(CONVERT(SMALLINT, pv.ParamValue),0)
	FROM tsmMigrateStepParam sp  WITH (NOLOCK)
	LEFT JOIN tsmMigrateStepParamValue pv ON pv.SetupStepKey = sp.SetupStepKey AND pv.ParamNo = sp.ParamNo
	WHERE pv.MigrateSessionKey = @iSessionKey AND sp.ParamType = @DFLT_CUST_PARM_TYPE

	SELECT @UseItemDefaultIfBlank = COALESCE(CONVERT(SMALLINT, pv.ParamValue),0)
	FROM tsmMigrateStepParam sp  WITH (NOLOCK)
	LEFT JOIN tsmMigrateStepParamValue pv ON pv.SetupStepKey = sp.SetupStepKey AND pv.ParamNo = sp.ParamNo
	WHERE pv.MigrateSessionKey = @iSessionKey AND sp.ParamType = @DFLT_ITEM_PARM_TYPE

	SELECT @DfltSOAckFormID = COALESCE(mspv.ParamValue,'')
	FROM tsmMigrateStepParam msp WITH (NOLOCK)
	LEFT OUTER JOIN tsmMigrateStepParamValue mspv WITH (NOLOCK)
		ON (mspv.SetupStepKey = msp.SetupStepKey
		AND mspv.ParamNo = msp.ParamNo)
	WHERE (mspv.MigrateSessionKey = @iSessionKey
		AND msp.ParamType = @DFLT_PARM_TYPE
		AND mspv.ParamNo = @DfltSOAckFormKey_Parm_No)

	-----------------------------------------------------------------------------
	-- Get other constant
	-- --------------------------------------------------------------------------
	-- Get LocalString Text
	SELECT 	@CI_MODULE_DESC = COALESCE(LocalText,'')
	FROM 	tsmLocalString WITH (NOLOCK)
	WHERE 	StringNo = @CI_MODULE_STRINGNO AND LanguageID = @LanguageID

	-- Get LocalString Text
	SELECT 	@AR_MODULE_DESC = COALESCE(LocalText,'')
	FROM 	tsmLocalString WITH (NOLOCK)
	WHERE 	StringNo = @AR_MODULE_STRINGNO AND LanguageID = @LanguageID

	-- Get LocalString Text
	SELECT 	@SO_MODULE_DESC = COALESCE(LocalText,'')
	FROM 	tsmLocalString WITH (NOLOCK)
	WHERE 	StringNo = @SO_MODULE_STRINGNO AND LanguageID = @LanguageID

	-- Get @HomeCurrID
	SELECT  @HomeCurrID = CurrID
	FROM 	tsmCompany WITH (NOLOCK)
	WHERE 	CompanyID = @iCompanyID

	SELECT 	@DigitsAfterDecimal = DigitsAfterDecimal
	FROM 	tmcCurrency WITH (NOLOCK)
	WHERE	CurrID = @HomeCurrID

	SELECT  @QtyDecPlaces = QtyDecPlaces,
			@UnitCostDecPlaces = UnitCostDecPlaces,
			@UnitPriceDecPlaces = UnitPriceDecPlaces
	FROM	tciOptions WITH (NOLOCK)
	WHERE	CompanyID = @iCompanyID

	SELECT	@TrackChngOrders = TrackChngOrders
	FROM	tsoOptions WITH (NOLOCK)
	WHERE	CompanyID = @iCompanyID

	-- Get Create Date
	SELECT 	@CreateDate = GETDATE()

	-----------------------------------------------------------------------------
	-- Check for cancel action.  Write out log record if any logging is requested
	-----------------------------------------------------------------------------
	IF @iCancel = 1
	BEGIN
		-- Log cancellation request to report log
		IF @iRptOption <> @RPT_PRINT_NONE
			BEGIN
				EXEC ciBuildString @RPT_STRINGNO_CANCELLED,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
	
				EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
				@iEntityID = '', @iColumnID = '', @iColumnValue = '',
				@iStatus = @MIG_STAT_INFO, @iDuplicate = 0, @iComment = @Message,
				@oRetVal = @RetVal OUTPUT
			END

		-- Return with success when user requests cancel
		SELECT 	@_oRetVal = @RET_SUCCESS
		GOTO CloseAndDeallocateCursors
		RETURN
	END

	-- Check Module activition
	IF NOT EXISTS (SELECT 1 FROM tsmCompanyModule WITH (NOLOCK) WHERE Active = 1 AND  ModuleNo = @MODULE_NO_AR AND CompanyID = @iCompanyID)
	BEGIN
		SELECT @VALIDATION_FAILED = 1
		-- Log error into report log
		IF (@iRptOption = @RPT_PRINT_ALL
		   OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
		   OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))
		BEGIN
			EXEC ciBuildString @RPT_STRINGNO_MODULE_INACTIVE,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, @AR_MODULE_DESC, @iCompanyID

			EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
		     	@iEntityID = '', @iColumnID = '', @iColumnValue = '',
		     	@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
		     	@oRetVal = @RetVal OUTPUT
		END

		-- Return with success when module is not activated.
		SELECT  @_oRetVal = @RET_SUCCESS
		GOTO CloseAndDeallocateCursors
	        RETURN

	END

	IF NOT EXISTS (SELECT 1 FROM tsmCompanyModule WITH (NOLOCK) WHERE Active = 1 AND  ModuleNo = @MODULE_NO_CI AND CompanyID = @iCompanyID)
	BEGIN
		SELECT @VALIDATION_FAILED = 1
		-- Log error into report log
		IF (@iRptOption = @RPT_PRINT_ALL
		   OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
		   OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))
		BEGIN
			EXEC ciBuildString @RPT_STRINGNO_MODULE_INACTIVE,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, @CI_MODULE_DESC, @iCompanyID

			EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
		     	@iEntityID = '', @iColumnID = '', @iColumnValue = '',
		     	@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
		     	@oRetVal = @RetVal OUTPUT
		END

		-- Return with success when module is not activated.
		SELECT  @_oRetVal = @RET_SUCCESS
		GOTO CloseAndDeallocateCursors
	        RETURN

	END

	IF NOT EXISTS (SELECT 1 FROM tsmCompanyModule WITH (NOLOCK) WHERE Active = 1 AND  ModuleNo = @MODULE_NO_SO AND CompanyID = @iCompanyID)
	BEGIN
		SELECT @VALIDATION_FAILED = 1
		-- Log error into report log
		IF (@iRptOption = @RPT_PRINT_ALL
		   OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
		   OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))
		BEGIN
			EXEC ciBuildString @RPT_STRINGNO_MODULE_INACTIVE,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, @SO_MODULE_DESC, @iCompanyID

			EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
		     	@iEntityID = '', @iColumnID = '', @iColumnValue = '',
		     	@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
		     	@oRetVal = @RetVal OUTPUT
		END

		-- Return with success when module is not activated.
		SELECT  @_oRetVal = @RET_SUCCESS
		GOTO CloseAndDeallocateCursors
	        RETURN

	END

	SELECT @Rec_Exist = 0
	
	IF @UseStageTable = 0
		SELECT @Rec_Exist = 1
		FROM   #StgSOLine a
		WHERE  a.SessionKey = @iSessionKey
		AND    LTRIM(RTRIM(COALESCE(a.TranNo,''))) NOT IN (SELECT TranNo
														   FROM   #StgSalesOrder
														   WHERE  SessionKey = @iSessionKey
														   AND	  TranNo  IS NOT NULL)
	ELSE
	  SELECT @Rec_Exist = 1
	  FROM  StgSOLine a WITH (NOLOCK)
	  WHERE a.SessionKey = @iSessionKey
	  AND LTRIM(RTRIM(COALESCE(a.TranNo,''))) NOT IN (SELECT TranNo
													  FROM StgSalesOrder WITH (NOLOCK)
													  WHERE	SessionKey = @iSessionKey
													  AND TranNo  IS NOT NULL)
	IF @Rec_Exist = 1
	BEGIN
		SELECT @VALIDATION_FAILED = 1
		-- Log error into report log
		IF (@iRptOption = @RPT_PRINT_ALL
		   OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
		   OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))
		BEGIN
		  EXEC ciBuildString @RPT_STRINGNO_HDR_REC_MISSING,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, @SO_MODULE_DESC, @iCompanyID

		  -- Write out to the final temp table for permenent error log table	
		  IF @UseStageTable = 0
			  INSERT tdmMigrationLogWrk (ColumnID, ColumnValue, Duplicate, Comment, EntityID, Status, SessionKey)
			  SELECT 'TranNo',LTRIM(RTRIM(COALESCE(a.TranNo,'(Blank)'))), 0, @Message, '#StgSOLine', @MIG_STAT_FAILURE, @iSessionKey
			  FROM	#StgSOLine a
			  WHERE	a.SessionKey = @iSessionKey
			  AND	LTRIM(RTRIM(COALESCE(a.TranNo,''))) NOT IN (SELECT TranNo
																FROM   #StgSalesOrder
																WHERE  SessionKey = @iSessionKey
																AND	   TranNo  IS NOT NULL)
			  OPTION(KEEPFIXED PLAN, KEEP PLAN)
		ELSE
			INSERT	 tdmMigrationLogWrk
					(ColumnID, ColumnValue, Duplicate, Comment, EntityID, Status, SessionKey)
			SELECT 	'TranNo',LTRIM(RTRIM(COALESCE(a.TranNo,'(Blank)'))), 0, @Message, 'StgSOLine', @MIG_STAT_FAILURE, @iSessionKey
			FROM	StgSOLine a WITH (NOLOCK)
			WHERE	a.SessionKey = @iSessionKey
			AND		LTRIM(RTRIM(COALESCE(a.TranNo,''))) NOT IN (SELECT TranNo
																FROM   StgSalesOrder WITH (NOLOCK)
																WHERE  SessionKey = @iSessionKey
																AND	   TranNo  IS NOT NULL)
			OPTION(KEEPFIXED PLAN, KEEP PLAN)
		END

		-- Return with success.
		SELECT  @_oRetVal = @RET_SUCCESS
		GOTO CloseAndDeallocateCursors
		RETURN
	END

	--------------------------------------------------
	-- Create Temp Tables
	--------------------------------------------------
	-- Create GL Validation temp tables.
	IF OBJECT_ID ('tempdb..#tdmMigrationLogEntryWrk') IS NULL
SELECT * INTO #tdmMigrationLogEntryWrk FROM tdmMigrationLogWrk WHERE 1=2

	-- Update Comment valid for #tdmMigrationLogWrk
	IF OBJECT_ID ('tempdb..#tciMessageBuilder') IS NULL
	CREATE TABLE #tciMessageBuilder (
		DMValGLAcctKey 	INTEGER		NULL,
		KeyValue	INTEGER		NOT NULL,
		ColumnID	VARCHAR(35)	NULL,
		ColumnValue	VARCHAR(100)	NULL,
		SessionKey	INTEGER		NOT NULL,
		MessageText	VARCHAR(255)	NULL,
		Duplicate	SMALLINT	NOT NULL,
		EntityID	VARCHAR (255)	NOT NULL DEFAULT '',
		Status		VARCHAR(15)	NULL,
		StringNo	INTEGER		NOT NULL,
		StringData0	VARCHAR(100)	NULL,
		StringData1	VARCHAR(30)	NULL,
		StringData2	VARCHAR(30)	NULL,
		StringData3	VARCHAR(30)	NULL,
		StringData4	VARCHAR(30)	NULL,
		StringData5	VARCHAR(30)	NULL,
		StringData6	VARCHAR(30)	NULL,
		StringData7	VARCHAR(30)	NULL,
		StringData8	VARCHAR(30)	NULL,
		StringData9	VARCHAR(30)	NULL,
		ParmPos		INTEGER 	NULL)

	IF OBJECT_ID ('tempdb..#tsoSOLine') IS NULL
		CREATE TABLE #tsoSOLine
(AllowDecQty        smallint     NULL,
		AllowDropShip      smallint     NULL,
		AllowPriceOvrd     smallint     NULL,
		AllowTradeDisc     smallint     NULL,
		BadDfltPrice       smallint     NULL,
		BadPriceMsg        int          NULL,
		CalcExtAmt         smallint     NULL,
		CalcPrice          smallint     NULL,
		CalcQty            smallint     NULL,
		CloseDate          datetime     NULL,
		CmntOnly           smallint     NOT NULL,
		CommClassKey       int          NULL,
		CommPlanKey        int          NULL,
		CustQuoteLineKey   int          NULL,
		Description        varchar(40)  NULL,
		DfltIMWhseKey      integer      NULL,
		DfltPrice          dec(15,5)    NULL,
		EstimateKey	   int		NULL,
		ExtCmnt            varchar(255) NULL,
		InclOnPackList     smallint     NOT NULL,
		InclOnPickList     smallint     NOT NULL,
		ItemGLAcctKey      int          NULL,
		ItemID             VARCHAR(30) 	NULL,
		ItemKey            int          NULL,
		NonInventory       smallint     NULL,
		OrigItemKey	   int		NULL,
		ReqCert            smallint     NOT NULL,
		SalesPromotionID   varchar(15)  NULL,
		SalesPromotionKey  int		NULL,
		SOKey              int          NOT NULL,
		SOLineKey          int          NOT NULL,
		SOLineNo           smallint     NULL,
		Status             smallint     NOT NULL,
		SystemPriceDetermination smallint NULL,
		UnitPriceFromSchedule    decimal(15,5) NULL,
		UnitPriceFromSalesPromo  decimal(15,5) NULL,
		STaxClassKey       int          NULL,
		UnitMeasKey        int          NULL,
		UnitPrice          dec(15,5)    NULL,
		UnitPriceOvrd      smallint     NOT NULL,
		UpdateCounter      int          NOT NULL,
		UserFld1           VARCHAR(15)	NULL,
		UserFld2           VARCHAR(15) 	NULL)

	IF OBJECT_ID ('tempdb..#tsoSOLineCompItem') IS NULL
		CREATE TABLE #tsoSOLineCompItem
	(SOLineCompItemKey int      NOT NULL,
	CompItemQty        dec(16,8) NOT NULL,
	CompItemKey        int       NOT NULL,
	SOLineKey          int       NOT NULL)

	IF OBJECT_ID ('tempdb..#DropShipLineVal') IS NULL
		CREATE TABLE #DropShipLineVal
		 (SOLineKey int NOT NULL,
	 POLineKey int NULL)

	IF OBJECT_ID ('tempdb..#KitCompListVal') IS NULL
		CREATE TABLE #KitCompListVal
		(RowKey int NOT NULL,
		SessionKey int NOT NULL,
		CompItemID VARCHAR(30) NOT NULL,
		CompItemKey int NULL,
		OrigCompItemQty dec(16,8) NOT NULL,
		CalcCompItemQty	dec(16,8) NULL,
		UOMID VARCHAR(6) NULL)

	IF OBJECT_ID ('tempdb..#tsoSOLineDist') IS NULL
		CREATE TABLE #tsoSOLineDist
	(SOLineDistKey     int       NOT NULL ,
	AcctRefKey         int       NULL ,
		AmtInvcd           dec(15,3) NOT NULL ,
		BlnktSOLineDistKey int       NULL ,
		CreatePO	   smallint  NULL,
		CreateShipTo       smallint  NULL,
		DeliveryMeth           smallint  NOT NULL ,
		ExtAmt             dec(15,3) NOT NULL ,
		FOBKey             int       NULL ,
		FreightAmt         dec(15,3) NOT NULL ,
		GLAcctKey          int       NULL ,
		Hold               smallint  NOT NULL,
		HoldReason         VARCHAR(20)  NULL,
		OrigOrdered        dec(16,8) NOT NULL ,
		OrigPromiseDate    datetime  NULL ,
		PromiseDate        datetime  NULL ,
		PurchVendAddrKey   int       NULL,
		OrigWhseKey	   int 	     NULL,
		QtyInvcd           dec(16,8) NOT NULL ,
		QtyOnBO            dec(16,8) NOT NULL ,
		QtyOpenToShip      dec(16,8) NOT NULL ,
		QtyOrd             dec(16,8) NOT NULL ,
		QtyRtrnCredit      dec(16,8) NOT NULL ,
		QtyRtrnReplacement dec(16,8) NOT NULL ,
		QtyShip            dec(16,8) NOT NULL ,
		RequestDate        datetime  NULL ,
		ShipDate           datetime  NULL ,
		ShipMethKey        int       NULL ,
		ShipPriority       smallint  NOT NULL ,
		ShipToAddrKey      int       NULL ,
		ShipToAddrName     VARCHAR(40)  NULL,
		ShipToAddrLine1    VARCHAR(40)  NULL,
		ShipToAddrLine2    VARCHAR(40)  NULL,
		ShipToAddrLine3    VARCHAR(40)  NULL,
		ShipToAddrLine4    VARCHAR(40)  NULL,
		ShipToAddrLine5    VARCHAR(40)  NULL,
		ShipToCity         VARCHAR(20)  NULL,
		ShipToCountryID    VARCHAR(03)  NULL,
		ShipToCustAddrKey  int       NULL ,
		ShipToCustAddrUpdateCntr int    NULL,
		ShipToPostalCode   VARCHAR(09)  NULL,
		ShipToState        VARCHAR(03)  NULL,
		ShipToTranOvrd     smallint  NULL,
		ShipZoneKey        int       NULL ,
		SOLineKey          int       NOT NULL ,
		Status             smallint  NOT NULL ,
		STaxTranKey        int       NULL ,
		TradeDiscAmt       dec(15,3) NULL,
		TradeDiscPct       dec(5,4)  NULL,
		VendKey            int       NULL ,
		WhseKey            int       NULL ,
		UpdateCounter      int       NOT NULL)
	
	IF OBJECT_ID ('tempdb..#tsoAPIValid') IS NULL
		CREATE TABLE #tsoAPIValid
(AcctRefUsage       smallint   NULL,	
		AckDate            datetime   NULL,	
		AmtInvcd           dec(15,3)  NULL,	
		ApprovalDate       datetime   NULL,	
		ApprovalStatus     smallint   NULL,	
		ARSTaxSchdKey      int        NULL,	
		AutoAcctAdd        smallint   NULL,	
		AutoAck	          smallint   NULL,
		BillToAddrKey      integer    NULL,	
		BillToCustAddrKey  integer    NULL,	
		BlnktRelNo         smallint   NULL,	
		BlnktSOKey         integer    NULL,	
		BTAddrLine1        VARCHAR(40)   NULL,	
		BTAddrLine2        VARCHAR(40)   NULL,	
		BTAddrLine3        VARCHAR(40)   NULL,	
		BTAddrLine4        VARCHAR(40)   NULL,	
		BTAddrLine5        VARCHAR(40)   NULL,	
		BTAddrName         VARCHAR(40)   NULL,	
		BTCity             VARCHAR(20)   NULL,	
		BTCountryID        VARCHAR(03)   NULL,	
		BTPostalCode       VARCHAR(09)   NULL,	
		BTState            VARCHAR(03)   NULL,	
		BTTransactionOverride smallint	NULL,
		CCOvrdSegKey       int        NULL,	
		ChkCreditLimit     smallint   NULL,	
		ChngOrdDate        datetime   NULL,	
		ChngOrdNo          smallint   NULL,	
		ChngReason         VARCHAR(40)   NULL,	
		ChngUserID         VARCHAR(30)   NULL,	
		ClassOvrdGL        smallint   NULL,	
		ClassOvrdSegValue  VARCHAR(15)   NULL,	
		CloseDate          datetime   NULL,	
		CntctKey           integer    NULL,	
		CompanyID          VARCHAR(03)   NULL,	
		ConfirmNo 	   varchar(10)  NULL,
		CrCardAuthNo       VARCHAR(15)   NULL,	
		CrCardExp          VARCHAR(10)   NULL,	
		CrCardNo           VARCHAR(20)   NULL,	
		CreateBillTo       smallint   NULL,	
		CreateShipTo       smallint   NULL,	
		CreditLimit        dec(15,3)  NULL,	
		CreditLimitAgeCat  smallint   NULL,	
		CurrExchRate       float      NULL,	
		CurrExchSchdKey    integer    NULL,	
		CurrID             VARCHAR(03)   NULL,	
		CustAcctKey        integer    NULL,	
		CustClassKey       integer    NULL,	
		CustID             VARCHAR(12)   NULL,	
		CustKey            integer    NULL,	
		CustPONo           VARCHAR(15)   NULL,	
		CustQuoteKey       integer    NULL,	
		CustSTaxSchdKey    integer    NULL,	
		CustWhseKey        integer    NULL,	
		DfltAcctRefKey     integer    NULL,	
		DfltCommPlanKey    integer    NULL,	
		DfltCreatePO	   smallint   NULL,
		DfltDeliveryMeth   smallint   NULL,	
		DfltFOBKey         integer    NULL,	
		DfltPriority       smallint   NULL,	
		DfltPromDate       datetime   NULL,	
		DfltPurchVAddrKey  integer    NULL,	
		DfltQuoteFormKey   int        NULL,	
		DfltRequestDate    datetime   NULL,	
		DfltShipDate       datetime   NULL,	
		DfltShipMethKey    integer    NULL,	
		DfltShipToAddrKey  integer    NULL,	
		DfltShipToCAddrKey integer    NULL,	
		DfltShipZoneKey    integer    NULL,	
		DfltSOAckFormKey   int        NULL,	
		DfltSOWhseKey      int        NULL,	
		DfltVendKey        integer    NULL,	
		DfltWhseKey        integer    NULL,	
		DocRoundAmt        smallint   NULL,	
		DropShipLeadTime  smallint   NULL,	
		DSTAddrLine1       VARCHAR(40)   NULL,	
		DSTAddrLine2       VARCHAR(40)   NULL,	
		DSTAddrLine3       VARCHAR(40)   NULL,	
		DSTAddrLine4       VARCHAR(40)   NULL,	
		DSTAddrLine5       VARCHAR(40)   NULL,	
		DSTAddrName        VARCHAR(40)   NULL,	
		DSTCity            VARCHAR(20)   NULL,	
		DSTCountryID       VARCHAR(03)   NULL,	
		DSTPostalCode      VARCHAR(09)   NULL,	
		DSTState           VARCHAR(03)   NULL,	
		DSTTransactionOverride smallint NULL,
		Expiration         datetime   NULL,	
		FreightAmt         dec(15,3)  NULL,	
		FreightMethod	   smallint   NULL,
		Hold               smallint   NULL,	
		HoldReason         VARCHAR(20)   NULL,	
		HomeCurrDP         smallint   NULL,	
		HomeCurrID         VARCHAR(03)   NULL,	
		ImportLogKey       integer    NULL,	
		IntegrateWithIM    smallint   NULL,	
		LastRetVal         smallint   NULL,	
		LogSuccessful      smallint   NULL,	
		MarginTolerance    dec(15,3)  NULL,	
		NextLineNo         integer    NULL,	
		OpenAmt            dec(15,3)  NULL,	
		OpenAmtHC          dec(15,3)  NULL,	
		OpenOrdersInCC     smallint   NULL,	
		PmtTermsKey        integer    NULL,	
		PrimarySperKey     integer    NULL,	
		QtyDecPlaces       smallint   NULL,	
		RecordNumber       int        NULL,	
		RecurSOKey         integer    NULL,	
		RequireSOAck	  smallint   NULL,
		RequireSource      smallint   NULL,	
		SalesAmt           dec(15,3)  NULL,	
		SalesAmtHC         dec(15,3)  NULL,	
		SalesSourceKey     integer    NULL,	
		ShipDays           smallint   NULL,	
		SOAckFormKey       integer    NULL,	
		SOKey              int        NULL,	
		Spid               int        NULL,	
		Status             smallint   NULL,	
		STaxAmt            dec(15,3)  NULL,	
		STaxCalc           smallint   NULL,	
		STaxTranKey        integer    NULL,	
		TenderTypeKey      int        NULL,	
		TrackSTax          smallint   NULL,	
		TradeDiscPct       smallint   NULL,	
		TranAmt            dec(15,3)  NULL,	
		TranAmtHC          dec(15,3)  NULL,	
		TranCmnt           VARCHAR(50)   NULL,	
		TranDate           datetime   NULL,	
		TranID             VARCHAR(13)   NULL,	
		TranIDChngChar     VARCHAR(1)    NULL,	
		TranIDRelChar      VARCHAR(1)    NULL,	
		TranNo             VARCHAR(10)   NULL,	
		TranNoChngOrd      VARCHAR(15)   NULL,	
		TranNoRel          VARCHAR(15)   NULL,	
		TranNoRelChngOrd   VARCHAR(20)   NULL,	
		TranType           integer    NULL,	
		UFDataType1        smallint   NULL,	
		UFDataType2        smallint   NULL,	
		UFDataType3        smallint   NULL,	
		UFDataType4        smallint   NULL,	
		UFDataTypel1       smallint   NULL,	
		UFDataTypel2       smallint   NULL,	
		UFKey1             integer    NULL,	
		UFKey2             integer    NULL,	
		UFKey3             integer    NULL,	
		UFKey4             integer    NULL,	
		UFKeyl1            integer    NULL,	
		UFKeyl2            integer    NULL,	
		UFUsage1           smallint   NULL,	
		UFUsage2           smallint   NULL,	
		UFUsage3           smallint   NULL,	
		UFUsage4           smallint   NULL,	
		UFUsagel1          smallint   NULL,	
		UFUsagel2          smallint   NULL,	
		UniqueID           VARCHAR(80)   NULL,	
		UnitPriceDecPlaces smallint   NULL,	
		UpdateBillTo	   smallint   NULL,
		UpdateShipTo       smallint   NULL,
		UpdateCounter      integer    NULL,	
		UpdateDate         datetime   NULL,	
		UpdateUserID       VARCHAR(30)   NULL,	
		UseBlnktRelNos     smallint   NULL,	
		UseMultCurr        smallint   NULL,	
		UserFld1           VARCHAR(15)   NULL,	
		UserFld2           VARCHAR(15)   NULL,	
		UserFld3           VARCHAR(15)   NULL,	
		UserFld4           VARCHAR(15)   NULL,	
		UserID             VARCHAR(30)   NULL,	
		UseSameRangeForBlanket smallint   NULL,	
		UseSameRangeForQuote   smallint   NULL,
		UseSper            smallint   NULL)

	IF OBJECT_ID ('tempdb..#tciSTaxCodeTran') IS NULL
		SELECT * INTO #tciSTaxCodeTran
		FROM tciSTaxCodeTran
		WHERE 1 = 2

	IF OBJECT_ID ('tempdb..#tciSTaxTran') IS NULL
		CREATE TABLE #tciSTaxTran
	(STaxTranKey      int       NOT NULL,
	STaxSchdKey       int       NULL,
	NeedInsert        smallint  NOT NULL)

	IF OBJECT_ID ('tempdb..#tciSTaxDelete') IS NULL
		CREATE TABLE #tciSTaxDelete
	(STaxTranKey      int       NOT NULL,
	NeedsDelete       smallint  NOT NULL)

	-- Temp Table for Qty, Amt decimal digits validation
	IF OBJECT_ID ('tempdb..#QtyAmtValCol') IS NULL
		CREATE TABLE #QtyAmtValCol (ColNum int not null identity(1,1),
					ColumnName varchar(255) NULL,
					ColumnType smallint NULL,
					TableName varchar(255) NULL,
					SessionKey int NULL)
	ELSE	
		TRUNCATE TABLE #QtyAmtValCol

	IF OBJECT_ID ('tempdb..#SOHdrQtyAmtValData') IS NULL
		CREATE TABLE #SOHdrQtyAmtValData
		    (RowKey 		int not null,
		     SessionKey 	int not null,
		     EntityID 		varchar(255) null,
		     FreightAmt		decimal(15,3) null,
		     OpenAmt 		decimal(15,3) null,
		     STaxAmt		decimal(15,3) null,
		     TradeDiscAmt	decimal(15,3) null,
		     CurrID 		VARCHAR(3) null)

	IF OBJECT_ID ('tempdb..#SODetlQtyAmtValData') IS NULL
		CREATE TABLE #SODetlQtyAmtValData
		    (RowKey 		int not null,
		     SessionKey 	int not null,
		     EntityID 		varchar(255) null,
		     AmtInvcd		decimal(15,3) null,
		     ExtAmt 		decimal(15,3) null,
		     OrigOrdered	decimal(16,8) null,
		     QtyInvcd		decimal(16,8) null,
		     QtyOnBO		decimal(16,8) null,
		     QtyOrd		decimal(16,8) null,
		     QtyRtrnCredit	decimal(16,8) null,
		     QtyRtrnReplacement	decimal(16,8) null,
		     QtyShip		decimal(16,8) null,
		     TradeDiscAmt	decimal(15,3) null,
		     UnitPrice		decimal(15,5) null,
		     CurrID 		VARCHAR(3) null)

	IF OBJECT_ID ('tempdb..#tmpFrtAlloc') IS NULL
		CREATE TABLE #tmpFrtAlloc
	       	(SOKey 		INT NOT NULL,
		SOLineKey 	INT NOT NULL,
		SOLineDistKey 	INT NOT NULL,
	       	QtyOrd		DECIMAL(16,8) NULL,
		ExtAmt		DECIMAL(15,3) NULL,
		FreightAmt	DECIMAL(15,3) NULL)


	IF OBJECT_ID ('tempdb..#ShipToAddrs') IS NULL
		CREATE TABLE #ShipToAddrs
		(OrigShipAddrKey    int       NULL,
		NewShipAddrKey     int        NOT NULL)

	IF OBJECT_ID ('tempdb..#GLAcctColumns') IS NULL
		CREATE TABLE #GLAcctColumns (ColumnName varchar(255))

	IF object_id('tempdb..#SalesOrderUpdColumnList') IS NULL
	CREATE TABLE #SalesOrderUpdColumnList
		(SessionKey int NOT NULL,
		 ColumnName varchar(255) NOT NULL)


	--Build Index
	If @UseStageTable = 0
	BEGIN
		EXEC spDMCreateTmpTblIndex '#StgSalesOrder', 'RowKey', @RetVal OUTPUT

		IF @RetVal <> @RET_SUCCESS
	BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
		RETURN
	END

		EXEC spDMCreateTmpTblIndex '#StgSOLine', 'RowKey', @RetVal OUTPUT

		IF @RetVal <> @RET_SUCCESS
	BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
		RETURN
	END

		IF NOT EXISTS (	
			SELECT * FROM tempdb..sysindexes
			WHERE id = object_id('tempdb..#StgSOLine')
			AND (indid = 1 OR name = '#StgSOLine_NONCLUSTERED_index_5t813vre5')
			)
		BEGIN

			-----------------------------------------------------------------------------------------------
			-- Please do not remove next line (comment), it's required for the data replication because of
			-- a bug on the SQL Server 2005 replication engine. See Scopus 37655. 06/20/07
			-- CREATE CLUSTERED INDEX #StgSOLine_NONCLUSTERED_index_5t813vre5
			-- ON #StgSOLine (TranNo, SessionKey, RowKey)
			-----------------------------------------------------------------------------------------------
			CREATE NONCLUSTERED INDEX #StgSOLine_NONCLUSTERED_index_5t813vre5
			ON #StgSOLine (TranNo, SessionKey, RowKey)
		END

	END

	-- Initialize the total record count if this is the first call to this sp.
	-- Because this routine will only process @MAX_ROWS_PER_RUN if staging tables
	-- are being used, it may be called more than once.
	IF @oTotalRecs = 0
		IF @UseStageTable = 1
			SELECT @oTotalRecs = (SELECT COUNT(*) FROM StgSalesOrder WHERE ProcessStatus = @NOT_PROCESSED AND SessionKey = @iSessionKey)
		ELSE
			SELECT @oTotalRecs = (SELECT COUNT(*) FROM #StgSalesOrder WHERE ProcessStatus = @NOT_PROCESSED)

	IF (@UseStageTable = 1)
	BEGIN
		--If the caller wants me to use the staging table, then I'll create my own copy of the temporary tables
		--(if they haven't already been created).
		IF OBJECT_ID('tempdb..#StgSalesOrder') IS NOT NULL
			SELECT @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
		--Temp table not found
		ELSE
		BEGIN
			SELECT @TempTableCreation = @CREATED_INSIDE_THIS_SP
			SELECT * INTO #StgSalesOrder FROM StgSalesOrder WITH (NOLOCK) WHERE 1 = 2
		END

		IF OBJECT_ID('tempdb..#StgSOLine') IS NOT NULL
			SELECT @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
		--Temp table not found
		ELSE
		BEGIN
			SELECT @TempTableCreation = @CREATED_INSIDE_THIS_SP
			SELECT * INTO #StgSOLine FROM StgSOLine WITH (NOLOCK) WHERE 1 = 2
		END
	END
	ELSE
	BEGIN
		SELECT @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
	END

	TRUNCATE TABLE #QtyAmtValCol
	-- Validate SO Header Qyt, Amt columns #SOHdrQtyAmtValData
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'FreightAmt', @ColType_AmtNC, '#SOHdrQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'OpenAmt', @ColType_AmtNC, '#SOHdrQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'STaxAmt', @ColType_AmtNC, '#SOHdrQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'TradeDiscAmt', @ColType_AmtNC, '#SOHdrQtyAmtValData', @iSessionKey

	-- Validate SO Detail Qyt, Amt columns #SODetlQtyAmtValData
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'AmtInvcd', @ColType_AmtNC, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'ExtAmt', @ColType_AmtNC, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'TradeDiscAmt', @ColType_AmtNC, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'OrigOrdered', @ColType_Qty, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'QtyInvcd', @ColType_Qty, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'QtyOnBO', @ColType_Qty, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'QtyRtrnCredit', @ColType_Qty, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'QtyRtrnReplacement', @ColType_Qty, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'QtyShip', @ColType_Qty, '#SODetlQtyAmtValData', @iSessionKey
	INSERT #QtyAmtValCol (ColumnName, ColumnType, TableName, SessionKey) SELECT 'UnitPrice', @ColType_PRICE, '#SODetlQtyAmtValData', @iSessionKey

	--Cursor Not Allocated
	IF CURSOR_STATUS ('global', 'curSalesOrder') = @STATUS_CURSOR_DOES_NOT_EXIST
	BEGIN		
		DECLARE curSalesOrder INSENSITIVE CURSOR FOR
		SELECT	a.RowKey
		FROM 	#StgSalesOrder a WITH (NOLOCK)
	        ORDER BY a.Action
		-- This is Insert/Update/Delete flag (Ins/Upd first, Del 2nd)
	END
		
	-----------------------------------------------------------------------
	-- Start of response time control loop.  Attempts to set the appoximate
	-- response time to @MIGRATE_SECS.
	-----------------------------------------------------------------------
	
	SELECT @StartDate = GetDate()
	WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS OR @UseStageTable = 0
	BEGIN
	
		--	Create temp tables if necessary.
		IF @UseStageTable = 1
			BEGIN

				--Update Action flag to 'Insert' if it is NULL
				UPDATE	StgSalesOrder
				SET		Action = @REC_INSERT
				WHERE	Action = @REC_AUTO
				OR		Action IS NULL

--				--Update Action flag if it is specified as Automatic
--				--Set Action to 'Update' if record already exists in database
--				--Otherwise, set the Action to 'Insert'.
--				UPDATE 	StgSalesOrder
--				SET	Action = CASE WHEN so.TranNo IS NOT NULL THEN @REC_UPDATE
--							ELSE @REC_INSERT END
--				FROM	StgSalesOrder
--				LEFT OUTER JOIN tsoSalesOrder so WITH (NOLOCK)
--				ON	StgSalesOrder.TranNo = so.TranNo
--				AND	so.CompanyID = @iCompanyID
--				WHERE 	StgSalesOrder.Action = @REC_AUTO
--				OPTION(KEEP PLAN)


				-- If the caller wants me to use the staging table, then I'll create my own copy
				-- of the temporary tables.
				TRUNCATE TABLE #StgSalesOrder

			        IF @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
			        BEGIN
			           	SET ROWCOUNT @MAX_ROWS_PER_RUN
			        END /* End @_lTempTableCreation = @CREATED_OUTSIDE_THIS_SP */
			        ELSE
			        BEGIN
			         	SET ROWCOUNT 0
			        END /* End @_lTempTableCreation <> @CREATED_OUTSIDE_THIS_SP */

				SET IDENTITY_INSERT #StgSalesOrder ON

				--Insert 'Action-Insert' records into #StgSalesOrder
				INSERT INTO #StgSalesOrder (
					RowKey,			ProcessStatus,		SessionKey,
					AckDate,		BillToAddrLine1,	BillToAddrLine2,
					BillToAddrLine3,	BillToAddrLine4,	BillToAddrLine5,
					BillToAddrName,		BillToCity,		BillToCountryID,
					BillToPostalCode,	BillToStateID,		BillToCustAddrID,
					CloseDate,		ContactName,		ConfirmNo,
					CurrExchRate,		CurrID,			CustID,
					CustClassID,		CustPONo,		DfltAcctRefCode,
					CurrExchSchdID,		DfltPurchVAddrID,
					DfltCommPlanID,		DfltFOBID,		DfltPromDate,
					DfltRequestDate,	DfltShipDate,		DfltShipMethID,
					DfltShipPriority,	DfltShipToAddrLine1,	DfltShipToAddrLine2,
					DfltShipToAddrLine3,	DfltShipToAddrLine4,	DfltShipToAddrLine5,
					DfltShipToAddrName,	DfltShipToCity,	DfltShipToCountryID,
					DfltShipToPostalCode,	DfltShipToStateID,	DfltShipToCustAddrID,
					DfltWarehouseID,	Expiration,		FreightAmt,
					Hold,			HoldReason,		OpenAmt,
					PmtTermsID,		PrimarySperID,		RecurSOTranNo,
					RequireSOAck,		SalesSourceID,		SOAckFormID,
					Status,			STaxAmt,		
					TradeDiscAmt,		TranCmnt,		TranDate,
					TranNo,			UserFld1,		UserFld2,
					UserFld3,		UserFld4,		Action)
				SELECT
					RowKey,			ProcessStatus,		SessionKey,
					AckDate,		BillToAddrLine1,	BillToAddrLine2,
					BillToAddrLine3,	BillToAddrLine4,	BillToAddrLine5,
					BillToAddrName,		BillToCity,		BillToCountryID,
					BillToPostalCode,	BillToStateID,		BillToCustAddrID,
					CloseDate,		ContactName,		ConfirmNo,
					CurrExchRate,		CurrID,			CustID,
					CustClassID,		CustPONo,		DfltAcctRefCode,
					CurrExchSchdID,		DfltPurchVAddrID,
					DfltCommPlanID,		DfltFOBID,		DfltPromDate,
					DfltRequestDate,	DfltShipDate,		DfltShipMethID,
					DfltShipPriority,	DfltShipToAddrLine1,	DfltShipToAddrLine2,
					DfltShipToAddrLine3,	DfltShipToAddrLine4,	DfltShipToAddrLine5,
					DfltShipToAddrName,	DfltShipToCity,	DfltShipToCountryID,
					DfltShipToPostalCode,	DfltShipToStateID,	DfltShipToCustAddrID,
					DfltWarehouseID,	Expiration,		FreightAmt,
					Hold,			HoldReason,		OpenAmt,
					PmtTermsID,		PrimarySperID,		RecurSOTranNo,
					RequireSOAck,		SalesSourceID,		SOAckFormID,
					Status,			STaxAmt,		
					TradeDiscAmt,		TranCmnt,		TranDate,
					TranNo,			UserFld1,		UserFld2,
					UserFld3,		UserFld4,		Action				
				FROM 	StgSalesOrder WITH (NOLOCK)
				WHERE 	SessionKey = @iSessionKey
				AND 	ProcessStatus = @NOT_PROCESSED
				AND 	Action = @REC_INSERT
				OPTION(KEEP PLAN)

				--Insert 'Action-Update' records into #StgSalesOrder
				--First check if there are updatable columns specified
				--for StgSalesOrder in the VI Job
				SET ROWCOUNT 0

				SELECT @UpdColList = ''

				TRUNCATE TABLE #SalesOrderUpdColumnList
				INSERT INTO #SalesOrderUpdColumnList
						(SessionKey, ColumnName)
				SELECT @iSessionkey, tc.InternalColumnName
				FROM 	tdiDataTrnsfrColumnMap tc WITH (NOLOCK)
					JOIN	(SELECT DISTINCT DataTrnsfrjobDefStepKey
									FROM tdiDataTrnsfrJobDefStep tjs WITH (NOLOCK)
										JOIN 	tdiDataTrnsfrJobExecute tj WITH (NOLOCK)	ON	tjs.DataTrnsfrJobDefKey = tj.DataTrnsfrJobDefKey
									WHERE tj.SessionKey = @iSessionkey AND tjs.InternalTableName = 'StgSalesOrder') derv
								ON	tc.DataTrnsfrjobDefStepKey = derv.DataTrnsfrjobDefStepKey
				WHERE	tc.AllowUpdate = 1 		
				OPTION(KEEP PLAN)	

				SELECT	@UpdColList = @UpdColList + ColumnName,
						@UpdColList = @UpdColList + ', '
				FROM 	#SalesOrderUpdColumnList	
				OPTION(KEEP PLAN)		
			
				--If there are updatable column specified
				--Create the insert statement to get the data
				--to be updated from StgSalesOrder to #StgSalesOrder
				IF LEN(RTRIM(LTRIM(@UpdColList))) > 0
				BEGIN
					SELECT @UpdColList = SUBSTRING(@UpdColList, 1, LEN(@UpdColList) - 1)
					SELECT @UpdColList = 'RowKey, TranNo, ProcessStatus, Action, SessionKey, ' + @UpdColList
	
					SELECT @SQLStmt = ''
					SELECT @SQLStmt = 'INSERT INTO #StgSalesOrder (' + RTRIM(LTRIM(@UpdColList)) + ') SELECT ' + RTRIM(LTRIM(@UpdColList)) + ' FROM StgSalesOrder WHERE SessionKey = ' + CONVERT(VARCHAR(10), @iSessionKey) + ' AND ProcessStatus = ' + CONVERT(VARCHAR(1), @NOT_PROCESSED) + ' AND Action = 2 OPTION(KEEP PLAN)'
	
					EXECUTE (@SQLStmt)	
				END
				ELSE
				BEGIN
				-- If there is no updatable column specified but there are records with the 'Update' flag, mark the records
				-- to "Fail"
					-- Raise flag to skip insert into final table.
					TRUNCATE TABLE #tciMessageBuilder
					TRUNCATE TABLE #tdmMigrationLogEntryWrk

					-- Check for count of unverified records with the 'Update' flag
					IF @UseStageTable = 1
							SELECT	 @UpdRecCount = COUNT(*) FROM StgSalesOrder
							WHERE	SessionKey = @iSessionKey
							AND		ProcessStatus = @NOT_PROCESSED
							AND		Action = @REC_UPDATE
							OPTION(KEEP PLAN)
					ELSE	
							SELECT @UpdRecCount = COUNT(*) FROM #StgSalesOrder
							WHERE	SessionKey = @iSessionKey
							AND		ProcessStatus = @NOT_PROCESSED
							AND		Action = @REC_UPDATE
							OPTION(KEEP PLAN)
				
					-- Record found, run validation
					IF @UpdRecCount > 0
					BEGIN
		
						SELECT @EntityColName = 'COALESCE(TranNo, '''')'
					
						EXEC spDMValidateUpdColforUpdRec @iSessionKey, @UseStageTable,'StgSalesOrder', @EntityColName,
								@LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @ErrorCount OUTPUT, @RetVal OUTPUT

						-- Update record count
						SELECT	@oFailedRecs = @oFailedRecs + @ErrorCount
						SELECT	@oRecsProcessed = @oRecsProcessed + @ErrorCount

						IF @RetVal <> @RET_SUCCESS
						BEGIN
							SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
							RETURN
						END
					END
				END

				SET IDENTITY_INSERT #StgSalesOrder OFF
				SET ROWCOUNT 0

				-- Load the SO Lines for the current set of Sales Order
				-- to be processed
				TRUNCATE TABLE #StgSOLine
				SET IDENTITY_INSERT #StgSOLine ON

				INSERT INTO #StgSOLine (
					RowKey,			ProcessStatus,		SessionKey,
					AcctRefCode,		AmtInvcd,		CloseDate,
					CmntOnly,		CommClassID,		CommPlanID,
					KitComponent,		Description,		ExtAmt,
					ExtCmnt,		FOBID,			FreightAmt,
					GLAcctNo,		Hold,			HoldReason,
					ItemAliasID,		ItemID,			OrigOrdered,
					OrigPromiseDate,	PromiseDate,		QtyInvcd,
					QtyOnBO,		QtyOrd,			QtyRtrnCredit,
					QtyRtrnReplacement,	QtyShip,		ReqCert,
					RequestDate,		ShipDate,		ShipMethID,
					ShipPriority,		ShipToAddrLine1,	ShipToAddrLine2,
					ShipToAddrLine3,	ShipToAddrLine4,	ShipToAddrLine5,
					ShipToAddrName,		ShipToCity,		ShipToCountryID,
					ShipToPostalCode,	ShipToStateID,	
					SOLineNo,		Status,			STaxClassID,
					TradeDiscAmt,		TradeDiscPct,		TranNo,
					UnitMeasID,		UnitPrice,
					UserFld1,		UserFld2,		WarehouseID,
					DeliveryMeth,		VendorID,		PONumber,
					Action)
				SELECT
					RowKey,			ProcessStatus,		SessionKey,
					AcctRefCode,		AmtInvcd,		CloseDate,
					CmntOnly,		CommClassID,		CommPlanID,
					KitComponent,		Description,		ExtAmt,
					ExtCmnt,		FOBID,			FreightAmt,
					GLAcctNo,		Hold,			HoldReason,
					ItemAliasID,		ItemID,			OrigOrdered,
					OrigPromiseDate,	PromiseDate,		QtyInvcd,
					QtyOnBO,		QtyOrd,			QtyRtrnCredit,
					QtyRtrnReplacement,	QtyShip,		ReqCert,
					RequestDate,		ShipDate,		ShipMethID,
					ShipPriority,		ShipToAddrLine1,	ShipToAddrLine2,
					ShipToAddrLine3,	ShipToAddrLine4,	ShipToAddrLine5,
					ShipToAddrName,		ShipToCity,		ShipToCountryID,
					ShipToPostalCode,	ShipToStateID,	
					SOLineNo,		Status,			STaxClassID,
					TradeDiscAmt,		TradeDiscPct,		TranNo,
					UnitMeasID,		UnitPrice,
					UserFld1,		UserFld2,		WarehouseID,
					DeliveryMeth,		VendorID,		PONumber,
					Action
				FROM StgSOLine SOL WITH (NOLOCK)
				WHERE TranNo IN (SELECT TranNo FROM #StgSalesOrder WITH (NOLOCK))
				AND SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
				OPTION(KEEP PLAN)
				
				SET IDENTITY_INSERT #StgSOLine OFF

			END
			ELSE
			BEGIN
				--Update Action flag to 'Insert' if it is NULL
				UPDATE	#StgSalesOrder
				SET		Action = @REC_INSERT
				WHERE	Action = @REC_AUTO
				OR		Action IS NULL
		
--				--Update Action flag if it is specified as Automatic
--				--Set Action to 'Update' if record already exists in database
--				--Otherwise, set the Action to 'Insert'.
--				UPDATE 	#StgSalesOrder
--				SET	Action = CASE WHEN so.TranNo IS NOT NULL THEN @REC_UPDATE
--							ELSE @REC_INSERT END
--				FROM	#StgSalesOrder
--				LEFT OUTER JOIN tsoSalesOrder so WITH (NOLOCK)
--				ON	#StgSalesOrder.TranNo = so.TranNo
--				AND	so.CompanyID = @iCompanyID
--				WHERE 	#StgSalesOrder.Action = @REC_AUTO
--				OPTION(KEEP PLAN)
			END

--------------------------
--FOR FUTURE USE
---------------------------
				--Update Action flag if it is specified as Automatic
				--Set Action to 'Update' if record already exists in database
				--Otherwise, set the Action to 'Insert'.
				UPDATE 	#StgSOLine
				SET		Action = @REC_INSERT
				WHERE	Action = @REC_AUTO
				OR		Action IS NULL

-- 				--UPDATE Action flag if it is specified as Automatic
-- 				--Set Action to 'Update' if record already exists in database
-- 				--Otherwise, set the Action to 'Insert'.
-- 				UPDATE 	#StgSOLine
-- 				SET	Action = CASE WHEN so.TranNo IS NOT NULL THEN @REC_UPDATE
-- 							ELSE @REC_INSERT END
-- 				FROM	#StgSOLine
-- 				LEFT OUTER JOIN tsoSalesOrder so WITH (NOLOCK)
-- 				ON	#StgSOLine.TranNo = so.TranNo
-- 				AND	so.CompanyID = @iCompanyID
-- 				WHERE 	#StgSOLine.Action = @REC_AUTO
-- 				OPTION(KEEP PLAN)
---------------------------
--FOR FUTURE USE
---------------------------
		
		-- Determine if any remaining rows exist for insertion.  If not, then consider this successful.
		SELECT @SourceRowCount = COUNT(*) FROM #StgSalesOrder  WITH (NOLOCK) WHERE ProcessStatus = @NOT_PROCESSED

		-- Exit if there are no rows to migrate.  Consider this success and tell the caller
		-- not to continue calling this SP.
		IF @SourceRowCount = 0
			BEGIN
				SELECT @_oRetVal = @RET_SUCCESS, @oContinue = 0
				BREAK
			END

		----------------------------------------------------------------------------
		-- Start of primary logic.
		-- We will process payment terms into tapVendClass row by row.
		-- The only real validation we have to contend with is duplicate values.
		--
		-- Process the rows until we're "done", where done either means we ran out
		-- of rows or we were told to stop.  If the original data was obtained
		-- from staging tables, then we will only process up to the maximum number
		-- of rows that will result in decent response time.  If the data was
		-- passed in the temporary tables directly, then we will process all rows.
		-----------------------------------------------------------------------------
		-- For enumerate fields, convert the staging table's column values
		-- to Enterprise's enumerated values.  If they cannot be enumerated,
		-- leave the value as is so it can be reported.

		-- Default SO Status if the Status if blank
		UPDATE	#StgSalesOrder
		SET		Status = CASE WHEN @SetupStepKey = @SOInsertion_OPEN
								THEN @SOStatus_OPEN
						ELSE @SOStatus_CLOSED
						END
		WHERE	NULLIF(Status, '') IS NULL

		-- Default SO Line Status if the Status if blank
		UPDATE	#StgSOLine
		SET		Status = CASE WHEN @SetupStepKey = @SOInsertion_OPEN
								THEN @SOStatus_OPEN
						ELSE @SOStatus_CLOSED
						END
		WHERE	NULLIF(Status, '') IS NULL

		-- Get Enums for DiscDateOption Fields
		EXEC spDMListReplacement '#StgSalesOrder', 'Status', 'tsoSalesOrder', 'Status', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		EXEC spDMListReplacement '#StgSalesOrder', 'Action', 'StgSalesOrder', 'Action', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		EXEC spDMListReplacement '#StgSOLine', 'Status', 'tsoSOLine', 'Status', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		EXEC spDMListReplacement '#StgSOLine', 'Action', 'StgSOLine', 'Action', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END


		EXEC spDMListReplacement '#StgSOLine', 'DeliveryMeth', 'tsoSOLineDist', 'DeliveryMeth', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		-- Update the YesNo fields
		EXEC spDMYesNoCodeUpd '#StgSalesOrder', 'StgSalesOrder', @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		-- Update the YesNo fields
		EXEC spDMYesNoCodeUpd '#StgSOLine', 'StgSOLine', @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		-- Insert default values for the temporary table columns containing null, where the
		-- corresponding permanent table column has a default rule.
		EXEC spDMTmpTblDfltUpd '#StgSalesOrder', 'tsoSalesOrder', @RetVal OUTPUT, @ACTION_COL_USED
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		EXEC spDMTmpTblDfltUpd '#StgSOLine', 'tsoSOLine', @RetVal OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		EXEC spDMTmpTblDfltUpd '#StgSOLine', 'tsoSOLineDist', @RetVal OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		--Strip GL Acct Mast
		TRUNCATE TABLE #GLAcctColumns
		
		INSERT #GLAcctColumns SELECT 'GLAcctNo'

		EXEC spDMMASGLMaskStrip '#StgSOLine', @RetVal  OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		--Strip out the postal code mask
		EXEC spDMStrMaskStrip '#StgSalesOrder', 'StgSalesOrder', @RetVal  OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		EXEC spDMStrMaskStrip '#StgSOLine', 'StgSOLine', @RetVal  OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		-- Validate SO Header Qyt, Amt columns
		TRUNCATE TABLE #tciMessageBuilder
		TRUNCATE TABLE #tdmMigrationLogEntryWrk

		INSERT INTO #SOHdrQtyAmtValData
		    	(RowKey,		SessionKey,	EntityID,
		     	FreightAmt,		OpenAmt,	STaxAmt,
		     	TradeDiscAmt,		CurrID)
		SELECT  RowKey,			SessionKey,	LTRIM(RTRIM(TranNo)),
		     	FreightAmt,		OpenAmt,	STaxAmt,
		     	TradeDiscAmt,		COALESCE(CurrID, @HomeCurrID)
		FROM	#StgSalesOrder  WITH (NOLOCK)
		OPTION(KEEPFIXED PLAN, KEEP PLAN)

		EXEC spDMValidateQtyAmt @iSessionKey, '#SOHdrQtyAmtValData','#StgSalesOrder',
		        @QtyDecPlaces, @UnitCostDecPlaces, @UnitPriceDecPlaces, @HomeCurrID,
			@DigitsAfterDecimal, @LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @RetVal OUTPUT

		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			RETURN
		END

		-- Validate SO Detail Qyt, Amt columns

		TRUNCATE TABLE #tciMessageBuilder
		TRUNCATE TABLE #tdmMigrationLogEntryWrk

		INSERT INTO #SODetlQtyAmtValData
		    	(RowKey,		SessionKey,		EntityID,
		     	AmtInvcd,		ExtAmt, 		OrigOrdered,	
				QtyInvcd,		QtyOnBO,		QtyOrd,		
				QtyRtrnCredit,		QtyRtrnReplacement,	QtyShip,		
				TradeDiscAmt,	UnitPrice,	CurrID)
		SELECT  SOL.RowKey,		SOL.SessionKey,		LTRIM(RTRIM(SOL.TranNo)) + '|Item ' + COALESCE(SOL.ItemID, '') + ' Line',
		     	SOL.AmtInvcd,		SOL.ExtAmt, 		SOL.OrigOrdered,	
				SOL.QtyInvcd,		SOL.QtyOnBO,		SOL.QtyOrd,		
				SOL.QtyRtrnCredit,	SOL.QtyRtrnReplacement,	SOL.QtyShip,		
				SOL.TradeDiscAmt,	SOL.UnitPrice,		COALESCE(SO.CurrID, @HomeCurrID)
		FROM	#StgSOLine SOL WITH (NOLOCK)
		JOIN	#StgSalesOrder SO WITH (NOLOCK)
		ON	SOL.TranNo = SO.TranNo
		AND	SO.SessionKey = @iSessionKey
		AND	SOL.SessionKey = @iSessionKey
		OPTION(KEEPFIXED PLAN, KEEP PLAN)

		EXEC spDMValidateQtyAmt @iSessionKey, '#SODetlQtyAmtValData','#StgSOLine',
			        @QtyDecPlaces, @UnitCostDecPlaces, @UnitPriceDecPlaces, @HomeCurrID,
				@DigitsAfterDecimal, @LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @RetVal OUTPUT

		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			RETURN
		END

		-------------------------------------
		-- Start Cursor
		-------------------------------------
		OPEN 	curSalesOrder
	
		FETCH 	curSalesOrder
		INTO 	@RowKey

		SELECT @RowsProcThisCall = 0
		
		WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL)
		BEGIN	
			-- Populate the variable
			SELECT
				@AckDate = AckDate,
				@BillToAddrLine1 = BillToAddrLine1,
				@BillToAddrLine2 = BillToAddrLine2,
				@BillToAddrLine3 = BillToAddrLine3,
				@BillToAddrLine4 = BillToAddrLine4,
				@BillToAddrLine5 = BillToAddrLine5,
				@BillToAddrName = BillToAddrName,
				@BillToCity = BillToCity,
				@BillToCountryID = BillToCountryID,
				@BillToPostalCode = BillToPostalCode,
				@BillToStateID = BillToStateID,
				@BillToCustAddrID = BillToCustAddrID,
				@CloseDate = CloseDate,
				@ContactName = ContactName,
				@ConfirmNo = ConfirmNo,
				@CurrExchRate = CurrExchRate,
				@CurrID = CurrID,
				@CustID = CustID,
				@CustClassID = CustClassID,
				@CustPONo = CustPONo,
				@CurrExchSchdID = CurrExchSchdID,
				@DfltPurchVAddrID = DfltPurchVAddrID,
				@DfltAcctRefCode = DfltAcctRefCode,
				@DfltCommPlanID = DfltCommPlanID,
				@DfltFOBID = DfltFOBID,
				@DfltPromDate = DfltPromDate,
				@DfltRequestDate = DfltRequestDate,
				@DfltShipDate = DfltShipDate,
				@DfltShipMethID = DfltShipMethID,
				@DfltShipPriority = DfltShipPriority,
				@DfltShipToAddrLine1 = DfltShipToAddrLine1,
				@DfltShipToAddrLine2 = DfltShipToAddrLine2,
				@DfltShipToAddrLine3 = DfltShipToAddrLine3,
				@DfltShipToAddrLine4 = DfltShipToAddrLine4,
				@DfltShipToAddrLine5 = DfltShipToAddrLine5,
				@DfltShipToAddrName = DfltShipToAddrName,
				@DfltShipToCity = DfltShipToCity,
				@DfltShipToCountryID = DfltShipToCountryID,
				@DfltShipToPostalCode = DfltShipToPostalCode,
				@DfltShipToStateID = DfltShipToStateID,
				@DfltShipToCustAddrID = DfltShipToCustAddrID,
				@DfltWarehouseID = DfltWarehouseID,
				@Expiration = Expiration,
				@FreightAmt = FreightAmt,
				@Hold = Hold,
				@HoldReason = HoldReason,
				@OpenAmt = OpenAmt,
				@PmtTermsID = PmtTermsID,
				@PrimarySperID = PrimarySperID,
				@RecurSOTranNo = RecurSOTranNo,
				@RequireSOAck = RequireSOAck,
				@SalesSourceID = SalesSourceID,
				@SOAckFormID = COALESCE(SOAckFormID,@DfltSOAckFormID),
				@SOStatus = Status,
				@STaxAmt = STaxAmt,
				@TradeDiscAmt = TradeDiscAmt,
				@TranCmnt = TranCmnt,
				@TranDate = TranDate,
				@TranNo = TranNo,
				@UserFld1 = UserFld1,
				@UserFld2 = UserFld2,
				@UserFld3 = UserFld3,
				@UserFld4 = UserFld4,
				@ProcessStatus = ProcessStatus,
				@Action = Action
			FROM	#StgSalesOrder  WITH (NOLOCK)
			WHERE 	RowKey = @RowKey
			AND 	SessionKey = @iSessionKey

			-- Retrieve the SOKey if the record Action is 'Update'
			IF @Action = @REC_UPDATE
			BEGIN
				SELECT 	@SOKey = so.SOKey,
					@OrgiSOStatus = CONVERT(VARCHAR(12),so.Status),
					@SOStatus = COALESCE(@SOStatus, CONVERT(VARCHAR(12), so.Status))
				FROM	#StgSalesOrder stg WITH (NOLOCK)
				JOIN	tsoSalesOrder so WITH (NOLOCK)
				ON	stg.TranNo = so.TranNo
				AND	so.CompanyID = @iCompanyID
				WHERE 	stg.RowKey = @RowKey
				AND 	stg.SessionKey = @iSessionKey
			END

			----------------------------------------------------------------------------------
			-- General validation of business rules would occur before the insert loop
			----------------------------------------------------------------------------------
			-- Business rule/validation code would go here -----------------------------------
			-- Reset the validation flag
			SELECT @VALIDATION_FAILED = 0
			SELECT @TranNo = COALESCE(@TranNo, '')

			-- Clear out the temporary error log table.
		TRUNCATE TABLE #tdmMigrationLogEntryWrk
			TRUNCATE TABLE #tciMessageBuilder

			-- Check for pre-cursor validation failures.
			IF @ProcessStatus = @GENRL_PROC_ERR
			BEGIN
				SELECT @VALIDATION_FAILED = 1
			END

			IF @Action <> @REC_DELETE           -- Don't need to validate fields for Deletions
			BEGIN
				-- Validate a SO can not be updated if TrackChngOrders is ON.
				IF @TrackChngOrders = 1 AND @Action = @REC_UPDATE
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
			  		 OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))				
					BEGIN
						EXEC ciBuildString @RPT_STRINGNO_SOChngOrdOnNoUpd,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'Action', @iColumnValue = 'Update',
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END	
				END

				-- Validate a Sales order need to be exist for Update.
				IF @SOKey IS NULL AND @Action = @REC_UPDATE
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
			  		 OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))				
					BEGIN
						EXEC ciBuildString @RPT_STRINGNO_X_IS_INVALID,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'TranNo', @iColumnValue = @TranNo,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END	
				END

				--Validate a Tran No.
				IF LTRIM(RTRIM(@TranNo)) = '' AND @Action = @REC_INSERT
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1

					-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL
					OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
					OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))
					BEGIN
						EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'TranNo'

						EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
						@iEntityID = @TranNo, @iColumnID = 'TranNo', @iColumnValue = @TranNo,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END
				END

				-- Validate CustID must exist
				IF @CustID IS NULL AND @Action <> @REC_UPDATE
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
			  		 OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))				
					BEGIN
						EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'CustID'
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'CustID', @iColumnValue = @CustID,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END	
				END

				-- Validate CustID
				IF @CustID IS NULL AND @Action <> @REC_UPDATE
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
			  		 OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))				
					BEGIN
						EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'CustID'
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'CustID', @iColumnValue = @CustID,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END	
				END
	
	 			/* Get Data from tarCustomer ****************************** */
				SELECT @CustKey = CustKey
				FROM tarCustomer WITH (NOLOCK)
				WHERE CustID = @CustID
				AND CompanyID = @iCompanyID
	
				SELECT 	@DfltWhseKey = WhseKey
				FROM 	tarCustAddr WITH (NOLOCK)
				WHERE	CustKey = @CustKey
				AND	CustAddrID = @CustID
	
				IF @DfltWhseKey IS NULL
					SELECT @DfltWhseKey = DfltWhseKey
				FROM 	tsoOptions WITH (NOLOCK)
				WHERE	CompanyID = @iCompanyID
	
				IF @CustID IS NOT NULL AND @CustKey IS NULL
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1

					SELECT @CustID = COALESCE(@CustID,'')
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND @VALIDATION_FAILED = 1)
			  		 OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))				
					BEGIN
						EXEC spCIGetMenuTitle @TASKID_CUSTOMER, @TaskString OUTPUT
						EXEC ciBuildString @RPT_STRINGNO_SEEFORVALID,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'Value', @TaskString
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'CustID', @iColumnValue = @CustID,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END	
				END
	
				--Validate @Hold
				IF ((@Hold IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@Hold))),'') NOT IN ('0', '1')))
					OR ((@Hold IS NULL) AND @Action <> @REC_UPDATE)
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL)
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
					BEGIN
						IF  @Hold IS NULL
							EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'Hold'
						ELSE
							EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'Hold'
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'Hold', @iColumnValue = @Hold,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END
	
					-- Assign value to avoid additional validation in API when the status value is valid
					SELECT @lHold = 0
				END
				ELSE	
					SELECT @lHold = CONVERT(SMALLINT, LTRIM(RTRIM(@Hold)))
	
				-- Validate @RequireSOAck
				IF ((@RequireSOAck IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@RequireSOAck))),'') NOT IN ('0', '1')))
					OR (@RequireSOAck IS NULL  AND @Action <> @REC_UPDATE)
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL)
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
					BEGIN
						IF  @RequireSOAck IS NULL
							EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'RequireSOAck'
						ELSE
							EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'RequireSOAck'
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'RequireSOAck', @iColumnValue = @RequireSOAck,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END
	
					-- Assign value to avoid additional validation in API when the status value is valid
					SELECT @lRequireSOAck = 0
				END
				ELSE	
					SELECT @lRequireSOAck = CONVERT(SMALLINT, LTRIM(RTRIM(@RequireSOAck)))
	
				-- Validate SO Status
				IF ((@SOStatus IS NOT NULL)
					AND (COALESCE(UPPER(LTRIM(RTRIM(@SOStatus))),'') NOT
						IN (@SOStatus_NOTACT, @SOStatus_OPEN, @SOStatus_CANCELED, @SOStatus_CLOSED)))
					OR (@SOStatus IS NULL AND @Action <> @REC_UPDATE)
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL)
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
					BEGIN
						IF  @SOStatus IS NULL
							EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'SO Status'
						ELSE
							EXEC ciBuildString @RPT_STRINGNO_AS_LIST,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, ': Open[1], Closed[4]'
				
						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'SO Status', @iColumnValue = @SOStatus,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END
	
					-- Assign value to avoid additional validation in API when the status value is valid
					SELECT @lSOStatus = 4,	@CloseDate = @TranDate, @AckDate = @TranDate
				END
				ELSE	
					SELECT @lSOStatus = CONVERT(SMALLINT, LTRIM(RTRIM(@SOStatus)))

				-- Validate SO Status against Setup Step
				IF ((UPPER(LTRIM(RTRIM(@SOStatus))) = @SOStatus_OPEN
					AND	@SetupStepKey = @SOInsertion_CLOSED))
					OR	((UPPER(LTRIM(RTRIM(@SOStatus))) = @SOStatus_CLOSED
					AND	@SetupStepKey = @SOInsertion_OPEN))
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL)
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
					BEGIN
			
						IF UPPER(LTRIM(RTRIM(@SOStatus))) = @SOStatus_OPEN
						BEGIN
							EXEC ciBuildString @RPT_STRINGNO_AS_LIST,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, ': Closed[4]'
							SELECT @Status_Str = @Str_OPEN
						END
						ELSE
						BEGIN	
							EXEC ciBuildString @RPT_STRINGNO_AS_LIST,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, ': Open[1]'
							SELECT @Status_Str = @Str_CLOSED
						END
							

						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'SO Status', @iColumnValue = @Status_Str,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END
	
					-- Assign value to avoid additional validation in API when the status value is valid
					SELECT @lSOStatus = 4,	@CloseDate = @TranDate, @AckDate = @TranDate
				END

				-- Only 'Unacknowleged'[0] and 'Open' [1]Order can be updated
				IF @OrgiSOStatus NOT IN (@SOStatus_NOTACT, @SOStatus_OPEN) AND @Action = @REC_UPDATE
					AND @SOStatus <> @SOStatus_CANCELED
				BEGIN
					-- Raise flag to skip insert into final table.
					SELECT @VALIDATION_FAILED = 1
	
	 				-- Log error into report log
					IF (@iRptOption = @RPT_PRINT_ALL)
			   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
					BEGIN
						EXEC ciBuildString @RPT_STRINGNO_CannotUpdCloseOrd,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
						
						SELECt @lColumnValue = @OrgiSOStatus + '|' + 'Update'

						EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
						@iEntityID = @TranNo, @iColumnID = 'Status|Action', @iColumnValue = @lColumnValue,
						@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
						@oRetVal = @RetVal OUTPUT
					END
				END

				-- Only Sales Order with no activity can be canceled.
				IF @SOStatus = @SOStatus_CANCELED AND @Action = @REC_UPDATE
				BEGIN

					-- Check SO activity
					SELECT	@lActivity = 0
					EXEC spsoSOActivity @SOKey, @lActivity OUTPUT

					IF @lActivity > 0
					BEGIN
						-- Raise flag to skip insert into final table.
						SELECT @VALIDATION_FAILED = 1
		
		 				-- Log error into report log
						IF (@iRptOption = @RPT_PRINT_ALL)
				   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
						BEGIN
							EXEC ciBuildString @RPT_STRINGNO_CannotCancelwActivity,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
							
							SELECt @lColumnValue = 'Canceled [' + @SOStatus + ']|' + 'Update'
	
							EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
							@iEntityID = @TranNo, @iColumnID = 'Status|Action', @iColumnValue = @lColumnValue,
							@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
							@oRetVal = @RetVal OUTPUT
						END
					END
				END

				----------------------------------------------------------------		
				-- Calling SO APIs
				----------------------------------------------------------------
				/* Sales Order Options */
				SELECT @RetVal = 0
				EXEC spsoGetSOAPIOptions
					@iCompanyID,  	1,		@RetVal OUTPUT,	@lSpid  OUTPUT,
					@Migration,	@iRptOption,	@iPrintWarnings

				IF @@ERROR <> 0
					GOTO CloseAndDeallocateCursors
		
				-- Evaluate Return
				  IF @RetVal > 2
				    BEGIN
					SELECT @VALIDATION_FAILED = 1
				    END
	
				/* Sales Order Header */	
				SELECT @RetVal = 0		
				EXEC spsoGetSalesOrdDflts
					@iCompanyID,
					@CustKey,
			 		@CustID,
		     			@TranTypeSO,
					@lSOStatus,
					@AckDate,
		     			NULL, /*@_iBillToCustAddrKey*/
					@BillToCustAddrID,
		     			NULL, /*@_iBillToCopyKey */
					@BillToCustAddrID, /*@_iBillToCopyID*/
		    			@BillToAddrName,
		  			@BillToAddrLine1,
		     			@BillToAddrLine2,
		 			@BillToAddrLine3,
					@BillToAddrLine4,
		 			@BillToAddrLine5,
		     			@BillToCity,		
			 		@BillToStateID,
					@BillToCountryID,
					@BillToPostalCode,
					0, /*@_iBTTransactionOverride*/
					0, /*@_iBlnktRelNo*/
					NULL, /*@_iBlnktSOKey */
					NULL, /*@_iBlnktSOID*/	
					NULL, /*@_iChngOrdDate*/
					0, /*@_iChngOrdNo */		
					NULL, /*@_iChngReason */
					NULL, /*@_iChngUserID*/		
					@CloseDate,
					NULL, /*@_iCntctKey*/	
					@ContactName,
					@CurrExchRate,
					NULL, /*@_iCurrExchSchdKey*/
					@CurrExchSchdID, /*@_iCurrExchSchdID */
					@CurrID, 			
					NULL, /*@_iCustClassKey*/
					@CustClassID, 			
					@CustPONo,
					NULL, /*@_iCustQuoteKey */
					NULL, /*@_iCustQuoteID */
					NULL, /*@_iDfltAcctRefKey*/	
					@DfltAcctRefCode,
					NULL, /*@_iDfltCommPlanKey*/
					@DfltCommPlanID,
					NULL, /*@_iDfltCreatePO*/
					NULL, /*@_iDfltDeliveryMeth*/
					NULL, /*@_iDfltFOBKey */
					@DfltFOBID, 	
					@DfltPromDate,
					@DfltRequestDate,	
					@DfltShipDate,
					@DfltShipPriority,
					NULL, /*@_iDfltShipMethKey*/
					@DfltShipMethID, 	
					NULL, /*@_iDSTCustAddrKey */
					@DfltShipToCustAddrID,	
					NULL, /*@_iDSTCopyKey*/
					@DfltShipToCustAddrID, /*@_iDSTCopyID*/
					@DfltShipToAddrName,
					@DfltShipToAddrLine1,
					@DfltShipToAddrLine2,
					@DfltShipToAddrLine3,
		 			@DfltShipToAddrLine4,
					@DfltShipToAddrLine5,
		 			@DfltShipToCity,
					@DfltShipToStateID,
					@DfltShipToCountryID,
					@DfltShipToPostalCode,
		 			0, /*@_iDSTTransactionOverride*/	
					NULL, /*@_iDfltShipZoneKey */
					NULL, /*@_iDfltShipZoneID */
					NULL, /*@_iDfltVendKey */
					NULL, /*@_iDfltVendID  */
					NULL, /*@_iDfltPurchVAddrKey */
					@DfltPurchVAddrID, /*@_iDfltPurchVAddrID */
					NULL, /*@_iDfltWhseKey    */
					@DfltWarehouseID,
		 			@Expiration,
					NULL, /*@_iFreightMethod*/
					@lHold,		
					@HoldReason,
					NULL, /*@_iImportLogKey*/
					NULL, /*@_iPmtTermsKey*/
					@PmtTermsID,	
					NULL, /*@_iRequireSOAck*/
					NULL, /*@_iPrimarySperKey*/
					@PrimarySperID,	
					NULL, /*@_iRecurSOKey*/		
					NULL, /*@_iRecurSOID */
					NULL, /*@_iSOAckFormKey*/
					@SOAckFormID,	
					NULL, /*@_iSalesSourceKey*/
					@SalesSourceID,
					@TranCmnt,
					@TranDate,
					@TranNo,
					@UserFld1,		
					@UserFld2,
					@UserFld3,		
					@UserFld4,
					@TranNo, /*@UniqueID*/
					@UseCustomerDefaultIfBlank, /*@DefaultIfNull*/
					@UserID,
					NULL, /*@_iConfirmNo*/			
					@SOKey OUTPUT,
					@TranID OUTPUT,
					@RetVal OUTPUT,
					@Migration,
					@iRptOption,	
					@iPrintWarnings,
					@BlankInvalidReference,
					@InvalidGLUseSuspense,
					@Action

				IF @@ERROR <> 0
					GOTO CloseAndDeallocateCursors

				-- Evaluate Return
				  IF @RetVal > 2
				    BEGIN
					SELECT @VALIDATION_FAILED = 1
				    END
	
				/* Loop through Detail Records for a New Sales Order*/
				-- Reset all the variables
				SELECT @LineRowKey = 0
				SELECT @CHILD_VALIDATION_FAILED = 0

				-- Get the Current SOLineNo Used
				IF @Action = @REC_UPDATE AND @SOKey IS NOT NULL
					SELECT 	@SOLineNo = COALESCE(MAX(SOLineNo),0)
					FROM	tsoSOLine WITH (NOLOCK)
					WHERE 	SOKey = @SOKey
				ELSE
					SELECT @SOLineNo = 0

				WHILE @VALIDATION_FAILED = 0
				BEGIN
					TRUNCATE TABLE #tdmMigrationLogEntryWrk
					TRUNCATE TABLE #tciMessageBuilder
					SELECT @RetVal = 0
	
				      	-- Position to Next SO detail
				      	SELECT 	@LineRowKey = MIN(#StgSOLine.RowKey)				
				        FROM 	#StgSOLine WITH (NOLOCK)
				       	WHERE 	TranNo = @TranNo
				        AND 	RowKey > @LineRowKey
					AND	SessionKey = @iSessionKey
					AND	COALESCE(KitComponent,'0') <> '1'
	
				     	-- Done with detail for this Sales Order
				      	IF @LineRowKey IS NULL
				        	BREAK
	
				     	-- Get SOline detail values from staging table		
					-- Assign SO Line No
					SELECT @SOLineNo = @SOLineNo + 1	
	
					SELECT @lEntityID = LTRIM(RTRIM(COALESCE(@TranNo, ''))) + '|Line ' + CONVERT(VARCHAR(5),@SOLineNo)
						
					SELECT
					@AcctRefCode = AcctRefCode,										--AcctRefCode from the Staging table is intialized.
					@AmtInvcd = AmtInvcd,			@LineCloseDate = CloseDate,
					@CmntOnly = CmntOnly,			@CommClassID = CommClassID,
					@CommPlanID = CommPlanID,		@Description = Description,		
					@ExtAmt = ExtAmt,			@ExtCmnt = ExtCmnt,	
					@LineFreightAmt = FreightAmt,		@FOBID	 = FOBID,			
					@GLAcctNo = GLAcctNo,			@Hold = Hold,
					@HoldReason = HoldReason,		@ItemAliasID = ItemAliasID,
					@ItemID = ItemID,			@OrigOrdered = OrigOrdered,
					@OrigPromiseDate = OrigPromiseDate,	@OrigSOLineNo = SOLineNo,
					@PromiseDate = PromiseDate,		@QtyInvcd = QtyInvcd,			
					@QtyOnBO = QtyOnBO,			@QtyOpenToShip = COALESCE(QtyOrd,0) - COALESCE(QtyShip,0) ,	
					@QtyOrd = QtyOrd,			@QtyRtrnCredit = QtyRtrnCredit,		
					@QtyRtrnReplacement = QtyRtrnReplacement,@QtyShip = QtyShip,			
					@ReqCert = ReqCert,			@RequestDate = RequestDate,	
					@ShipDate = ShipDate,			@ShipMethID = ShipMethID,	
					@ShipPriority = ShipPriority,		@ShipToAddrLine1 = ShipToAddrLine1,
					@ShipToAddrLine2 = ShipToAddrLine2,	@ShipToAddrLine3 = ShipToAddrLine3,
					@ShipToAddrLine4 = ShipToAddrLine4,	@ShipToAddrLine5 = ShipToAddrLine5,
					@ShipToAddrName = ShipToAddrName,	@ShipToCity = ShipToCity,
					@ShipToCountryID = ShipToCountryID,	@ShipToPostalCode = ShipToPostalCode,
					@ShipToStateID = ShipToStateID,		@ShipToCustAddrID = @DfltShipToCustAddrID,
					@LineStatus = Status,			@STaxClassID = STaxClassID,
					@LineTradeDiscAmt = TradeDiscAmt,	@LineTradeDiscPct = TradeDiscPct,
					@TranNo = TranNo,			@UnitMeasID = UnitMeasID,		
					@UnitPrice = UnitPrice,			@LineUserFld1 = UserFld1,
					@LineUserFld2 = UserFld2,		@WarehouseID = WarehouseID,
					@ProcessStatus = ProcessStatus,		@DeliveryMeth = DeliveryMeth,
					@VendorID = VendorID,			@PONumber = PONumber,
					@DetailAction = Action,			@KitComponent = KitComponent
					FROM #StgSOLine  WITH (NOLOCK)
					WHERE RowKey = @LineRowKey
					AND SessionKey = @iSessionKey

					-- Check for pre-cursor validation failures.
					IF @ProcessStatus = @GENRL_PROC_ERR
					BEGIN
						SELECT @CHILD_VALIDATION_FAILED = 1
					END

					IF @DetailAction <> @REC_DELETE
					BEGIN
		
						--Validate the Yes/No field in SOLine
						IF ((@CmntOnly IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@CmntOnly))),'') NOT IN ('0', '1')))
							OR (@CmntOnly IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @CmntOnly IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'CmntOnly'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'CmntOnly'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'CmntOnly', @iColumnValue = @CmntOnly,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
							SELECT @lCmntOnly = 0
						END
						ELSE	
							SELECT @lCmntOnly = CONVERT(SMALLINT, LTRIM(RTRIM(@CmntOnly)))
		
						--@DeliveryMeth
						IF ((@DeliveryMeth IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@DeliveryMeth))),'') NOT IN ('1', '2', '3', '4')))
							OR (@DeliveryMeth IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @DeliveryMeth IS NULL
								BEGIN
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'DeliveryMeth'
								END
								ELSE
								BEGIN	
									EXEC spDMListValues 'tsoSOLineDist', 'DeliveryMeth', 2, @lListValues OUTPUT, @RetVal		
									EXEC ciBuildString @RPT_STRINGNO_AS_LIST,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, @lListValues
								END
					
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'DeliveryMeth', @iColumnValue = @DeliveryMeth,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
							SELECT @lDeliveryMeth = 0
						END
						ELSE	
							SELECT @lDeliveryMeth = CONVERT(SMALLINT, LTRIM(RTRIM(@DeliveryMeth)))
		
						--@ReqCert
						IF ((@ReqCert IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@ReqCert))),'') NOT IN ('0', '1')))
							OR (@ReqCert IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @ReqCert IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'ReqCert'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'ReqCert'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'ReqCert', @iColumnValue = @ReqCert,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
							SELECT @lReqCert = 0
						END
						ELSE	
							SELECT @lReqCert = CONVERT(SMALLINT, LTRIM(RTRIM(@ReqCert)))
			
						--@Hold
						IF ((@Hold IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@Hold))),'') NOT IN ('0', '1')))
							OR (@Hold IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @Hold IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'Hold'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'Hold'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'Hold', @iColumnValue = @Hold,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
		
							SELECT @lHold = 0
						END
						ELSE	
							SELECT @lHold = CONVERT(SMALLINT, LTRIM(RTRIM(@Hold)))
			
						--@KitComponent

						IF ((@KitComponent IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@KitComponent))),'') NOT IN ('0', '1')))
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @KitComponent IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'KitComponent'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'KitComponent'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'KitComponent', @iColumnValue = @KitComponent,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
						END

						-- Validate SO Line Status
						IF ((@LineStatus IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@LineStatus))),'') NOT IN ('1', '2')))
							OR (@LineStatus IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @LineStatus IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'SO Status'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_AS_LIST,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, ': Open[1], Closed[2]'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'SO Line Status', @iColumnValue = @LineStatus,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
			
							-- Assign value to avoid additional validation in API when the status value is valid
							SELECT @lLineStatus = 2,	@LineCloseDate = @TranDate
						END
						ELSE	

							SELECT @lLineStatus = CONVERT(SMALLINT, LTRIM(RTRIM(@LineStatus)))
		
						SELECT @RetVal = 0	
						/*SO Line Validation*/
						EXEC spsoGetSOItem
							@SOKey,
							@LineCloseDate,		
							NULL, /*@_iCommClassKey*/
			     				@CommClassID,
							NULL, /*@_iCommPlanKey*/
							@CommPlanID,
							@lCmntOnly,
							NULL, /*@_iCustQuoteLineKey*/
							@Description,
							@ExtCmnt,
							NULL,/*@lInclOnPackList,*/
							1, /*@lInclOnPickList,*/
							NULL, /*@_iItemKey*/
							@ItemID,
							NULL, /*@_iOrigItemKey*/
							NULL, /*@_iOrigItemID*/
							@lReqCert,
			    				@SOLineNo,
							NULL,/*@_iSTaxClassKey*/
							@STaxClassID,
							@lLineStatus,		
							0, /*@_iSystemPriceDetermination*/
							NULL, /*@_iSalesPromotionID*/
							NULL, /*@_iSalesPromotionKey*/
							0, /*@_iUnitPriceFromSalesPromo*/
							0, /*@_iUnitPriceFromSchedule*/
							NULL,/*@_iUnitMeasKey*/
							@UnitMeasID,
							@UnitPrice,
							@LineUserFld1,
							@LineUserFld2,
							@UseItemDefaultIfBlank, /*@_iDefaultIfNull*/
							@LanguageID,
							@SOLineKey OUTPUT,
							@RetVal OUTPUT,
							@Migration,
							@iRptOption,	
							@iPrintWarnings,
							@BlankInvalidReference,
							@InvalidGLUseSuspense,
							1 /*@lCustomizedKit*/

						IF @@ERROR <> 0
							GOTO CloseAndDeallocateCursors
		
					      	-- Evaluate Return
					      	IF @RetVal IN (0, 3, 4)
					        BEGIN
					          	SELECT @CHILD_VALIDATION_FAILED = 1
					        END
		
						IF @lCmntOnly <> 1
						-- Do not create SO line dist record for Comment only item
						BEGIN
							/* SO Lines Validation*/
							SELECT @RetVal = 0
							SELECT @LineTradeDiscPct = @LineTradeDiscPct/100
		
			      				EXEC spsoGetSOLineDist --api
								@SOKey,
								@SOLineKey,
								NULL, /*@_iAcctRefKey*/
								@AcctRefCode, /*@_iAcctRefID*/
								@AmtInvcd,
								NULL, /*@_iBlnktLineDistKey*/
								NULL, /*@_iRecurLineDistKey*/
								0, /*@_iCreatePO*/
								@lDeliveryMeth, /*@_iDeliveryMeth*/
								NULL, /*@_iOrigWhseKey*/
								NULL, /*@_iOrigWhseID*/
								NULL, /*@_iVendKey*/
								@VendorID, /*@_iVendID*/
								NULL, /*@_iPurchVendAddrKey*/
								NULL,/*@_iPurchVendAddrID*/
								@ExtAmt,
								NULL,/*@_iShipMethKey*/
								@ShipMethID,	
								NULL, /*@_iShipZoneKey*/
								NULL,/*@_iShipZoneID*/
							 	NULL, /*@_iFOBKey*/
								@FOBID,
						 		@LineFreightAmt,
								@ShipPriority,
								NULL, /*@_iGLAcctKey*/
								@GLAcctNo,
								@lHold,
								@HoldReason,
								@QtyOrd, /*@OrigQtyOrd*/
								@OrigPromiseDate,
								@PromiseDate,
								@RequestDate,
								@ShipDate,
								@QtyInvcd,
								@QtyOnBO,
								@QtyOpenToShip,
								@QtyOrd,
								@QtyShip,
								@QtyRtrnCredit,
								@QtyRtrnReplacement,
								NULL, /*@_iSTaxSchdKey*/
								NULL, /*@STaxSchdID*/
								NULL,/*@_iCopySTaxTranKey*/
								@lLineStatus,
								NULL, /*@_iShipToAddrKey*/
								@ShipToCustAddrID,/*@_iShipToAddrID*/
								@ShipToAddrName,
								NULL, /*@_iShipToCustAddrKey*/
								@ShipToCustAddrID,
								@ShipToAddrLine1,
								@ShipToAddrLine2,
								@ShipToAddrLine3,
								@ShipToAddrLine4,
								@ShipToAddrLine5,
								@ShipToCity,
								@ShipToStateID,
							    	@ShipToCountryID,
								@ShipToPostalCode,
								0, /*@_iShipToTranOvrd*/
								0, /*@_iShipToCustAddrUpdCtr*/
								@LineTradeDiscAmt,
								@LineTradeDiscPct,
								NULL,/*@_iWhseKey*/
								@WarehouseID,
								@UseCustomerDefaultIfBlank, /*@_iDefaultIfNull*/	
								1, /*@_iPerformOvrd*/
								@SOLineDistKey OUTPUT,
								@RetVal OUTPUT,
								@PONumber,
								@Migration,
								@iRptOption,	
								@iPrintWarnings,
								@BlankInvalidReference,
								@InvalidGLUseSuspense

							IF @@ERROR <> 0
								GOTO CloseAndDeallocateCursors
			
						      	-- Evaluate Return
						      	IF @RetVal IN (0, 3, 4)
						        BEGIN
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
		
					      	/* Kit Routine **************************************** */
						--Validation BTO components if any.  If the BTO components are not passed		
						--in, create the components list based on the Standard Kit definition.
						--If the Item is a AssemblyKit Item, warning the user all the components
						--lines will be ignored during insertion
						SELECT @RetVal = 0
		
						SELECT 	@KitItemKey = SOL.ItemKey,
							@KitWhseKey = SOLD.WhseKey,
							@KitQty = SOLD.QtyOrd
						FROM 	#tsoSOLine SOL WITH (NOLOCK)
						JOIN	#tsoSOLineDist SOLD WITH (NOLOCK)
						ON	SOL.SOLineKey = SOLD.SOLineKey
						WHERE 	SOL.SOLineKey = @SOLineKey
		
						SELECT 	@lItemType = NULL
		
						SELECT 	@lItemType = ItemType
						FROM	timItem WITH (NOLOCK)
						WHERE	ItemKey = @KitItemKey
		
						-- If the Item is a BTOKit Item, varified the BTO Kit and it's
						-- Components
						IF @lItemType = @ItemType_BTOKit
						BEGIN
							DELETE #KitCompListVal
							INSERT INTO #KitCompListVal
								(RowKey, SessionKey, CompItemID, OrigCompItemQty, UOMID)
							SELECT 	RowKey, @iSessionKey, ItemID, QtyOrd, UnitMeasID
							FROM	#StgSOLine  WITH (NOLOCK)
							WHERE 	SOLineNo = @OrigSOLineNo
							AND	KitComponent = '1'	
							AND	SessionKey = @iSessionKey
							AND	TranNo = @TranNo
										
			      				EXEC spsoValidKitComponent
							@KitItemKey,		@KitQty,		@WarehouseID,
							@KitWhseKey,		@SOLineKey,		@iCompanyID,		
							@LanguageID,		@lEntityID,		@lSpid,			
							@RetVal OUTPUT,		
							@Migration,		@iRptOption,		@iPrintWarnings
		

							IF @@ERROR <> 0
								GOTO CloseAndDeallocateCursors

						      	IF @RetVal IN (0, 3)
						        BEGIN
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
		
						-- If the Item is a AssemblyKit Item
						IF @lItemType = @ItemType_AssemblyKit
						BEGIN
		
							DELETE #KitCompListVal
							INSERT INTO #KitCompListVal
								(RowKey, SessionKey, CompItemID, OrigCompItemQty, UOMID)
							SELECT 	RowKey, @iSessionKey, ItemID, QtyOrd, UnitMeasID
							FROM	#StgSOLine  WITH (NOLOCK)
							WHERE 	SOLineNo = @OrigSOLineNo
							AND	KitComponent = '1'	
							AND	SessionKey = @iSessionKey
							AND	TranNo = @TranNo
		
							IF EXISTS (SELECT 1 FROM #KitCompListVal)
							BEGIN
								IF @iRptOption <> @RPT_PRINT_NONE AND @iPrintWarnings = 1
								BEGIN
									EXEC ciBuildString @RPT_STRINGNO_IGNORE_COMP_LINE,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
		
									EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
							     		@iEntityID = @lEntityID, @iColumnID = 'ItemID', @iColumnValue = @ItemID,
							     		@iStatus = @MIG_STAT_WARNING, @iDuplicate = 0, @iComment = @Message,
							     		@oRetVal = @RetVal OUTPUT
								END
							END
		
						      	IF @@ERROR <> 0
						        BEGIN
								SELECT @RetVal = @@ERROR
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
						--End of Kit Validation
			
						SELECT @RetVal = 0
					      	EXEC  spsoKitFromSOLine @SOLineKey,  @SOLineKey, @RetVal OUTPUT
	
						IF @@ERROR <> 0
							GOTO CloseAndDeallocateCursors
	
						 /* Line Amount Validation*/
						SELECT @RetVal = 0
		
						IF @lCmntOnly <> 1
						BEGIN
			      				EXEC spsoLineAmts
			             				@SOKey, @SOLineKey, @RetVal OUTPUT,
								@Migration, @iRptOption, @iPrintWarnings

							IF @@ERROR <> 0
								GOTO CloseAndDeallocateCursors
			
						      	-- Evaluate Return
						      	IF @RetVal IN (0, 3, 4)
						        BEGIN
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
					END        -- IF @DetailAction <> @REC_DELETE
---------------------------
--FOR FUTURE USE
---------------------------
-- 					ELSE
-- 					BEGIN      -- Delete the Sales Order Line BEGIN
-- 		                		IF NOT EXISTS (SELECT 1 FROM tsoSOLine
-- 		                                WHERE SOKey = @SOKey
-- 		                                  AND SOLineNo = @SOLineNo)
-- 						BEGIN
-- 			                   		SELECT   @CHILD_VALIDATION_FAILED = 1
-- 			                            			,@Duplicate = 0
-- 			    						,@Status = @MIG_STAT_FAILURE
-- 			    						,@StringNo = @RPT_STRINGNO_DELETE_FAILED
-- 						END
-- 						ELSE
-- 						BEGIN
-- 							DELETE tsoSOLine
-- 							WHERE SOKey = @SOKey
-- 							AND SOLineNo = @SOLineNo
-- 							
-- 							IF @@ERROR <> 0 	-- an error occured
-- 							BEGIN
-- 								SELECT @CHILD_VALIDATION_FAILED = 1, @Status = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
-- 							END
-- 							ELSE			-- an error did not occur
-- 							BEGIN
-- 								SELECT @Status = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
-- 							END
-- 						END
-- 					END	
---------------------------
--FOR FUTURE USE
---------------------------
				END        -- Delete the Sales Order Line END
				--             WHILE 1 = 1

				/* Allocate Freight if necesary*/
				IF (@VALIDATION_FAILED = 0) AND (@CHILD_VALIDATION_FAILED = 0)
				BEGIN
					SELECT @RetVal = 0
		
					EXEC spsoAllocateFrght
						@SOKey, @FreightAmt, @RetVal OUTPUT,
						@AllocateMeth_AMT, @Migration, @iRptOption

					IF @@ERROR <> 0
						GOTO CloseAndDeallocateCursors
		
				      	-- Evaluate Return
				      	IF @RetVal IN (0, 3)
				        BEGIN
				          	SELECT @VALIDATION_FAILED = 1
				        END
				END
	
				----------------------------------------------------------------------------------
				-- Create SO
				----------------------------------------------------------------------------------		
				IF (@VALIDATION_FAILED = 1) OR (@CHILD_VALIDATION_FAILED = 1)
					IF @CHILD_VALIDATION_FAILED = 1
						SELECT @Duplicate = 0, @Status = @MIG_STAT_FAILURE, @StringNo = 430046
					ELSE
						SELECT @Duplicate = 0, @Status = @MIG_STAT_FAILURE
	
				ELSE
				BEGIN
					-- SO creation
					SELECT @RetVal = 0
					EXEC spsoCreateSalesOrder
					     @SOKey,		NULL, /*SalesAmt*/	@FreightAmt,
					     @STaxAmt,		NULL, /*TranAmt*/	@OpenAmt,
					     NULL,/*@AmtInvcd,*/@TradeDiscAmt,		NULL, /*@CrHold*/
					     0, /*@_iUpdateRel*/0, /*@_iFixedRate*/	@RetVal  OUTPUT,
					     @Migration,	@iRptOption,		@iPrintWarnings

					IF @@ERROR <> 0
						GOTO CloseAndDeallocateCursors

					IF @RetVal IN (0,3,4) 	-- an error occured
					BEGIN
						SELECT @Status = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
					END
					ELSE			-- an error did not occur
					BEGIN
						SELECT @Status = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
					END
				END

				-- Log the Errors to tdmMigrationLogWrk
				INSERT INTO #tciMessageBuilder (
					KeyValue,		ColumnID,		ColumnValue,		
					SessionKey,		Duplicate,		EntityID,		
					Status,			
					StringNo,		StringData0,		StringData1)
				SELECT
					EntryNo,		StringData2,		StringData3,
					@iSessionKey,		0,			StringData1,		
					CASE WHEN Severity = 2 	THEN @MIG_STAT_FAILURE
					     WHEN Severity = 1 	THEN @MIG_STAT_WARNING  END,			
					StringNo,		StringData4,		StringData5
				FROM	tciErrorLog WITH (NOLOCK)
				WHERE	SessionID = @lSpid
				AND	ErrorType = 3
				AND	Severity IN (1, 2)
	
				DELETE 	tciErrorLog
				WHERE  	SessionID = @lSpid
				AND	ErrorType = 3
	
				-- Populate the error log with detail message
				IF (@iRptOption <> @RPT_PRINT_NONE)
				BEGIN
					IF (@iRptOption = @RPT_PRINT_SUCCESSFUL)
						DELETE #tciMessageBuilder
						WHERE Status = @MIG_STAT_FAILURE
						OPTION(KEEPFIXED PLAN, KEEP PLAN)
				
					/* Call the sp to build the error message */
					EXEC spimUpdateErrCmnt @LanguageID, @RetVal OUTPUT
				
					IF @RetVal <> @RET_SUCCESS
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						GOTO CloseAndDeallocateCursors
						RETURN
					END
				
					-- Write out to the final temp table for permenent error log tale	
					INSERT	 #tdmMigrationLogEntryWrk
						(ColumnID, ColumnValue, Duplicate, Comment, EntityID, Status, SessionKey)
					SELECT 	ColumnID, COALESCE(ColumnValue,''), Duplicate, MessageText, EntityID, Status, @iSessionKey
					FROM 	#tciMessageBuilder  WITH (NOLOCK)
					WHERE 	SessionKey = @iSessionKey
					OPTION(KEEPFIXED PLAN, KEEP PLAN)
				END	
	
				----------------------------------------------------------------
				-- Log errors into report log
				----------------------------------------------------------------
	            		IF (@iRptOption = @RPT_PRINT_ALL
	               			OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND (@VALIDATION_FAILED = 1 OR @CHILD_VALIDATION_FAILED = 1))
	              			OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))
	            		BEGIN
	                        	EXEC spdmCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT
	           		END
			END
---------------------------
--FOR FUTURE USE
---------------------------
--  			-- IF @Action <> @REC_DELETE
-- 			ELSE
-- 			BEGIN       -- Delete the Sales Order BEGIN
-- 				IF NOT EXISTS (SELECT 1 FROM tsoSalesOrder
-- 				                WHERE @iCompanyID = CompanyID
-- 				                  AND @TranTypeSO = TranType
-- 				                  AND @TranNo = TranNoRel)
-- 				BEGIN
-- 				    SELECT   @Duplicate = 0
-- 					,@Status = @MIG_STAT_FAILURE
-- 					,@StringNo = @RPT_STRINGNO_DELETE_FAILED
-- 					,@VALIDATION_FAILED = 1
-- 				
-- 				END
-- 				ELSE
-- 				BEGIN
-- 					DELETE FROM tsoSalesOrder
-- 					WHERE @iCompanyID = CompanyID
-- 					AND @TranTypeSO = TranType
-- 					AND @TranNo = TranNoRel
-- 				
-- 				    	IF @@ERROR <> 0 	-- an error occured
-- 					BEGIN
-- 						SELECT @Status = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
-- 					END
-- 					ELSE			-- an error did not occur
-- 					BEGIN
-- 						SELECT @Status = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
-- 					END
-- 				END
--  			END        -- Delete the Sales Order END
---------------------------
--FOR FUTURE USE
---------------------------
			--------------------------------------------
			-- Write Migration Log record if appropriate
			--------------------------------------------
			IF @VALIDATION_FAILED = 0 AND
				((@iRptOption = @RPT_PRINT_ALL) OR
				(@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL AND @Status = @MIG_STAT_FAILURE) OR
				(@iRptOption =  @RPT_PRINT_SUCCESSFUL AND @Status = @MIG_STAT_SUCCESSFUL))
			BEGIN
				-- Default @Message to NULL string				
				SELECT @Message = NULL
						
				-- Lookup the comment if one was specified
				IF @StringNo IS NOT NULL
					BEGIN
						EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
					END
								
				EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
				@iEntityID = @TranNo, @iColumnID = '', @iColumnValue = '',
				@iStatus = @Status, @iDuplicate = @Duplicate,
				@iComment = @Message, @oRetVal = @RetVal OUTPUT
				
				--SELECT @EntryNo = @EntryNo + 1
	
			END /* Reporting was requested */

			IF @Status = @MIG_STAT_FAILURE
				SELECT @oFailedRecs = @oFailedRecs + 1
	
			------------------------------------------------------------------------------------
			-- If using staging tables, then mark failed records with the failed
			-- record processing status and delete successful rows.
			------------------------------------------------------------------------------------
			IF @UseStageTable <> 1
			   BEGIN -- using temp table only
				IF @Status = @MIG_STAT_FAILURE -- did not process
				BEGIN
					UPDATE #StgSalesOrder
					SET ProcessStatus = @GENRL_PROC_ERR
					WHERE RowKey = @RowKey

					UPDATE #StgSOLine
					SET ProcessStatus = @GENRL_PROC_ERR
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	
				END
				ELSE
				BEGIN
					DELETE #StgSalesOrder
					WHERE RowKey = @RowKey

					DELETE #StgSOLine
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	
				END
			   END
			ELSE
			   BEGIN -- using staging table
				IF @Status = @MIG_STAT_FAILURE -- did not process
				BEGIN
					UPDATE StgSalesOrder SET ProcessStatus = @GENRL_PROC_ERR
					FROM StgSalesOrder WITH (NOLOCK)
					WHERE RowKey = @RowKey

					UPDATE StgSOLine
					SET ProcessStatus = @GENRL_PROC_ERR
					FROM StgSOLine a
					JOIN #StgSOLine b
					ON a.RowKey = b.RowKey
					WHERE b.TranNo = @TranNo
					AND b.SessionKey = @iSessionKey	
					AND a.SessionKey = @iSessionKey
				END
				ELSE
				BEGIN
					DELETE StgSalesOrder
					WHERE RowKey = @RowKey

					DELETE StgSOLine
					FROM StgSOLine a
					JOIN #StgSOLine b
					ON a.RowKey = b.RowKey
					WHERE b.TranNo = @TranNo
					AND b.SessionKey = @iSessionKey	
					AND a.SessionKey = @iSessionKey
				END
			   END
						

			SELECT @RowsProcThisCall = @RowsProcThisCall + 1	
	
			-- Keep SP caller informed of how many rows were processed.
			SELECT @oRecsProcessed = @oRecsProcessed + 1
			
			FETCH 	curSalesOrder
			INTO 	@RowKey
			
		END /* WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL) */
	
		IF CURSOR_STATUS ('global', 'curSalesOrder') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
			CLOSE curSalesOrder
	
	END /* WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS */

	-- Tell the caller not to continue calling me if there are no more error free records in the
	-- staging tables.  If temporary tables were passed in, all of their records were processed,
	-- so we will in this case also tell the caller not to continue calling.
	IF @UseStageTable = 1 AND
	(SELECT COUNT(*) FROM StgSalesOrder WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED) > 0
		SELECT @oContinue = 1
	ELSE
		SELECT @oContinue = 0

	IF @oContinue = 0
	BEGIN
		-- Time to Deallocate the Cursor
		IF (CURSOR_STATUS ('global', 'curSalesOrder') = @STATUS_CURSOR_CLOSED)
			DEALLOCATE curSalesOrder
	END
	ELSE
	BEGIN
		SELECT @_oRetVal = @RET_SUCCESS
		RETURN	
	END

	----------------------------------------------------
	-- Start processing SOLine without SO Header Record
	----------------------------------------------------
	-- Initialize the total record count if this is the first call to this sp.
	-- Because this routine will only process @MAX_ROWS_PER_RUN if staging tables
	-- are being used, it may be called more than once.

	-- Create an entry in #StgSalesOrder for each order appears in #StgSOLine
	-- with Action 'Update'
	IF @UseStageTable = 1
	BEGIN
		-- If the caller wants me to use the staging table, then I'll create my own copy
		-- of the temporary tables.
		TRUNCATE TABLE #StgSalesOrder
		
		IF @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
		BEGIN
		   	SET ROWCOUNT @MAX_ROWS_PER_RUN
		END /* End @_lTempTableCreation = @CREATED_OUTSIDE_THIS_SP */
		ELSE
		BEGIN
		 	SET ROWCOUNT 0
		END /* End @_lTempTableCreation <> @CREATED_OUTSIDE_THIS_SP */
	
		INSERT INTO #StgSalesOrder (
			ProcessStatus,	SessionKey,		
			TranNo,		Action)
		SELECT  DISTINCT ProcessStatus,	SessionKey,
			TranNo,		@REC_UPDATE				
		FROM 	StgSOLine WITH (NOLOCK)
		WHERE 	SessionKey = @iSessionKey
		AND 	ProcessStatus = @NOT_PROCESSED
		AND	TranNo IS NOT NULL
		OPTION(KEEP PLAN)

		SET ROWCOUNT 0
	END
	ELSE
	BEGIN
		INSERT INTO #StgSalesOrder (
			ProcessStatus,	SessionKey,		
			TranNo,		Action)
		SELECT  DISTINCT ProcessStatus,	SessionKey,
			TranNo,		@REC_UPDATE				
		FROM 	#StgSOLine WITH (NOLOCK)
		WHERE 	SessionKey = @iSessionKey
		AND 	ProcessStatus = @NOT_PROCESSED
		AND	TranNo IS NOT NULL
		OPTION(KEEP PLAN)		
	END

	IF @oTotalRecs = 0
		SELECT @oTotalRecs = (SELECT COUNT(*) FROM #StgSalesOrder WHERE ProcessStatus = @NOT_PROCESSED)

	SELECT @RowsProcThisCall = 0

	--Create Cursor
	IF CURSOR_STATUS ('global', 'curSOLine') = @STATUS_CURSOR_DOES_NOT_EXIST
	BEGIN		
		DECLARE curSOLine INSENSITIVE CURSOR FOR
		SELECT	a.RowKey
		FROM 	#StgSalesOrder a WITH (NOLOCK)
	END

	--Start Time Loop
	SELECT @StartDate = GetDate()
	WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS OR @UseStageTable = 0
	BEGIN
		IF @UseStageTable = 1
		BEGIN
			-- Get the SO Lines
			TRUNCATE TABLE #StgSOLine
			SET IDENTITY_INSERT #StgSOLine ON
			
			INSERT INTO #StgSOLine (
				RowKey,			ProcessStatus,		SessionKey,
				AcctRefCode,		AmtInvcd,		CloseDate,
				CmntOnly,		CommClassID,		CommPlanID,
				KitComponent,		Description,		ExtAmt,
				ExtCmnt,		FOBID,			FreightAmt,
				GLAcctNo,		Hold,			HoldReason,
				ItemAliasID,		ItemID,			OrigOrdered,
				OrigPromiseDate,	PromiseDate,		QtyInvcd,
				QtyOnBO,		QtyOrd,			QtyRtrnCredit,
				QtyRtrnReplacement,	QtyShip,		ReqCert,
				RequestDate,		ShipDate,		ShipMethID,
				ShipPriority,		ShipToAddrLine1,	ShipToAddrLine2,
				ShipToAddrLine3,	ShipToAddrLine4,	ShipToAddrLine5,
				ShipToAddrName,		ShipToCity,		ShipToCountryID,
				ShipToPostalCode,	ShipToStateID,	
				SOLineNo,		Status,			STaxClassID,
				TradeDiscAmt,		TradeDiscPct,		TranNo,
				UnitMeasID,		UnitPrice,
				UserFld1,		UserFld2,		WarehouseID,
				DeliveryMeth,		VendorID,		PONumber,
				Action)
			SELECT
				RowKey,			ProcessStatus,		SessionKey,
				AcctRefCode,		AmtInvcd,		CloseDate,
				CmntOnly,		CommClassID,		CommPlanID,
				KitComponent,		Description,		ExtAmt,
				ExtCmnt,		FOBID,			FreightAmt,
				GLAcctNo,		Hold,			HoldReason,
				ItemAliasID,		ItemID,			OrigOrdered,
				OrigPromiseDate,	PromiseDate,		QtyInvcd,
				QtyOnBO,		QtyOrd,			QtyRtrnCredit,
				QtyRtrnReplacement,	QtyShip,		ReqCert,
				RequestDate,		ShipDate,		ShipMethID,
				ShipPriority,		ShipToAddrLine1,	ShipToAddrLine2,
				ShipToAddrLine3,	ShipToAddrLine4,	ShipToAddrLine5,
				ShipToAddrName,		ShipToCity,		ShipToCountryID,
				ShipToPostalCode,	ShipToStateID,	
				SOLineNo,		Status,			STaxClassID,
				TradeDiscAmt,		TradeDiscPct,		TranNo,
				UnitMeasID,		UnitPrice,
				UserFld1,		UserFld2,		WarehouseID,
				DeliveryMeth,		VendorID,		PONumber,
				Action
			FROM StgSOLine SOL WITH (NOLOCK)
			WHERE TranNo IN (SELECT TranNo FROM #StgSalesOrder WITH (NOLOCK))
			AND SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
			OPTION(KEEP PLAN)
			
			SET IDENTITY_INSERT #StgSOLine OFF
		END
	
		-- Determine if any remaining rows exist for insertion.  If not, then consider this successful.
		SELECT @SourceRowCount = COUNT(*) FROM #StgSalesOrder  WITH (NOLOCK) WHERE ProcessStatus = @NOT_PROCESSED
	
		-- Exit if there are no rows to migrate.  Consider this success and tell the caller
		-- not to continue calling this SP.
		IF @SourceRowCount = 0
			BEGIN
				SELECT @_oRetVal = @RET_SUCCESS, @oContinue = 0
				BREAK
			END
	
		----------------------------------------------------------------------------
		-- Start of primary logic.
		-- We will process payment terms into tapVendClass row by row.
		-- The only real validation we have to contend with is duplicate values.
		--
		-- Process the rows until we're "done", where done either means we ran out
		-- of rows or we were told to stop.  If the original data was obtained
		-- from staging tables, then we will only process up to the maximum number
		-- of rows that will result in decent response time.  If the data was
		-- passed in the temporary tables directly, then we will process all rows.
		-----------------------------------------------------------------------------
		-- For enumerate fields, convert the staging table's column values
		-- to Enterprise's enumerated values.  If they cannot be enumerated,
		-- leave the value as is so it can be reported.
		EXEC spDMListReplacement '#StgSOLine', 'Status', 'tsoSOLine', 'Status', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END
	
		EXEC spDMListReplacement '#StgSOLine', 'Action', 'StgSOLine', 'Action', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END
	
	
		EXEC spDMListReplacement '#StgSOLine', 'DeliveryMeth', 'tsoSOLineDist', 'DeliveryMeth', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END
	
		-- Update the YesNo fields
		EXEC spDMYesNoCodeUpd '#StgSOLine', 'StgSOLine', @RetVal OUTPUT
		IF (@RetVal <> @RET_SUCCESS) BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		-- Insert default values for the temporary table columns containing null, where the
		-- corresponding permanent table column has a default rule.
		EXEC spDMTmpTblDfltUpd '#StgSOLine', 'tsoSOLine', @RetVal OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END
	
		EXEC spDMTmpTblDfltUpd '#StgSOLine', 'tsoSOLineDist', @RetVal OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END

		--Strip GL Acct Mast
		TRUNCATE TABLE #GLAcctColumns
		
		INSERT #GLAcctColumns SELECT 'GLAcctNo'
	
		EXEC spDMMASGLMaskStrip '#StgSOLine', @RetVal  OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END
	
		EXEC spDMStrMaskStrip '#StgSOLine', 'StgSOLine', @RetVal  OUTPUT
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			GOTO CloseAndDeallocateCursors
			RETURN
		END
	
		-- Validate SO Detail Qyt, Amt columns
	
		TRUNCATE TABLE #tciMessageBuilder
		TRUNCATE TABLE #tdmMigrationLogEntryWrk
	
		INSERT INTO #SODetlQtyAmtValData
		    	(RowKey,		SessionKey,		EntityID,
		     	AmtInvcd,		ExtAmt, 		OrigOrdered,	
			QtyInvcd,		QtyOnBO,		QtyOrd,		
			QtyRtrnCredit,		QtyRtrnReplacement,	QtyShip,		
			TradeDiscAmt,	UnitPrice,	CurrID)
		SELECT  SOL.RowKey,		SOL.SessionKey,		LTRIM(RTRIM(SOL.TranNo)) + '|Item ' + COALESCE(SOL.ItemID, '') + ' Line',
		     	SOL.AmtInvcd,		SOL.ExtAmt, 		SOL.OrigOrdered,	
			SOL.QtyInvcd,		SOL.QtyOnBO,		SOL.QtyOrd,		
			SOL.QtyRtrnCredit,	SOL.QtyRtrnReplacement,	SOL.QtyShip,		
			SOL.TradeDiscAmt,	SOL.UnitPrice,		COALESCE(SO.CurrID, @HomeCurrID)
		FROM	#StgSOLine SOL WITH (NOLOCK)
		JOIN	#StgSalesOrder SO WITH (NOLOCK)
		ON	SOL.TranNo = SO.TranNo
		AND	SO.SessionKey = @iSessionKey
		AND	SOL.SessionKey = @iSessionKey
		OPTION(KEEPFIXED PLAN, KEEP PLAN)
	
		EXEC spDMValidateQtyAmt @iSessionKey, '#SODetlQtyAmtValData','#StgSOLine',
			        @QtyDecPlaces, @UnitCostDecPlaces, @UnitPriceDecPlaces, @HomeCurrID,
				@DigitsAfterDecimal, @LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @RetVal OUTPUT
	
		IF @RetVal <> @RET_SUCCESS
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			RETURN
		END
	
		-------------------------------------
		-- Start Cursor
		-------------------------------------
		OPEN 	curSOLine
	
		FETCH 	curSOLine
		INTO 	@RowKey
	
		SELECT @RowsProcThisCall = 0

		WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL)
		BEGIN	
			SELECT 	@Action = stg.Action,
					@TranNo = stg.TranNo
			FROM	#StgSalesOrder stg WITH (NOLOCK)
			WHERE 	stg.RowKey = @RowKey
			AND 	stg.SessionKey = @iSessionKey
		
			-- Retrieve the SOKey for SO to be updated
			SELECT	@SOKey = NULL,@SOStatus = NULL
			SELECT 	@SOKey = so.SOKey,
					@SOStatus = so.Status
			FROM	tsoSalesOrder so WITH (NOLOCK)
			WHERE	so.TranNo = @TranNo
			AND		so.CompanyID = @iCompanyID

			----------------------------------------------------------------------------------
			-- General validation of business rules would occur before the insert loop
			----------------------------------------------------------------------------------
			-- Business rule/validation code would go here -----------------------------------
			-- Reset the validation flag
			SELECT @VALIDATION_FAILED = 0
	
	   		-- Clear out the temporary error log table.
	        TRUNCATE TABLE #tdmMigrationLogEntryWrk
			TRUNCATE TABLE #tciMessageBuilder
	
			-- Check for pre-cursor validation failures.
			IF @ProcessStatus = @GENRL_PROC_ERR
			BEGIN
				SELECT @VALIDATION_FAILED = 1
			END

			-- Validate SO TranNo
			IF @SOKey IS NULL
			BEGIN
				-- Raise flag to skip insert into final table.
				SELECT @VALIDATION_FAILED = 1,
					 @Status = @MIG_STAT_FAILURE

				-- Log error into report log
				IF (@iRptOption = @RPT_PRINT_ALL)
		   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
				BEGIN
					EXEC ciBuildString @RPT_STRINGNO_X_IS_INVALID,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'TranNo'
			
					EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
					@iEntityID = @TranNo, @iColumnID = 'TranNo', @iColumnValue = @TranNo,
					@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
					@oRetVal = @RetVal OUTPUT
				END
			END


			-- Validate SO Status
			-- Only Open, Acknowledged Order can be updated
			IF (@SOStatus NOT IN (@SOStatus_NOTACT, @SOStatus_OPEN))
				OR (@SOStatus IS NULL)
			BEGIN
				-- Raise flag to skip insert into final table.
				SELECT @VALIDATION_FAILED = 1,
					 @Status = @MIG_STAT_FAILURE

				-- Log error into report log
				IF (@iRptOption = @RPT_PRINT_ALL)
		   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
				BEGIN
					EXEC ciBuildString @RPT_STRINGNO_CannotUpdCloseOrd,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
			
					EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
					@iEntityID = @TranNo, @iColumnID = 'SO Status', @iColumnValue = @SOStatus,
					@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
					@oRetVal = @RetVal OUTPUT
				END
			END

			-- Log the SOLines can not be updated due the error in header record
			IF @VALIDATION_FAILED = 1
			BEGIN
				TRUNCATE TABLE #tdmMigrationLogEntryWrk

				-- Populate the error log with detail message
				IF (@iRptOption = @RPT_PRINT_ALL)
		   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)		
				BEGIN
					EXEC ciBuildString @RPT_STRINGNO_CHILD_FAILURE, @LanguageID, @Message OUTPUT, @RetVal OUTPUT, 'Sales Order'

					INSERT INTO #tdmMigrationLogEntryWrk
							(ColumnID		,ColumnValue	,Comment
							,Duplicate		,EntityID		,SessionKey
							,Status)
					SELECT	'TranNo|LineNo'	,COALESCE(@TranNo, '') + ' | ' + COALESCE(CONVERT(VARCHAR(10), SOLineNo),'')	,@Message
							,0				,COALESCE(@TranNo, '') + ' | ' + COALESCE(CONVERT(VARCHAR(10), SOLineNo),''),@iSessionKey
							,@MIG_STAT_FAILURE
					FROM #StgSOLine
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	
				END	
			END

			IF @VALIDATION_FAILED = 0
			BEGIN
				----------------------------------------------------------------		
				-- Calling SO APIs
				----------------------------------------------------------------
				/* Sales Order Options */
				SELECT @RetVal = 0
				EXEC spsoGetSOAPIOptions
					@iCompanyID,  	1,		@RetVal OUTPUT,	@lSpid  OUTPUT,
					@Migration,	@iRptOption,	@iPrintWarnings
	
	
					IF @@ERROR <> 0
						GOTO CloseAndDeallocateCursors
		
				-- Evaluate Return
				  IF @RetVal > 2
				    BEGIN
					SELECT @VALIDATION_FAILED = 1
				    END
	
				/* Sales Order Header */	
				SELECT @RetVal = 0		
				EXEC spsoGetSalesOrdDflts
					@iCompanyID,
					@CustKey,
			 		@CustID,
		     			@TranTypeSO,
					@lSOStatus,
					@AckDate,
		     			NULL, /*@_iBillToCustAddrKey*/
					@BillToCustAddrID,
		     			NULL, /*@_iBillToCopyKey */
					@BillToCustAddrID, /*@_iBillToCopyID*/
		    			@BillToAddrName,
		  			@BillToAddrLine1,
		     			@BillToAddrLine2,
		 			@BillToAddrLine3,
					@BillToAddrLine4,
		 			@BillToAddrLine5,
		     			@BillToCity,		
			 		@BillToStateID,
					@BillToCountryID,
					@BillToPostalCode,
					0, /*@_iBTTransactionOverride*/
					0, /*@_iBlnktRelNo*/
					NULL, /*@_iBlnktSOKey */
					NULL, /*@_iBlnktSOID*/	
					NULL, /*@_iChngOrdDate*/
					0, /*@_iChngOrdNo */		
					NULL, /*@_iChngReason */
					NULL, /*@_iChngUserID*/		
					@CloseDate,
					NULL, /*@_iCntctKey*/	
					@ContactName,
					@CurrExchRate,
					NULL, /*@_iCurrExchSchdKey*/
					NULL, /*@_iCurrExchSchdID */
					@CurrID, 			
					NULL, /*@_iCustClassKey*/
					@CustClassID, 			
					@CustPONo,
					NULL, /*@_iCustQuoteKey */
					NULL, /*@_iCustQuoteID */
					NULL, /*@_iDfltAcctRefKey*/	
					@DfltAcctRefCode,
					NULL, /*@_iDfltCommPlanKey*/
					@DfltCommPlanID,
					0,    /*@_iDfltCreatePO*/
					NULL, /*@_iDfltDeliveryMeth*/
					NULL, /*@_iDfltFOBKey */
					@DfltFOBID, 	
					@DfltPromDate,
					@DfltRequestDate,	
					@DfltShipDate,
					@DfltShipPriority,
					NULL, /*@_iDfltShipMethKey*/
					@DfltShipMethID, 	
					NULL, /*@_iDSTCustAddrKey */
					@DfltShipToCustAddrID,	
					NULL, /*@_iDSTCopyKey*/
					@DfltShipToCustAddrID, /*@_iDSTCopyID*/
					@DfltShipToAddrName,
					@DfltShipToAddrLine1,
					@DfltShipToAddrLine2,
					@DfltShipToAddrLine3,
		 			@DfltShipToAddrLine4,
					@DfltShipToAddrLine5,
		 			@DfltShipToCity,
					@DfltShipToStateID,
					@DfltShipToCountryID,
					@DfltShipToPostalCode,
		 			0, /*@_iDSTTransactionOverride*/	
					NULL, /*@_iDfltShipZoneKey */
					NULL, /*@_iDfltShipZoneID */
					NULL, /*@_iDfltVendKey */
					NULL, /*@_iDfltVendID  */
					NULL, /*@_iDfltPurchVAddrKey */
					NULL, /*@_iDfltPurchVAddrID */
					NULL, /*@_iDfltWhseKey    */
					@DfltWarehouseID,
		 			@Expiration,
					NULL, /*@_iFreightMethod*/
					@lHold,		
					@HoldReason,
					NULL, /*@_iImportLogKey*/
					NULL, /*@_iPmtTermsKey*/
					@PmtTermsID,	
					NULL, /*@_iRequireSOAck*/
					NULL, /*@_iPrimarySperKey*/
					@PrimarySperID,	
					NULL, /*@_iRecurSOKey*/		
					NULL, /*@_iRecurSOID */
					NULL, /*@_iSOAckFormKey*/
					@SOAckFormID,	
					NULL, /*@_iSalesSourceKey*/
					@SalesSourceID,
					@TranCmnt,
					@TranDate,
					@TranNo,
					@UserFld1,		
					@UserFld2,
					@UserFld3,		
					@UserFld4,
					@TranNo, /*@UniqueID*/
					@UseCustomerDefaultIfBlank, /*@DefaultIfNull*/
					@UserID,
					NULL, /*@_iConfirmNo*/			
					@SOKey OUTPUT,
					@TranID OUTPUT,
					@RetVal OUTPUT,
					@Migration,
					@iRptOption,	
					@iPrintWarnings,
					@BlankInvalidReference,
					@InvalidGLUseSuspense,
					@Action,
					1 -- Skip Sales Order Header validation
	
					IF @@ERROR <> 0
						GOTO CloseAndDeallocateCursors

				-- Evaluate Return
				  IF @RetVal <> 1
				    BEGIN
					SELECT @VALIDATION_FAILED = 1
				    END
	
				/* Loop through Detail Records for a New Sales Order*/
				-- Reset all the variables
				SELECT @LineRowKey = 0
				SELECT @CHILD_VALIDATION_FAILED = 0
	
				IF @Action = @REC_UPDATE AND @SOKey IS NOT NULL
					SELECT 	@SOLineNo = COALESCE(MAX(SOLineNo),0)
					FROM	tsoSOLine WITH (NOLOCK)
					WHERE 	SOKey = @SOKey
				ELSE
					SELECT @SOLineNo = 0

				WHILE @VALIDATION_FAILED = 0 --AND @Action = @REC_INSERT
				BEGIN
					TRUNCATE TABLE #tdmMigrationLogEntryWrk
					TRUNCATE TABLE #tciMessageBuilder
					SELECT @RetVal = 0
	
			      		-- Position to Next SO detail
			      		SELECT 	@LineRowKey = MIN(#StgSOLine.RowKey)				
						FROM 	#StgSOLine WITH (NOLOCK)
			       		WHERE 	TranNo = @TranNo
						AND 	RowKey > @LineRowKey
						AND	SessionKey = @iSessionKey
						AND	COALESCE(KitComponent,'0') <> '1'
	
				     	-- Done with detail for this Sales Order
				      	IF @LineRowKey IS NULL
				        	BREAK
	
				     	-- Get SOline detail values from staging table		
					-- Assign SO Line No
					SELECT @SOLineNo = @SOLineNo + 1	
	
					SELECT @lEntityID = LTRIM(RTRIM(COALESCE(@TranNo, ''))) + '|Line ' + CONVERT(VARCHAR(5),@SOLineNo)
						
					SELECT
					@AmtInvcd = AmtInvcd,			@LineCloseDate = CloseDate,
					@CmntOnly = CmntOnly,			@CommClassID = CommClassID,
					@CommPlanID = CommPlanID,		@Description = Description,		
					@ExtAmt = ExtAmt,			@ExtCmnt = ExtCmnt,	
					@LineFreightAmt = FreightAmt,		@FOBID	 = FOBID,			
					@GLAcctNo = GLAcctNo,			@Hold = Hold,
					@HoldReason = HoldReason,		@ItemAliasID = ItemAliasID,
					@ItemID = ItemID,			@OrigOrdered = OrigOrdered,
					@OrigPromiseDate = OrigPromiseDate,	@OrigSOLineNo = SOLineNo,
					@PromiseDate = PromiseDate,		@QtyInvcd = QtyInvcd,			
					@QtyOnBO = QtyOnBO,			@QtyOpenToShip = COALESCE(QtyOrd,0) - COALESCE(QtyShip,0) ,	
					@QtyOrd = QtyOrd,			@QtyRtrnCredit = QtyRtrnCredit,		
					@QtyRtrnReplacement = QtyRtrnReplacement,@QtyShip = QtyShip,			
					@ReqCert = ReqCert,			@RequestDate = RequestDate,	
					@ShipDate = ShipDate,			@ShipMethID = ShipMethID,	
					@ShipPriority = ShipPriority,		@ShipToAddrLine1 = ShipToAddrLine1,
					@ShipToAddrLine2 = ShipToAddrLine2,	@ShipToAddrLine3 = ShipToAddrLine3,
					@ShipToAddrLine4 = ShipToAddrLine4,	@ShipToAddrLine5 = ShipToAddrLine5,
					@ShipToAddrName = ShipToAddrName,	@ShipToCity = ShipToCity,
					@ShipToCountryID = ShipToCountryID,	@ShipToPostalCode = ShipToPostalCode,
					@ShipToStateID = ShipToStateID,		@ShipToCustAddrID = @DfltShipToCustAddrID,
					@LineStatus = Status,			@STaxClassID = STaxClassID,
					@LineTradeDiscAmt = TradeDiscAmt,	@LineTradeDiscPct = TradeDiscPct,
					@TranNo = TranNo,			@UnitMeasID = UnitMeasID,		
					@UnitPrice = UnitPrice,			@LineUserFld1 = UserFld1,
					@LineUserFld2 = UserFld2,		@WarehouseID = WarehouseID,
					@ProcessStatus = ProcessStatus,		@DeliveryMeth = DeliveryMeth,
					@VendorID = VendorID,			@PONumber = PONumber,
					@DetailAction = Action,			@KitComponent = KitComponent
					FROM #StgSOLine  WITH (NOLOCK)
					WHERE RowKey = @LineRowKey
					AND SessionKey = @iSessionKey
	
					-- Check for pre-cursor validation failures.
					IF @ProcessStatus = @GENRL_PROC_ERR
					BEGIN
						SELECT @CHILD_VALIDATION_FAILED = 1
					END
	
					IF @DetailAction <> @REC_DELETE
					BEGIN
		
						--Validate the Yes/No field in SOLine
						IF ((@CmntOnly IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@CmntOnly))),'') NOT IN ('0', '1')))
							OR (@CmntOnly IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @CmntOnly IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'CmntOnly'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'CmntOnly'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'CmntOnly', @iColumnValue = @CmntOnly,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
							SELECT @lCmntOnly = 0
						END
						ELSE	
							SELECT @lCmntOnly = CONVERT(SMALLINT, LTRIM(RTRIM(@CmntOnly)))
		
						--@DeliveryMeth
						IF ((@DeliveryMeth IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@DeliveryMeth))),'') NOT IN ('1', '2', '3', '4')))
							OR (@DeliveryMeth IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @DeliveryMeth IS NULL
								BEGIN
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'DeliveryMeth'
								END
								ELSE
								BEGIN	
									EXEC spDMListValues 'tsoSOLineDist', 'DeliveryMeth', 2, @lListValues OUTPUT, @RetVal		
									EXEC ciBuildString @RPT_STRINGNO_AS_LIST,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, @lListValues
								END
					
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'DeliveryMeth', @iColumnValue = @DeliveryMeth,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
							SELECT @lDeliveryMeth = 0
						END
						ELSE	
							SELECT @lDeliveryMeth = CONVERT(SMALLINT, LTRIM(RTRIM(@DeliveryMeth)))
		
						--@ReqCert
						IF ((@ReqCert IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@ReqCert))),'') NOT IN ('0', '1')))
							OR (@ReqCert IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @ReqCert IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'ReqCert'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'ReqCert'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'ReqCert', @iColumnValue = @ReqCert,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
							SELECT @lReqCert = 0
						END
						ELSE	
							SELECT @lReqCert = CONVERT(SMALLINT, LTRIM(RTRIM(@ReqCert)))
			
						--@Hold
						IF ((@Hold IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@Hold))),'') NOT IN ('0', '1')))
							OR (@Hold IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @Hold IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'Hold'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'Hold'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'Hold', @iColumnValue = @Hold,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
		
							SELECT @lHold = 0
						END
						ELSE	
							SELECT @lHold = CONVERT(SMALLINT, LTRIM(RTRIM(@Hold)))
			

						--@KitComponent
						IF ((@KitComponent IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@KitComponent))),'') NOT IN ('0', '1')))
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @KitComponent IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'KitComponent'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_INVALID_YESNO,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'KitComponent'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'KitComponent', @iColumnValue = @KitComponent,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
						END

						-- Validate SO Line Status
						IF ((@LineStatus IS NOT NULL) AND (COALESCE(UPPER(LTRIM(RTRIM(@LineStatus))),'') NOT IN ('1', '2')))
							OR (@LineStatus IS NULL)
						BEGIN
							-- Raise flag to skip insert into final table.
							SELECT @CHILD_VALIDATION_FAILED = 1
			
			 				-- Log error into report log
							IF (@iRptOption = @RPT_PRINT_ALL)
					   		OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL)				
							BEGIN
								IF  @LineStatus IS NULL
									EXEC ciBuildString @RPT_STRINGNO_Col_IS_NULL,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, 'SO Status'
								ELSE
									EXEC ciBuildString @RPT_STRINGNO_AS_LIST,@LanguageID,@Message OUTPUT, @RetVal OUTPUT, ': Open[1], Closed[2]'
						
								EXEC spdmCreateMigrationLogEntry	@iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,				
								@iEntityID = @lEntityID, @iColumnID = 'SO Line Status', @iColumnValue = @LineStatus,
								@iStatus = @MIG_STAT_FAILURE, @iDuplicate = 0, @iComment = @Message,
								@oRetVal = @RetVal OUTPUT
							END
			
							-- Assign value to avoid additional validation in API when the status value is valid
							SELECT @lLineStatus = 2,	@LineCloseDate = @TranDate
						END
						ELSE	
	
							SELECT @lLineStatus = CONVERT(SMALLINT, LTRIM(RTRIM(@LineStatus)))
		
						SELECT @RetVal = 0	
						/*SO Line Validation*/
						EXEC spsoGetSOItem
							@SOKey,
							@LineCloseDate,		
							NULL, /*@_iCommClassKey*/
			     				@CommClassID,
							NULL, /*@_iCommPlanKey*/
							@CommPlanID,
							@lCmntOnly,
							NULL, /*@_iCustQuoteLineKey*/
							@Description,
							@ExtCmnt,
							NULL,/*@lInclOnPackList,*/
							1, /*@lInclOnPickList,*/
							NULL, /*@_iItemKey*/
							@ItemID,
							NULL, /*@_iOrigItemKey*/
							NULL, /*@_iOrigItemID*/
							@lReqCert,
			    				@SOLineNo,
							NULL,/*@_iSTaxClassKey*/
							@STaxClassID,
							@lLineStatus,		
							0, /*@_iSystemPriceDetermination*/
							NULL, /*@_iSalesPromotionID*/
							NULL, /*@_iSalesPromotionKey*/
							0, /*@_iUnitPriceFromSalesPromo*/
							0, /*@_iUnitPriceFromSchedule*/
							NULL,/*@_iUnitMeasKey*/
							@UnitMeasID,
							@UnitPrice,
							@LineUserFld1,
							@LineUserFld2,
							@UseItemDefaultIfBlank, /*@_iDefaultIfNull*/
							@LanguageID,
							@SOLineKey OUTPUT,
							@RetVal OUTPUT,
							@Migration,
							@iRptOption,	
							@iPrintWarnings,
							@BlankInvalidReference,
							@InvalidGLUseSuspense,
							1 /*@lCustomizedKit*/
	
						IF @@ERROR <> 0
							GOTO CloseAndDeallocateCursors
	
					      	-- Evaluate Return
					      	IF @RetVal IN (0, 3, 4)
					        BEGIN
					          	SELECT @CHILD_VALIDATION_FAILED = 1
					        END
		
						IF @lCmntOnly <> 1
						-- Do not create SO line dist record for Comment only item
						BEGIN
							/* SO Lines Validation*/
							SELECT @RetVal = 0
							SELECT @LineTradeDiscPct = @LineTradeDiscPct/100
		
			      				EXEC spsoGetSOLineDist --api
								@SOKey,
								@SOLineKey,
								NULL, /*@_iAcctRefKey*/
								NULL, /*@_iAcctRefID*/
								@AmtInvcd,
								NULL, /*@_iBlnktLineDistKey*/
								NULL, /*@_iRecurLineDistKey*/
								0, /*@_iCreatePO*/
								@lDeliveryMeth, /*@_iDeliveryMeth*/
								NULL, /*@_iOrigWhseKey*/
								NULL, /*@_iOrigWhseID*/
								NULL, /*@_iVendKey*/
								@VendorID, /*@_iVendID*/
								NULL, /*@_iPurchVendAddrKey*/
								NULL,/*@_iPurchVendAddrID*/
								@ExtAmt,
								NULL,/*@_iShipMethKey*/
								@ShipMethID,	
								NULL, /*@_iShipZoneKey*/
								NULL,/*@_iShipZoneID*/
							 	NULL, /*@_iFOBKey*/
								@FOBID,
						 		@LineFreightAmt,
								@ShipPriority,
								NULL, /*@_iGLAcctKey*/
								@GLAcctNo,
								@lHold,
								@HoldReason,
								@QtyOrd, /*@OrigQtyOrd*/
								@OrigPromiseDate,
								@PromiseDate,
								@RequestDate,
								@ShipDate,
								@QtyInvcd,
								@QtyOnBO,
								@QtyOpenToShip,
								@QtyOrd,
								@QtyShip,
								@QtyRtrnCredit,
								@QtyRtrnReplacement,
								NULL, /*@_iSTaxSchdKey*/
								NULL, /*@STaxSchdID*/
								NULL,/*@_iCopySTaxTranKey*/
								@lLineStatus,
								NULL, /*@_iShipToAddrKey*/
								@ShipToCustAddrID,/*@_iShipToAddrID*/
								@ShipToAddrName,
								NULL, /*@_iShipToCustAddrKey*/
								@ShipToCustAddrID,
								@ShipToAddrLine1,
								@ShipToAddrLine2,
								@ShipToAddrLine3,
								@ShipToAddrLine4,
								@ShipToAddrLine5,
								@ShipToCity,
								@ShipToStateID,
							    @ShipToCountryID,
								@ShipToPostalCode,
								0, /*@_iShipToTranOvrd*/
								0, /*@_iShipToCustAddrUpdCtr*/
								@LineTradeDiscAmt,
								@LineTradeDiscPct,
								NULL,/*@_iWhseKey*/
								@WarehouseID,
								@UseCustomerDefaultIfBlank, /*@_iDefaultIfNull*/	
								1, /*@_iPerformOvrd*/
								@SOLineDistKey OUTPUT,
								@RetVal OUTPUT,
								@PONumber,
								@Migration,
								@iRptOption,	
								@iPrintWarnings,
								@BlankInvalidReference,
								@InvalidGLUseSuspense
			
							IF @@ERROR <> 0
								GOTO CloseAndDeallocateCursors
	
						      	-- Evaluate Return
						      	IF @RetVal IN (0, 3, 4)
						        BEGIN
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
		
					      	/* Kit Routine **************************************** */
						--Validation BTO components if any.  If the BTO components are not passed		
						--in, create the components list based on the Standard Kit definition.
						--If the Item is a AssemblyKit Item, warning the user all the components
						--lines will be ignored during insertion
						SELECT @RetVal = 0
		
						SELECT 	@KitItemKey = SOL.ItemKey,
							@KitWhseKey = SOLD.WhseKey,
							@KitQty = SOLD.QtyOrd
						FROM 	#tsoSOLine SOL WITH (NOLOCK)
						JOIN	#tsoSOLineDist SOLD WITH (NOLOCK)
						ON	SOL.SOLineKey = SOLD.SOLineKey
						WHERE 	SOL.SOLineKey = @SOLineKey
		
						SELECT 	@lItemType = NULL
		
						SELECT 	@lItemType = ItemType
						FROM	timItem WITH (NOLOCK)
						WHERE	ItemKey = @KitItemKey
		
						-- If the Item is a BTOKit Item, varified the BTO Kit and it's
						-- Components
						IF @lItemType = @ItemType_BTOKit
						BEGIN
							DELETE #KitCompListVal
							INSERT INTO #KitCompListVal
								(RowKey, SessionKey, CompItemID, OrigCompItemQty, UOMID)
							SELECT 	RowKey, @iSessionKey, ItemID, QtyOrd, UnitMeasID
							FROM	#StgSOLine  WITH (NOLOCK)
							WHERE 	SOLineNo = @OrigSOLineNo
							AND	KitComponent = '1'	
							AND	SessionKey = @iSessionKey
							AND	TranNo = @TranNo
										
			      				EXEC spsoValidKitComponent
							@KitItemKey,		@KitQty,		@WarehouseID,
							@KitWhseKey,		@SOLineKey,		@iCompanyID,		
							@LanguageID,		@lEntityID,		@lSpid,			
							@RetVal OUTPUT,		
							@Migration,		@iRptOption,		@iPrintWarnings
	
							IF @@ERROR <> 0
								GOTO CloseAndDeallocateCursors
	
		
						      	IF @RetVal IN (0, 3)
						        BEGIN
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
		
						-- If the Item is a AssemblyKit Item
						IF @lItemType = @ItemType_AssemblyKit
						BEGIN
		
							DELETE #KitCompListVal
							INSERT INTO #KitCompListVal
								(RowKey, SessionKey, CompItemID, OrigCompItemQty, UOMID)
							SELECT 	RowKey, @iSessionKey, ItemID, QtyOrd, UnitMeasID
							FROM	#StgSOLine  WITH (NOLOCK)
							WHERE 	SOLineNo = @OrigSOLineNo
							AND	KitComponent = '1'	
							AND	SessionKey = @iSessionKey
							AND	TranNo = @TranNo
		
							IF EXISTS (SELECT 1 FROM #KitCompListVal)
							BEGIN
								IF @iRptOption <> @RPT_PRINT_NONE AND @iPrintWarnings = 1
								BEGIN
									EXEC ciBuildString @RPT_STRINGNO_IGNORE_COMP_LINE,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
		
									EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
							     		@iEntityID = @lEntityID, @iColumnID = 'ItemID', @iColumnValue = @ItemID,
							     		@iStatus = @MIG_STAT_WARNING, @iDuplicate = 0, @iComment = @Message,
							     		@oRetVal = @RetVal OUTPUT
								END
							END
		
						      	IF @@ERROR <> 0
						        BEGIN
								SELECT @RetVal = @@ERROR
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
						--End of Kit Validation
			
						SELECT @RetVal = 0
					      	EXEC  spsoKitFromSOLine @SOLineKey,  @SOLineKey, @RetVal OUTPUT
	
						IF @@ERROR <> 0
							GOTO CloseAndDeallocateCursors
		
						 /* Line Amount Validation*/
						SELECT @RetVal = 0
		
						IF @lCmntOnly <> 1
						BEGIN
			      				EXEC spsoLineAmts
			             				@SOKey, @SOLineKey, @RetVal OUTPUT,
								@Migration, @iRptOption, @iPrintWarnings
	
							IF @@ERROR <> 0
								GOTO CloseAndDeallocateCursors
			
						      	-- Evaluate Return
						      	IF @RetVal IN (0, 3, 4)
						        BEGIN
						          	SELECT @CHILD_VALIDATION_FAILED = 1
						        END
						END
					END        -- IF @DetailAction <> @REC_DELETE
---------------------------
--FOR FUTURE USE
---------------------------
	-- 					ELSE
	-- 					BEGIN      -- Delete the Sales Order Line BEGIN
	-- 		                		IF NOT EXISTS (SELECT 1 FROM tsoSOLine
	-- 		                                WHERE SOKey = @SOKey
	-- 		                                  AND SOLineNo = @SOLineNo)
	-- 						BEGIN
	-- 			                   		SELECT   @CHILD_VALIDATION_FAILED = 1
	-- 			                            			,@Duplicate = 0
	-- 			    						,@Status = @MIG_STAT_FAILURE
	-- 			    						,@StringNo = @RPT_STRINGNO_DELETE_FAILED
	-- 						END
	-- 						ELSE
	-- 						BEGIN
	-- 							DELETE tsoSOLine
	-- 							WHERE SOKey = @SOKey
	-- 							AND SOLineNo = @SOLineNo
	-- 							
	-- 							IF @@ERROR <> 0 	-- an error occured
	-- 							BEGIN
	-- 								SELECT @CHILD_VALIDATION_FAILED = 1, @Status = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
	-- 							END
	-- 							ELSE			-- an error did not occur
	-- 							BEGIN
	-- 								SELECT @Status = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
	-- 							END
	-- 						END
	-- 					END	
---------------------------
--FOR FUTURE USE
---------------------------
				END        -- Delete the Sales Order Line END
			--             WHILE 1 = 1
	
	
				/* Allocate Freight if necesary*/
				IF (@VALIDATION_FAILED = 0) AND (@CHILD_VALIDATION_FAILED = 0)
				BEGIN
					SELECT @RetVal = 0
		
					EXEC spsoAllocateFrght
						@SOKey, @FreightAmt, @RetVal OUTPUT,
						@AllocateMeth_AMT, @Migration, @iRptOption
	
					IF @@ERROR <> 0
						GOTO CloseAndDeallocateCursors
		
				      	-- Evaluate Return
				      	IF @RetVal IN (0, 3)
				        BEGIN
				          	SELECT @VALIDATION_FAILED = 1
				        END
				END

				----------------------------------------------------------------------------------
				-- Create SO
				----------------------------------------------------------------------------------		
				IF (@VALIDATION_FAILED = 1) OR (@CHILD_VALIDATION_FAILED = 1)
					IF @CHILD_VALIDATION_FAILED = 1
						SELECT @Duplicate = 0, @Status = @MIG_STAT_FAILURE, @StringNo = 430046
					ELSE
						SELECT @Duplicate = 0, @Status = @MIG_STAT_FAILURE
	
				ELSE
				BEGIN
					-- SO creation
					SELECT @RetVal = 0
					EXEC spsoCreateSalesOrder
					     @SOKey,		NULL, /*SalesAmt*/	@FreightAmt,
					     @STaxAmt,		NULL, /*TranAmt*/	@OpenAmt,
					     NULL,/*@AmtInvcd,*/@TradeDiscAmt,		NULL, /*@CrHold*/
					     0, /*@_iUpdateRel*/0, /*@_iFixedRate*/	@RetVal  OUTPUT,
					     @Migration,	@iRptOption,		@iPrintWarnings
	
					IF @@ERROR <> 0
						GOTO CloseAndDeallocateCursors
	
					IF @RetVal IN (0,3,4) 	-- an error occured
					BEGIN
						SELECT @Status = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
					END
					ELSE			-- an error did not occur
					BEGIN
						SELECT @Status = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
					END
				END
	
				-- Log the Errors to tdmMigrationLogWrk
				INSERT INTO #tciMessageBuilder (
					KeyValue,		ColumnID,		ColumnValue,		
					SessionKey,		Duplicate,		EntityID,		
					Status,			
					StringNo,		StringData0,		StringData1)
				SELECT
					EntryNo,		StringData2,		StringData3,
					@iSessionKey,		0,			StringData1,		
					CASE WHEN Severity = 2 	THEN @MIG_STAT_FAILURE
					     WHEN Severity = 1 	THEN @MIG_STAT_WARNING  END,			
					StringNo,		StringData4,		StringData5
				FROM	tciErrorLog WITH (NOLOCK)
				WHERE	SessionID = @lSpid
				AND	ErrorType = 3
				AND	Severity IN (1, 2)
	
				DELETE 	tciErrorLog
				WHERE  	SessionID = @lSpid
				AND	ErrorType = 3
	
				-- Populate the error log with detail message
				IF (@iRptOption <> @RPT_PRINT_NONE)
				BEGIN
					IF (@iRptOption = @RPT_PRINT_SUCCESSFUL)
						DELETE #tciMessageBuilder
						WHERE Status = @MIG_STAT_FAILURE
						OPTION(KEEPFIXED PLAN, KEEP PLAN)
				
					/* Call the sp to build the error message */
					EXEC spimUpdateErrCmnt @LanguageID, @RetVal OUTPUT
				
					IF @RetVal <> @RET_SUCCESS
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						GOTO CloseAndDeallocateCursors
						RETURN
					END
				
					-- Write out to the final temp table for permenent error log tale	
					INSERT	 #tdmMigrationLogEntryWrk
						(ColumnID, ColumnValue, Duplicate, Comment, EntityID, Status, SessionKey)
					SELECT 	ColumnID, COALESCE(ColumnValue,''), Duplicate, MessageText, EntityID, Status, @iSessionKey
					FROM 	#tciMessageBuilder  WITH (NOLOCK)
					WHERE 	SessionKey = @iSessionKey
					OPTION(KEEPFIXED PLAN, KEEP PLAN)
				END	
			END

			----------------------------------------------------------------
			-- Log errors into report log
			----------------------------------------------------------------
		IF (@iRptOption = @RPT_PRINT_ALL
			OR (@iRptOption = @RPT_PRINT_UNSUCCESSFUL AND (@VALIDATION_FAILED = 1 OR @CHILD_VALIDATION_FAILED = 1))
			OR (@iRptOption = @RPT_PRINT_SUCCESSFUL AND @VALIDATION_FAILED = 0))
		BEGIN
	EXEC spdmCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT
		END

			--------------------------------------------
			-- Write Migration Log record if appropriate
			--------------------------------------------
			IF @VALIDATION_FAILED = 0 AND
				((@iRptOption = @RPT_PRINT_ALL) OR
				(@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL AND @Status = @MIG_STAT_FAILURE) OR
				(@iRptOption =  @RPT_PRINT_SUCCESSFUL AND @Status = @MIG_STAT_SUCCESSFUL))
			BEGIN
				-- Default @Message to NULL string				
				SELECT @Message = NULL
						
				-- Lookup the comment if one was specified
				IF @StringNo IS NOT NULL
					BEGIN
						EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
					END
								
				EXEC spdmCreateMigrationLogEntry @iLanguageID = @LanguageID, @iSessionKey = @iSessionKey,
				@iEntityID = @TranNo, @iColumnID = '', @iColumnValue = '',
				@iStatus = @Status, @iDuplicate = @Duplicate,
				@iComment = @Message, @oRetVal = @RetVal OUTPUT
				
				--SELECT @EntryNo = @EntryNo + 1
	
			END /* Reporting was requested */

			IF @Status = @MIG_STAT_FAILURE
				SELECT @oFailedRecs = @oFailedRecs + 1

			------------------------------------------------------------------------------------
			-- If using staging tables, then mark failed records with the failed
			-- record processing status and delete successful rows.
			------------------------------------------------------------------------------------
			IF @UseStageTable <> 1
			   BEGIN -- using temp table only
				IF @Status = @MIG_STAT_FAILURE -- did not process
				BEGIN
					DELETE #StgSalesOrder
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	

					UPDATE #StgSOLine
					SET ProcessStatus = @GENRL_PROC_ERR
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	
				END
				ELSE
				BEGIN
					DELETE #StgSalesOrder
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	

					DELETE #StgSOLine
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	
				END
			   END
			ELSE
			   BEGIN -- using staging table
				IF @Status = @MIG_STAT_FAILURE -- did not process
				BEGIN
					DELETE #StgSalesOrder
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	

					UPDATE StgSOLine
					SET ProcessStatus = @GENRL_PROC_ERR
					FROM StgSOLine a
					JOIN #StgSOLine b
					ON a.RowKey = b.RowKey
					WHERE b.TranNo = @TranNo
					AND b.SessionKey = @iSessionKey	
					AND a.SessionKey = @iSessionKey
				END
				ELSE
				BEGIN

					DELETE #StgSalesOrder
					WHERE TranNo = @TranNo
					AND SessionKey = @iSessionKey	

					DELETE StgSOLine
					FROM StgSOLine a
					JOIN #StgSOLine b
					ON a.RowKey = b.RowKey
					WHERE b.TranNo = @TranNo
					AND b.SessionKey = @iSessionKey	
					AND a.SessionKey = @iSessionKey
				END
			   END
						

			SELECT @RowsProcThisCall = @RowsProcThisCall + 1	
	
			-- Keep SP caller informed of how many rows were processed.
			SELECT @oRecsProcessed = @oRecsProcessed + 1
			
			FETCH 	curSOLine
			INTO 	@RowKey
			
		END /* WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL) */
	
		IF CURSOR_STATUS ('global', 'curSOLine') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
			CLOSE curSOLine

	END /* End of Time Loop */

	IF EXISTS(SELECT * FROM #StgSalesOrder WITH (NOLOCK) WHERE
	ProcessStatus = @NOT_PROCESSED AND SessionKey = @iSessionKey)
	BEGIN
		SELECT @oContinue = 1
	END
	ELSE
	BEGIN
		SELECT @oContinue = 0
	
	END

	IF @oContinue = 0
	BEGIN
		-- Time to Deallocate the Cursor
		IF (CURSOR_STATUS ('global', 'curSOLine') = @STATUS_CURSOR_CLOSED)
			DEALLOCATE curSOLine
	END
	ELSE
	BEGIN
		SELECT @_oRetVal = @RET_SUCCESS
		RETURN	
	END

CloseAndDeallocateCursors:
	--We are not going to continue running this sp.
	SELECT @oContinue = 0

	--Close and Deallocate the cursor, if it exists.
	IF CURSOR_STATUS ('global', 'curSalesOrder') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
		CLOSE curSalesOrder

	--Time to Deallocate the Cursor
	IF (CURSOR_STATUS ('global', 'curSalesOrder') = @STATUS_CURSOR_CLOSED)
		DEALLOCATE curSalesOrder

	--Close and Deallocate the cursor, if it exists.
	IF CURSOR_STATUS ('global', 'curSOLine') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
		CLOSE curSOLine

	--Time to Deallocate the Cursor
	IF (CURSOR_STATUS ('global', 'curSOLine') = @STATUS_CURSOR_CLOSED)
		DEALLOCATE curSOLine

END /* MAIN CODE BLOCK */

--GRANT EXECUTE ON spSOapiSalesOrdIns TO public
--GO

GO



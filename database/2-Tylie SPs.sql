USE [MAS500_Test]
GO

/****** Object:  StoredProcedure [dbo].[spSOapiSalesOrdIns_Tylie]    Script Date: 12/18/2018 5:18:08 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



create procedure [dbo].[spSOapiSalesOrdIns_Tylie]
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
	@iSessionKey		INT,			
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

/****** Object:  StoredProcedure [dbo].[spARCustomerImport_Tylie]    Script Date: 12/18/2018 5:18:08 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create procedure [dbo].[spARCustomerImport_Tylie](
	@_oRetVal			INTEGER	OUTPUT	)
AS

---------------------------------------------------------------------------------
-- NAME: 		spARapiCustomerIns
--
-- DESCRIPTION:	Migrates Customers from staging or temporary tables
--		to the real tables via custom SQL statements contained within this
--		stored procedure.
--
-- MIGRATION SP INTERFACE PARAMETERS:
--	@oContinue		Supported.  0=All rows processed   1=Unprocessed rows still exist.
--	@iCancel		Supported.  0=Normal   1=Cancel any existing processing
--	@iSessionKey 		Supported.  Unique ID for the set of records to process.
--	@iCompanyID		Supported.  Enterprise Company.
--	@iRptOption		Partially Supported.  0=All  1=None  2=Good  3=Bad
--	@iPrintWarnings		Not applicable to this SP returns no warnings.
--	@iUseStageTable 		If 1, then data will be obtained from the staging tables, otherwise
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
-- 	- If the caller wants to use staging table data, then if the temporary tables already exist
--	they will be truncated.  If the temporary tables don't exist they will be created.
--  	Data from the staging tables will then be placed into the temporary tables.  If the caller
--	doesn't use staging tables, then it is assumed that they have created them and that the
--  	temporary tables contain the desired data.
--	- SET NOCOUNT is ON.  This is done to support asynchronous calls from ADO.
--	
--	AUTHOR: Best Enterprise Suite Data Migration Team
--  Copyright:         Copyright (c) 1995-2015 Sage Software, Inc. All Rights Reserved.
---------------------------------------------------------------------------------
Declare @oContinue			SMALLINT,		
	@iCancel			SMALLINT,	
	@iSessionKey			INTEGER,			
	@iCompanyID 			VARCHAR(3),
	@iRptOption			INTEGER 	= 1,	-- Default = Print none.
	@iPrintWarnings			SMALLINT 	= 1,	-- Default = Print warnings
	@iUseStageTable			SMALLINT 	= 1,	-- Default = Use staging tables.
	@oRecsProcessed			INTEGER,		
	@oFailedRecs			INTEGER,
	@oTotalRecs			INTEGER	

BEGIN -- MAIN CODE BLOCK
	SET NOCOUNT ON

	-------------------
	-- TEMP TABLES
	------------------

	-- Create GL Validation temp tables.
	IF OBJECT_ID ('tempdb..#tdmMigrationLogEntryWrk') IS NULL
		SELECT * INTO #tdmMigrationLogEntryWrk FROM tdmMigrationLogWrk WHERE 1=2

	IF OBJECT_ID ('tempdb..#DMValGLAcct') IS NULL
	CREATE TABLE #DMValGLAcct (
		DMValGLAcctKey integer not null IDENTITY(1,1),
		GLAcctNo varchar(100) not null,
		ColumnID varchar(35) null DEFAULT '',
		EntityID varchar(255) null DEFAULT '',
		CurrID VARCHAR(3) null,
		RefCode VARCHAR(20) null,
		EffectiveDate VARCHAR(20) null,
		GLAcctKey integer null,
		AcctRefKey integer null,
		IsValid smallint not null default 0,
		ValidationType smallint not null default 0)

	IF OBJECT_ID ('tempdb..#DMValGLAcctErrs') IS NULL
	CREATE TABLE #DMValGLAcctErrs (
		DMValGLAcctKey integer not null,
		GLAcctKey integer not null,
		ColumnID varchar(35) null DEFAULT '',
		ColumnValue varchar(100) null,
		Comment varchar(255) not null,
		Duplicate smallint null,
		EntityID varchar(255) null DEFAULT '',
		SessionKey int not null,
		Status VARCHAR(15) null,
		TargetCompanyID VARCHAR(3) null)

	IF object_id('tempdb..#tarCustHdrValWrk') IS NULL
	CREATE TABLE #tarCustHdrValWrk (
		ABANo VARCHAR(10) NULL ,
		Action smallint NOT NULL DEFAULT (0),
		AllowCustRefund VARCHAR(3) NULL ,
		AllowInvtSubst VARCHAR(3) NULL ,
		AllowWriteOff VARCHAR(3) NULL ,
		BillingType VARCHAR(25) NULL ,
		BOLReqd varchar(3)NULL,
		CarrierAcctNo varchar (20)NULL,
		CarrierBillMeth varchar(25) NULL,
		CreditLimit decimal(15, 3) NULL ,
		CreditLimitAgeCat VARCHAR(25) NULL ,
		CreditLimitUsed VARCHAR(3) NULL ,
		CRMCustID varchar (32) NULL ,
		CurrExchSchdID VARCHAR(15) NULL ,
		CurrExchSchdKey int NULL,
		CurrID VARCHAR(3) NULL ,
		CustClassID VARCHAR(15) NULL ,
		CustClassKey int NULL,
		CustID VARCHAR(12) NULL ,
		CustKey int NULL,
		CustName varchar (40) NULL ,
		CustRefNo VARCHAR(15) NULL ,
		DateEstab datetime NULL ,
		DfltBillToAddrID VARCHAR(15) NULL ,
		DfltBillToAddrKey int NULL,
		DfltItemID VARCHAR(30) NULL ,
		DfltItemKey int NULL,
		DfltSalesAcctKey int NULL,
		DfltSalesAcctNo varchar (100) NULL ,
		DfltSalesReturnAcctKey int NULL,
		DfltSalesReturnAcctNo varchar (100) NULL ,
		DfltShipToAddrID VARCHAR(15) NULL ,
		DfltShipToAddrKey int NULL,
		FinChgFlatAmt decimal(15, 3) NULL ,
		FinChgPct decimal(5, 2) NULL ,
		FreightMethod varchar (25) NULL,
		Hold VARCHAR(3) NULL ,
		InvoiceReqd varchar (3) NULL,
		Name varchar (40) NULL ,
		PackListContentsReqd varchar (3) NULL,
		PackListReqd varchar(3) NULL,
		PmtTermsID VARCHAR(15) NULL ,
		PmtTermsKey int NULL,
PriceAdj decimal(5,2),
		PrimaryAddrKey int NULL,
		PrimaryCntctKey int NULL,
		PrintDunnMsg VARCHAR(3) NULL ,
		PrintOrderAck VARCHAR(3) NULL ,
		ProcessStatus smallint NOT NULL DEFAULT (0),
		ReqPO VARCHAR(3) NULL ,
		RequireSOAck VARCHAR(3) NULL ,
		Residential varchar (3) NULL,
		RowKey int NOT NULL ,
		SalesSourceID VARCHAR(15) NULL ,
		SalesSourceKey int NULL,
		SessionKey int NOT NULL,
		ShipLabelsReqd varchar(3)NULL,
		ShipPriority smallint NULL ,
		Status VARCHAR(25) NULL ,
		StdIndusCodeID VARCHAR(7) NULL ,
		StmtCycleID VARCHAR(15) NULL ,
		StmtCycleKey int NULL,
		StmtFormID VARCHAR(15) NULL ,
		StmtFormKey int NULL,
		TradeDiscPct decimal(5, 2) NULL ,
		UserFld1 VARCHAR(15) NULL ,
		UserFld2 VARCHAR(15) NULL ,
		UserFld3 VARCHAR(15) NULL ,
		UserFld4 VARCHAR(15) NULL ,
		VendID VARCHAR(12) NULL ,
		VendKey int NULL)


	IF object_id('tempdb..#tarCustAddrValWrk') IS NULL

	CREATE TABLE #tarCustAddrValWrk (
		CtrlRowKey int IDENTITY(1,1) NOT NULL,
		RowKey int NULL ,
		AddrLine1 varchar (40) NULL ,
		AddrLine2 varchar (40) NULL ,
		AddrLine3 varchar (40) NULL ,
		AddrLine4 varchar (40) NULL ,
		AddrLine5 varchar (40) NULL ,
		AddrName varchar (40) NULL ,
		AllowInvtSubst VARCHAR (3) NULL ,
		City VARCHAR (20) NULL ,
		CloseSOLineOnFirstShip VARCHAR(3) NULL ,
		CloseSOOnFirstShip VARCHAR(3) NULL ,
		CommPlanID VARCHAR (15) NULL ,
		CountryID VARCHAR (3) NULL ,
		CurrExchSchdID VARCHAR (15) NULL ,
		CurrID VARCHAR (3) NULL ,
		CustAddrID VARCHAR (15) NULL ,
		CustID VARCHAR (12) NULL ,
		CustPriceGroupID VARCHAR (15) NULL ,
EMailAddr varchar (256) NULL ,
		Fax VARCHAR (17) NULL ,
		FaxExt VARCHAR (4) NULL ,
		FOBID VARCHAR (15) NULL ,
		InvcFormID VARCHAR (15) NULL ,
		InvcMsg varchar (100) NULL ,
		LanguageID VARCHAR (25) NULL ,
		Name varchar (40) NULL ,
		PackListFormID VARCHAR (15) NULL ,
		Phone VARCHAR (17) NULL ,
		PhoneExt VARCHAR (4) NULL ,
		PmtTermsID VARCHAR (15) NULL ,
		PostalCode varchar (10) NULL ,
		PriceBase VARCHAR (25) NULL ,
		PrintOrderAck VARCHAR (3) NULL ,
		RequireSOAck VARCHAR (3) NULL ,
		SalesTerritoryID VARCHAR (15) NULL ,
		ShipComplete VARCHAR (3) NULL ,
		ShipDays smallint NULL ,
		ShipLabelFormID VARCHAR (15) NULL ,
		ShipMethID VARCHAR (15) NULL ,
		SOAckFormID VARCHAR (15) NULL ,
		SperID VARCHAR (12) NULL ,
		StateID VARCHAR (3) NULL ,
		STaxSchdID VARCHAR (15) NULL ,
		Title varchar (40) NULL ,
		WhseID VARCHAR (6) NULL ,
		ProcessStatus smallint NULL ,
		AddrKey int NULL ,
		CommPlanKey int NULL ,
		CRMAddrID varchar (32) NULL ,
		CRMContactID varchar (32) NULL ,
		CurrExchSchdKey int NULL ,
		CustKey int NULL ,
		CustPriceGroupKey int NULL ,
		DfltCntctKey int NULL ,
		FOBKey int NULL ,
		ImportLogKey int NULL ,
		InvcFormKey int NULL ,
		PackListFormKey int NULL ,
		PmtTermsKey int NULL ,
		PriceAdj decimal(5, 2) NULL ,
		SalesTerritoryKey int NULL ,
		ShipLabelFormKey int NULL ,
		ShipMethKey int NULL ,
		SOAckFormKey int NULL ,
		SperKey int NULL ,
		STaxSchdKey int NULL ,
		WhseKey int NULL,
		BOLReqd varchar(3)NULL,
		InvoiceReqd varchar (3) NULL,
		PackListReqd varchar(3) NULL,
		PackListContentsReqd varchar (3) NULL,
		Residential varchar (3) NULL,
		ShipLabelsReqd varchar(3)NULL,
		CarrierAcctNo varchar (20)NULL,
		CarrierBillMeth varchar(25) NULL,
		FreightMethod varchar (25) NULL,
		Action smallint NOT NULL DEFAULT (0))

	IF object_id('tempdb..#tarCustDocTrnsmitWrk') IS NULL
	CREATE TABLE #tarCustDocTrnsmitWrk
		(RowKey INTEGER NULL,
		ProcessStatus SMALLINT NULL,
		CustKey INTEGER NULL,
		Action smallint NOT NULL DEFAULT (0))

	IF object_id('tempdb..#CustHdrUpdColumnList') IS NULL
	CREATE TABLE #CustHdrUpdColumnList
		(SessionKey int NOT NULL,
		 ColumnName varchar(255) NOT NULL)

	IF object_id('tempdb..#CustAddrUpdColumnList') IS NULL
	CREATE TABLE #CustAddrUpdColumnList
		(SessionKey int NOT NULL,
		 ColumnName varchar(255) NOT NULL)

	IF object_id('tempdb..#CustContactUpdColumnList') IS NULL
	CREATE TABLE #CustContactUpdColumnList
		(SessionKey int NOT NULL,
		 ColumnName varchar(255) NOT NULL)

	IF object_id('tempdb..#CustDocTranUpdColumnList') IS NULL
	CREATE TABLE #CustDocTranUpdColumnList
		(SessionKey int NOT NULL,
		 ColumnName varchar(255) NOT NULL)

	IF object_id('tempdb..#CustSTaxUpdColumnList') IS NULL
	CREATE TABLE #CustSTaxUpdColumnList
		(SessionKey int NOT NULL,
		 ColumnName varchar(255) NOT NULL)

	-------------
	-- DECLARATIONS OF VARIABLES AND CONSTANTS
	-------------
	------------------- GENERAL MIGRATION CONSTANTS --------------------------------
	-- Return Constants
	DECLARE @RET_UNKNOWN_ERR		SMALLINT,		@RET_SUCCESS			SMALLINT
	
	SELECT	@RET_UNKNOWN_ERR		= 0,			@RET_SUCCESS			= 1

	-- ProcessingStatus constants
	DECLARE @NOT_PROCESSED			SMALLINT,		@GENRL_PROC_ERR			SMALLINT,
			@PROCESSED				SMALLINT

	SELECT	@NOT_PROCESSED			= 0,			@GENRL_PROC_ERR			= 2,
			@PROCESSED				= 1

	-- Migration Status constants
	DECLARE @MIG_STAT_SUCCESSFUL	SMALLINT,		@MIG_STAT_FAILURE		SMALLINT,
			@MIG_STAT_WARNING		SMALLINT,		@MIG_STAT_INFO			SMALLINT

	SELECT	@MIG_STAT_INFO			= 0,			@MIG_STAT_SUCCESSFUL	= 3,
			@MIG_STAT_FAILURE		= 1,			@MIG_STAT_WARNING		= 2

	-- Reporting Constants
	DECLARE @RPT_PRINT_ALL			SMALLINT,		@RPT_PRINT_NONE			SMALLINT,
			@RPT_PRINT_SUCCESSFUL	SMALLINT,		@RPT_PRINT_UNSUCCESSFUL	SMALLINT

	SELECT	@RPT_PRINT_ALL			= 0,			@RPT_PRINT_NONE			= 1,
			@RPT_PRINT_SUCCESSFUL	= 2,			@RPT_PRINT_UNSUCCESSFUL	= 3

	DECLARE @RecordHadErrors		SMALLINT
	SELECT 	@RecordHadErrors			= 0

	-- Behavior constants: Invalid GL Account, DuplicationAction, invalid reference Parameter options
	DECLARE @DO_NOT_MIGRATE			SMALLINT,		@GL_USE_SUSPENSE_ACCT		SMALLINT,
			@REPLACE_DUPLICATE		SMALLINT,		@INVALIDREF_USE_Blank		SMALLINT
	SELECT	@DO_NOT_MIGRATE			= 0,			@GL_USE_SUSPENSE_ACCT		= 1,
			@REPLACE_DUPLICATE		= 1,			@INVALIDREF_USE_Blank		= 1

	--	Migration Constants that are based on the migration entity
		-- @MAX_ROWS_PER_RUN is the threshhold to trigger @oContinue to be set to true.
		-- It allows the caller to continuously call this SP so that a status can be reported
		-- back to the user.  The lower the value, the more frequently the user can recieve progress feedback.
	DECLARE @CREATE_TYPE_MIGRATE	SMALLINT,		@MAX_ROWS_PER_RUN		INTEGER,
			@MIGRATE_SECS			SMALLINT,		@BLOCK_PROCESS_SIZE		INTEGER

	SELECT	@CREATE_TYPE_MIGRATE		= 7,		@MAX_ROWS_PER_RUN		= 125,--For CustAddrInsert
			@MIGRATE_SECS			= 3

	--	Cursor constants
	DECLARE @FETCH_SUCCESSFUL		INTEGER,		@FETCH_PAST_LAST_ROW		INTEGER,		
			@FETCH_ROW_MISSING		INTEGER
	SELECT	@FETCH_SUCCESSFUL		= 0,			@FETCH_PAST_LAST_ROW		= -1,
			@FETCH_ROW_MISSING		= -2

	-- Temp Table Creation Constants
	DECLARE @CREATED_OUTSIDE_THIS_SP	INTEGER,	@CREATED_INSIDE_THIS_SP		INTEGER

	SELECT 	@CREATED_OUTSIDE_THIS_SP 	= 1,		@CREATED_INSIDE_THIS_SP 	= 2

	-- Cursor Status Constants
	DECLARE @STATUS_CURSOR_OPEN_WITH_DATA 	INTEGER,@STATUS_CURSOR_OPEN_BUT_EMPTY	INTEGER,
	@STATUS_CURSOR_CLOSED         	INTEGER,@STATUS_CURSOR_DOES_NOT_EXIST	INTEGER

	SELECT 	@STATUS_CURSOR_OPEN_WITH_DATA 	= 1,	@STATUS_CURSOR_OPEN_BUT_EMPTY 	= 0,
	@STATUS_CURSOR_CLOSED 		= -1,		@STATUS_CURSOR_DOES_NOT_EXIST 	= -3

	DECLARE @TempTableCreation       	SMALLINT

	-- Report Constants
	DECLARE @RPT_STRINGNO_UNSUCCESSFUL	INTEGER, 	@RPT_STRINGNO_CANCELLED			INTEGER,
			@RPT_STRINGNO_DUPLICATE		INTEGER,	@RPT_STRINGNO_INVALID_COMPANY	INTEGER,
			@RPT_STRINGNO_INVALIDREF	INTEGER, 	@RPT_STRINGNO_MISSING			INTEGER,
			@RPT_STRINGNO_COMMCLASS		INTEGER,	@RPT_STRINGNO_SPERID_INVALID 	INTEGER,
			@RPT_STRINGNO_TAXEXEMPT		INTEGER,	@RPT_STRINGNO_CANTBEBLANK		INTEGER,
			@RPT_STRINGNO_PARENTCUST	INTEGER,	@RPT_STRINGNO_YESNO      		INTEGER,
			@RPT_STRINGNO_UPDATE_FAILED	INTEGER,	@RPT_STRINGNO_DELETE_FAILED    	INTEGER,
			@RPT_STRINGNO_VALUEMUSTBE	INTEGER,	@RPT_STRINGNO_STGDATA_IGNORED	INTEGER

	SELECT	@RPT_STRINGNO_UNSUCCESSFUL	= 100447,	@RPT_STRINGNO_CANCELLED			= 100448,
			@RPT_STRINGNO_DUPLICATE		= 110067,	@RPT_STRINGNO_INVALID_COMPANY	= 160228,
			@RPT_STRINGNO_INVALIDREF	= 430009,	@RPT_STRINGNO_MISSING			= 51135,
			@RPT_STRINGNO_COMMCLASS		= 151136, 	@RPT_STRINGNO_SPERID_INVALID 	= 150788,
			@RPT_STRINGNO_TAXEXEMPT		= 262708,	@RPT_STRINGNO_CANTBEBLANK 		= 207,
			@RPT_STRINGNO_PARENTCUST	= 151769,	@RPT_STRINGNO_YESNO      		= 430106,
			@RPT_STRINGNO_UPDATE_FAILED	= 430127,	@RPT_STRINGNO_DELETE_FAILED    	= 430128,
			@RPT_STRINGNO_VALUEMUSTBE	= 100469,	@RPT_STRINGNO_STGDATA_IGNORED	= 430157

	DECLARE @LIST_DBVALUE			INTEGER, 		@LIST_LOCALTEXT			INTEGER
	SELECT	@LIST_DBVALUE			= 0, 			@LIST_LOCALTEXT			= 1

	-- tsmMigrateStepParam constants (The actual param numbers will differ for each routine)
	-- These are used to retrieve the parameters by a named constant (vs. ordinal ParamNo)
	DECLARE @PARAM_REFERENCE 		SMALLINT,		@PARAM_DUPLICATE		SMALLINT,
			@PARAM_GL				SMALLINT, 		@PARAM_DEFAULT			SMALLINT

	SELECT 	@PARAM_GL				= 1, 			@PARAM_REFERENCE 		= 2,
			@PARAM_DUPLICATE 		= 3, 			@PARAM_DEFAULT 			= 4	

	DECLARE @DO_NOT_LOG_ERRS 		SMALLINT, 		@RET_GLACCT_INVALID 	SMALLINT,
			@RET_SUSPENSE_ACCT 		SMALLINT

	SELECT	@DO_NOT_LOG_ERRS 		= 0,			@RET_GLACCT_INVALID 	= 2,
			@RET_SUSPENSE_ACCT 		= 2

	DECLARE @RET_VALIDATION_ERROR 	SMALLINT
	SELECT	@RET_VALIDATION_ERROR 	= 2

	-- Record Action Constants
	DECLARE	@REC_INSERT				SMALLINT,		@REC_UPDATE				SMALLINT,
			@REC_DELETE				SMALLINT,		@REC_AUTO				SMALLINT,	
			@ACTION_COL_USED		SMALLINT
	
	SELECT  @REC_INSERT 			= 1,			@REC_UPDATE 			= 2,
			@REC_DELETE 			= 3,			@REC_AUTO 				= 0,
			@ACTION_COL_USED		= 1

	-- Variables
	DECLARE @SourceRowCount			INTEGER,		@RowsProcThisCall		INTEGER,			
			@UserID					VARCHAR(30),	@Message				VARCHAR(255),	
			@LanguageID				INTEGER,		@Duplicate				SMALLINT,		
			@ProcessStatus			SMALLINT,		@StringNo				INTEGER,
			@StartDate				DATETIME,		@RetVal					INTEGER,			
			@ColumnValue			VARCHAR(50),--	@EntityID				VARCHAR(30),
			@Validated 				SMALLINT,		@RowKey					INTEGER,
			@SuspenseAcctKey		INTEGER,		@BlankInvalidReference	SMALLINT,
			@InvalidGLUseSuspense	SMALLINT,		@IsGLAcctValid 			SMALLINT,
			@AddrRowCount			INTEGER,		@StartKey 				INTEGER,
			@EndKey					INTEGER,		@CustAddrRowKey			INTEGER,
			@NeedsCntctIns 			INTEGER, 		@CreateDate				DATETIME,
			@HomeCurrID				VARCHAR(3),		@lRecFailed				INTEGER,
			@lRowCount 				INTEGER,		@NoOfKeys 				INTEGER,
			@KeyStart 				INTEGER,		@KeyEnd					INTEGER,
			@NumCustRows			INTEGER,		@CntctRowCount			INTEGER,
			@TaskString				VARCHAR(255),	@Message1				VARCHAR(255),
		@Action                 SMALLINT,		@UpdColList 			VARCHAR(4000),
			@UpdColList1 			VARCHAR(4000),	@SQLStmt 				VARCHAR(5000),
			@UpdColListHdr			VARCHAR(4000),	@UpdColListAddr			VARCHAR(4000),
			@UserBusinessDate		DATETIME,		@ErrorCount				INT,
			@UpdRecCount			INT,			@EntityColName varchar(255)

	-- Option variables
	DECLARE	@ChkCreditLimit			SMALLINT,		@PrintInvcs				SMALLINT,	
			@UseMultCurr			SMALLINT,		@UseSper				SMALLINT,	
			@SOActivated			SMALLINT,	
			@ExtShipSystem			SMALLINT,	
			@ARIntegratedWithMC		SMALLINT,		@APActivated			SMALLINT,
			@IMActivated			SMALLINT,		@UseSTax				SMALLINT,
			@STaxRecCount			INT

	-- Default output parameter values
	SELECT	@_oRetVal 				= @RET_UNKNOWN_ERR,
			@oRecsProcessed 		= COALESCE(@oRecsProcessed,0),
			@oTotalRecs 			= COALESCE(@oTotalRecs,0),	
			@oFailedRecs 			= COALESCE(@oFailedRecs,0),
			@oContinue	 			= 0,
			@CreateDate 			= getdate(), -- get the date for CreateDate in tarCustomer , tarCustAddr, tciContact
			@lRecFailed				= 0,
			@RowsProcThisCall		= 0
				
	-- Get the migration user
	EXEC spDMMigrUserIDGet @iSessionKey, @iUseStageTable, @UserID OUTPUT

	-- Get LanguageID and Business Date for migration user (use english if not found)
	-- Modified to check for SysDateAsBusDate is 1 if so then use SystemDate else BusinessDate
	SELECT 	@LanguageID = COALESCE(LanguageID, 1033),
			@UserBusinessDate = CASE  WHEN SysDateAsBusDate = 1 THEN GETDATE()
									  ELSE UserBusinessDate
								END
	FROM 	tsmUser WITH (NOLOCK)
	WHERE 	UserID = @UserID

	-- Added so LanguageID is not null, English is default 12 May 2006
	Select @LanguageID = COALESCE(@LanguageID, 1033)

	-- Get HomeCurrID
	SELECT  @HomeCurrID = CurrID
	FROM 	tsmCompany WITH (NOLOCK)
	WHERE 	CompanyID = @iCompanyID

	---------------------------------------------------------------------------------
	-- Entity Specific Constants and Variables		
	---------------------------------------------------------------------------------
	-- StgCustomer Variables

	DECLARE	@CustID				VARCHAR(12),	@CustAddrID			VARCHAR(15),
			@STaxCodeID			VARCHAR(15),	@ExmptNo			VARCHAR(15),
@Name               VARCHAR(40),    @EMailAddr   VARCHAR(256),
			@EntityID 			VARCHAR(255),	
			@EMail 				VARCHAR(3),		@EMailFormat 		VARCHAR(25),
			@Fax 				VARCHAR(3),		@HardCopy 			VARCHAR(3),
			@TranType			VARCHAR(25),	@ListValues			VARCHAR(255)	
	-- Foriegn Key variables

	DECLARE @CntctKey			INTEGER,		@AddrKey			INTEGER,
			@CurrExchSchdKey		INTEGER,	@CustKey			INTEGER,
			@STaxSchdKey			INTEGER,	@STaxCodeKey		INTEGER,	
			@DfltSalesReturnAcctKey 	INTEGER

	-- Constants
	DECLARE @MODULE_DESC 			VARCHAR(80),	@MODULE_AR 			INTEGER,
			@ENTITYTYPE_CUST 		INTEGER,		@RPT_STRINGNO_MODULE_INACTIVE	INTEGER,
			@RPT_STRINGNO_X_IS_INVALID 	INTEGER,	@RPT_STRINGNO_MODULE_DESC 	INTEGER,
			@RPT_INVALID_NULL_STRINGNO 	INTEGER,	@RPT_INVALID_GL 		INTEGER,
			@MODULE_AP			INTEGER,			@MODULE_SO			INTEGER,
			@MODULE_IM			INTEGER,			@MODULE_MC			INTEGER	
	

	SELECT 	@MODULE_AR 			= 05,				@ENTITYTYPE_CUST 		= 501,
			@RPT_STRINGNO_MODULE_INACTIVE 	= 100463,	@RPT_STRINGNO_X_IS_INVALID	= 151465,
			@RPT_STRINGNO_MODULE_DESC 	= 50774,	@RPT_INVALID_NULL_STRINGNO	= 430008,
			@RPT_INVALID_GL 		= 10039,		@MODULE_AP 			= 4,
			@MODULE_IM			= 7,				@MODULE_SO			= 8,
			@MODULE_MC			= 10
	

	DECLARE @TASKID_CUSTOMER	 	INTEGER,		@RPT_STRINGNO_SEEFORVALID	INTEGER,
			@TASKID_STAXCODE		INTEGER

	SELECT	@TASKID_CUSTOMER 		= 83952612,		@RPT_STRINGNO_SEEFORVALID	= 430103,
			@TASKID_STAXCODE		= 33619972
	


	-- tsmMigrateStepParam constants (The actual param numbers will differ for each routine)
	-- These are used to retrieve the parameters by a named constant (vs. ordinal ParamNo)
	DECLARE @INVALIDGL_PARM_TYPE	SMALLINT, 		@REF_PARM_TYPE 			SMALLINT,
			@DUPL_PARM_TYPE			SMALLINT,		@DFLT_PARM_TYPE			SMALLINT

	SELECT @INVALIDGL_PARM_TYPE		= 1, 			@REF_PARM_TYPE 			= 2,
			@DUPL_PARM_TYPE 		= 3, 			@DFLT_PARM_TYPE 		= 4


	-- End Entity Specific Constants and Variables ------------------------------
	SELECT @MODULE_DESC = RTRIM(LocalText) FROM tsmLocalString
	WHERE StringNo = @RPT_STRINGNO_MODULE_DESC AND LanguageID = @LanguageID

	SELECT	@APActivated = Active
	FROM	tsmCompanyModule WITH (NOLOCK)
	WHERE	ModuleNo = @MODULE_AP
	AND 	CompanyID = @iCompanyID

	SELECT	@IMActivated = Active
	FROM	tsmCompanyModule WITH (NOLOCK)
	WHERE	ModuleNo = @MODULE_IM
	AND 	CompanyID = @iCompanyID

	SELECT	@SOActivated = Active
	FROM	tsmCompanyModule WITH (NOLOCK)
	WHERE	ModuleNo = @MODULE_SO
	AND 	CompanyID = @iCompanyID

	SELECT	@ChkCreditLimit = ChkCreditLimit,
			@PrintInvcs = PrintInvcs,
			@UseMultCurr = UseMultCurr,
			@UseSper = UseSper,
			@ARIntegratedWithMC = IntegrateWithCM,
			@UseSTax = TrackSTaxOnSales
	FROM	tarOptions WITH (NOLOCK)
	WHERE	CompanyID = @iCompanyID

	SELECT	@ExtShipSystem = ExtShipSystem
	FROM	tsoOptions WITH (NOLOCK)
	WHERE	CompanyID = @iCompanyID

	-----------------------------------------------------------------------------
	-- Get input parameters from tsmMigrateStepParamValue and tsmMigrateStepParam
	-- These will be unique to the specific migration entity
	-- --------------------------------------------------------------------------

	-- End Entity Specific Input Parameter Block --------------------------------

	-----------------------------------------------------------------------------
	-- Check for cancel action.  Write out log record if any logging is requested
	-----------------------------------------------------------------------------	
	IF @iCancel = 1
	BEGIN
		-- Log cancellation request to report log

			EXEC ciBuildString @RPT_STRINGNO_CANCELLED,@LanguageID,@Message OUTPUT, @RetVal OUTPUT
	
			EXEC spDMCreateMigrationLogEntry	@LanguageID, @iSessionKey,				
			'', '', '', @MIG_STAT_INFO, 0, @Message,
			@RetVal OUTPUT

		-- Return with success when user requests cancel
		SELECT 	@_oRetVal = @RET_SUCCESS
		RETURN
	END


	-----------------------------------------------------------------------------
	-- Check if the module is activated.
	-----------------------------------------------------------------------------
	IF NOT EXISTS (SELECT * FROM tsmCompanyModule WITH (NOLOCK)WHERE Active = 1 AND  ModuleNo = @MODULE_AR AND CompanyID = @iCompanyID)
	BEGIN
		
		-- Log error into report log

			EXEC ciBuildString @RPT_STRINGNO_MODULE_INACTIVE, @LanguageID,@Message OUTPUT,
			@RetVal OUTPUT, @MODULE_DESC, @iCompanyID

			EXEC spDMCreateMigrationLogEntry	@LanguageID, @iSessionKey,				
			'', '', '',
			@MIG_STAT_FAILURE, 0, @Message,
			@RetVal OUTPUT

			-- Return with success when module is not activated.
			SELECT 	@_oRetVal = @RET_SUCCESS
			RETURN

	END

	-----------------------------------------------------------------------------
	-- Check for StgCustSTaxExmpt data vs. UseSystemGeneratedSTax Option.
	-- If the UseSystemGeneratedSTax option is selected, issue warning is there
	-- is data entered in StgCustSTaxExmpt
	--
	-- This will be true also if the company does no track Sales Tax on sales
	-----------------------------------------------------------------------------
	IF @UseSTax = 0
	BEGIN
		IF @iUseStageTable = 1
			SELECT @STaxRecCount =
				(SELECT COUNT(*)
				FROM StgCustSTaxExmpt  WITH (NOLOCK)
				WHERE SessionKey = @iSessionKey)
				
		ELSE
			SELECT @STaxRecCount =
				(SELECT COUNT(*)
				FROM #StgCustSTaxExmpt WITH (NOLOCK)
				WHERE SessionKey = @iSessionKey)

		-- Log warning
		IF @STaxRecCount > 0
		BEGIN
			IF 	@iRptOption = 	@RPT_PRINT_ALL OR
				(@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL AND @RecordHadErrors <> 1) OR
				(@iRptOption =	@RPT_PRINT_SUCCESSFUL AND @RecordHadErrors = 0)
			BEGIN
					EXEC ciBuildString @RPT_STRINGNO_STGDATA_IGNORED,@LanguageID,@Message OUTPUT, @RetVal OUTPUT,
						'StgCustSTaxExmpt', 'Do Not Track Sales Tax on Sales'

				EXEC spdmCreateMigrationLogEntry	@LanguageID, 	@iSessionKey,				
									'', '', '',
									@MIG_STAT_INFO, 0, @Message,
									@RetVal OUTPUT
			END
		END
	END

	-- Initialize the total record count if this is the first call to this sp.
	-- Because this routine will only process @MAX_ROWS_PER_RUN if staging tables
	-- are being used, it may be called more than once.
	IF @oTotalRecs = 0
	BEGIN

		IF @iUseStageTable = 1
		BEGIN
			
			-- Count Stg Customer Records once for the customer record
			SELECT @oTotalRecs = (SELECT COUNT(*)
	FROM StgCustomer WITH (NOLOCK)
	WHERE ProcessStatus = @NOT_PROCESSED
	AND SessionKey = @iSessionKey)

			-- Count StgCustAddr Records
			SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
	FROM StgCustAddr WITH (NOLOCK)
	WHERE ProcessStatus = @NOT_PROCESSED
	AND SessionKey = @iSessionKey)

			-- Count StgContact records
			SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
	 FROM StgContact WITH (NOLOCK)
	WHERE ProcessStatus = @NOT_PROCESSED
	AND SessionKey = @iSessionKey)

			-- Count any Doc Transmit records
			SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
		FROM StgCustDocTrnsmit WITH (NOLOCK)
	                        WHERE ProcessStatus = @NOT_PROCESSED
	                        AND SessionKey = @iSessionKey)

			-- Count any Tax Exemption records
			IF @UseSTax = 1
				SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
		FROM StgCustSTaxExmpt WITH (NOLOCK)
		WHERE ProcessStatus = @NOT_PROCESSED
		AND SessionKey = @iSessionKey)

		END -- End @iUseStageTable = 1
		ELSE
		BEGIN

			-- Count #StgCustomer Record once for the Customer Record
			SELECT @oTotalRecs = (SELECT COUNT(*)
			FROM #StgCustomer WITH (NOLOCK)
			WHERE ProcessStatus = @NOT_PROCESSED
			AND SessionKey = @iSessionKey)
			OPTION (KEEP PLAN)

			-- Count #StgCustAddr Records			
			SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
	FROM #StgCustAddr WITH (NOLOCK)
	WHERE ProcessStatus = @NOT_PROCESSED
	AND SessionKey = @iSessionKey)
			OPTION (KEEP PLAN)

			-- Count #StgContact Records
			SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
	FROM #StgContact WITH (NOLOCK)
	WHERE ProcessStatus = @NOT_PROCESSED
		AND SessionKey = @iSessionKey)
			OPTION (KEEP PLAN)

			-- Count any Doc Transmit records
			SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
		FROM #StgCustDocTrnsmit WITH (NOLOCK)
	                        WHERE ProcessStatus = @NOT_PROCESSED
	                        AND SessionKey = @iSessionKey)
			OPTION (KEEP PLAN)

			-- Count any Tax Exemption records
			IF @UseSTax = 1
				SELECT @oTotalRecs = @oTotalRecs + (SELECT COUNT(*)
			FROM #StgCustSTaxExmpt WITH (NOLOCK)
								WHERE ProcessStatus = @NOT_PROCESSED
								AND SessionKey = @iSessionKey)
				OPTION (KEEP PLAN)

	END -- End @iUseStageTable <> 1

	END -- End @oTotalRecs = 0

	-- Set up @MAX_ROWS_PER_RUN based on the TotalRecs
	SELECT @BLOCK_PROCESS_SIZE =
		   CASE WHEN @oTotalRecs <= 250   THEN 2
			WHEN @oTotalRecs <= 500   THEN 10
			WHEN @oTotalRecs <= 2500  THEN 50
			WHEN @oTotalRecs <= 10000 THEN 200
			WHEN @oTotalRecs <= 50000 THEN 1000
			ELSE 2500
		   END
	
	SELECT @SourceRowCount = 0

	IF @iUseStageTable = 1
	BEGIN
		-- If the caller wants me to use the staging table, then I'll create my own copy of the temporary tables
		-- (if they haven't already been created).
		IF OBJECT_ID('tempdb..#StgCustomer') IS NOT NULL
		BEGIN
			SELECT @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
		END
		ELSE
		BEGIN
			SELECT @TempTableCreation = @CREATED_INSIDE_THIS_SP
			SELECT * INTO #StgCustomer FROM StgCustomer WITH (NOLOCK) WHERE 1 = 2
		END

		-- create the other temp tables if neccessary

		IF OBJECT_ID('tempdb..#StgCustAddr') IS NULL
			SELECT * INTO #StgCustAddr FROM StgCustAddr WITH (NOLOCK) WHERE 1=2
	
		IF OBJECT_ID('tempdb..#StgContact') IS NULL
			SELECT * INTO #StgContact FROM StgContact WITH (NOLOCK) WHERE 1=2

		IF OBJECT_ID('tempdb..#StgCustDocTrnsmit') IS NULL
			SELECT * INTO #StgCustDocTrnsmit FROM StgCustDocTrnsmit WITH (NOLOCK) WHERE 1=2

		IF OBJECT_ID('tempdb..#StgCustSTaxExmpt') IS NULL
			SELECT * INTO #StgCustSTaxExmpt FROM StgCustSTaxExmpt WITH (NOLOCK) WHERE 1=2

	END -- End @iUseStageTable = 1
	ELSE
	BEGIN
		SELECT @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
	END -- End @iUseStageTable <> 1

	-- Add a clustered index on #StgCustomer.RowKey

	EXEC spDMCreateTmpTblIndex '#StgCustomer', 'RowKey', @RetVal OUTPUT

	IF @RetVal <> @RET_SUCCESS OR @@ERROR < > 0
	BEGIN
		SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
	END


	-- add a clustered index on #StgCustAddr.RowKey
	EXEC spDMCreateTmpTblIndex '#StgCustAddr', 'RowKey', @RetVal OUTPUT

	IF @RetVal <> @RET_SUCCESS OR @@ERROR < > 0
	BEGIN
		SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
	END

	-----------------------------------------------------------------------------------------------
	-- Please do not remove next line (comment), it's required for the data replication because of
	-- a bug on the SQL Server 2005 replication engine. See Scopus 37655. 06/20/07
	-- CREATE CLUSTERED INDEX #StgCustAddr_ID ON #StgCustAddr(CustID)
	-----------------------------------------------------------------------------------------------
	-- create a non clustered index on #StgCustAddr.CustID
	IF NOT EXISTS (SELECT * FROM tempdb..sysindexes
	WHERE id = object_id('tempdb..#StgCustAddr') AND name = '#StgCustAddr_ID')
	BEGIN
		CREATE NONCLUSTERED INDEX #StgCustAddr_ID ON #StgCustAddr(CustID)
	END

	-- add a clustered index #StgContact.RowKey
	EXEC spDMCreateTmpTblIndex '#StgContact', 'RowKey', @RetVal OUTPUT

	IF @RetVal <> @RET_SUCCESS OR @@ERROR < > 0
	BEGIN
		SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
	END

	-- add a clustered index on #StgCustDocTrnsmit.RowKey
	EXEC spDMCreateTmpTblIndex '#StgCustDocTrnsmit', 'RowKey', @RetVal OUTPUT

	IF @RetVal <> @RET_SUCCESS OR @@ERROR < > 0
	BEGIN
		SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
	END

	
	IF NOT EXISTS (SELECT * FROM tempdb..sysindexes WITH (NOLOCK)
	WHERE id = object_id('tempdb..#StgCustDocTrnsmit') AND name = '#StgCustDocTrnsmit_ID')
	BEGIN
		CREATE NONCLUSTERED INDEX #StgCustDocTrnsmit_ID ON #StgCustDocTrnsmit(CustID)
	END

	EXEC spDMCreateTmpTblIndex '#StgCustSTaxExmpt', 'RowKey', @RetVal OUTPUT

	IF @RetVal <> @RET_SUCCESS OR @@ERROR < > 0
	BEGIN
		SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
	END


	IF NOT EXISTS (SELECT * FROM tempdb..sysindexes WITH (NOLOCK)
	WHERE id = object_id('tempdb..#StgCustSTaxExmpt') AND name = '#StgCustSTaxExmpt_ID')
	BEGIN
		CREATE NONCLUSTERED INDEX #StgCustSTaxExmpt_ID ON #StgCustSTaxExmpt(CustID)
	END


	-- get invalid reference blank option
	-- This will determine if invalid ID references should fail migration

	-- @BlankInvalidReference ;
	-- 0=leave invalid reference and Do Not Migrate record;
	-- 1=replace invalid VendID with NULL and Migrate record
	SELECT @BlankInvalidReference = CONVERT(SMALLINT, pv.ParamValue)
	FROM tsmMigrateStepParam sp WITH (NOLOCK)
	INNER JOIN tsmMigrateStepParamValue pv ON pv.SetupStepKey = sp.SetupStepKey AND pv.ParamNo = sp.ParamNo
	WHERE pv.MigrateSessionKey = @iSessionKey AND sp.ParamType = @PARAM_REFERENCE
	OPTION(KEEP PLAN)

	--SELECT '@BlankInvalidReference = ', @BlankInvalidReference
	-- For this build only -- remove after reference
	SELECT @BlankInvalidReference = COALESCE(@BlankInvalidReference, 1)

	SELECT @InvalidGLUseSuspense = CONVERT(SMALLINT, pv.ParamValue)
	FROM tsmMigrateStepParam sp WITH (NOLOCK)
	LEFT JOIN tsmMigrateStepParamValue pv ON pv.SetupStepKey = sp.SetupStepKey AND pv.ParamNo = sp.ParamNo
	WHERE pv.MigrateSessionKey = @iSessionKey AND sp.ParamType = @INVALIDGL_PARM_TYPE
	OPTION(KEEP PLAN)

	-----------------------------------------------------------------------
	-- Get the GL Suspense account to use if GL default flag is set to true
	-----------------------------------------------------------------------
	SELECT 	@SuspenseAcctKey = SuspenseAcctKey
	FROM 	tglOptions WITH (NOLOCK)
	WHERE	CompanyID = @iCompanyID
	
	-----------------------------------------------------------------------
	-- Start of response time control loop.  Attempts to set the appoximate
	-- response time to @MIGRATE_SECS.
	-----------------------------------------------------------------------

	-- reset running counts for rows and failures
	SELECT @RowsProcThisCall =0, @lRecFailed = 0

	-- start the timing loop
	SELECT @StartDate = GetDate()
	-- added AND @SourceRowCount to continue until no more rows need processing
	SELECT @SourceRowCount = 1
	WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS AND @SourceRowCount > 0
	BEGIN
	
		-- Populate temp tables IF using Staging tables.
		IF @iUseStageTable = 1
		BEGIN
	
			TRUNCATE TABLE #StgCustomer

			IF @TempTableCreation = @CREATED_OUTSIDE_THIS_SP
			BEGIN
				SET ROWCOUNT @BLOCK_PROCESS_SIZE
			END
			ELSE
			BEGIN
				SET ROWCOUNT 0
			END
		
			-- Populate #StgCustomer
			SET IDENTITY_INSERT #StgCustomer ON

			INSERT #StgCustomer

			(RowKey,ABANo,AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName,AllowCustRefund,AllowInvtSubst
			,AllowWriteOff,BillingType,City,CommPlanID,CountryID,DfltSalesReturnAcctNo, CloseSOLineOnFirstShip, CloseSOOnFirstShip
			,CreditLimit,CreditLimitAgeCat,CreditLimitUsed,CRMCustID,CurrExchSchdID,CurrID,CustClassID,CustID
			,CustName,CustPriceGroupID,CustRefNo,DateEstab,DfltBillToAddrID,DfltItemID,DfltSalesAcctNo,DfltShipToAddrID
			,EMailAddr,Fax,FaxExt,FinChgFlatAmt,FinChgPct,FOBID,Hold,InvcFormID,InvcMsg,LanguageID,Name,PackListFormID
			,Phone,PhoneExt,PmtTermsID,PostalCode,PriceBase,PrintDunnMsg,PrintOrderAck
			,ReqPO,RequireSOAck,SalesSourceID,SalesTerritoryID,ShipComplete,ShipDays,ShipLabelFormID
			,ShipMethID,ShipPriority,SOAckFormID,SperID,StateID,Status,STaxSchdID,StdIndusCodeID
			,StmtCycleID,StmtFormID,Title,TradeDiscPct,UserFld1,UserFld2,UserFld3,UserFld4
			,VendID,WhseID,BOLReqd,InvoiceReqd,PackListReqd,PackListContentsReqd,Residential,ShipLabelsReqd,CarrierAcctNo
			,CarrierBillMeth,FreightMethod,ProcessStatus,PriceAdj,SessionKey,Action,CRMContactID,CRMAddrID)
			SELECT RowKey,ABANo,AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName,AllowCustRefund,AllowInvtSubst
			,AllowWriteOff,BillingType,City,CommPlanID,CountryID,DfltSalesReturnAcctNo, CloseSOLineOnFirstShip, CloseSOOnFirstShip
			,CreditLimit,CreditLimitAgeCat,CreditLimitUsed,CRMCustID,CurrExchSchdID,CurrID,CustClassID,CustID
			,CustName,CustPriceGroupID,CustRefNo,COALESCE(DateEstab,@UserBusinessDate),DfltBillToAddrID,DfltItemID,DfltSalesAcctNo,DfltShipToAddrID
			,EMailAddr,Fax,FaxExt,FinChgFlatAmt,FinChgPct,FOBID,Hold,InvcFormID,InvcMsg,LanguageID,Name,PackListFormID
			,Phone,PhoneExt,PmtTermsID,PostalCode,PriceBase,PrintDunnMsg,PrintOrderAck
			,ReqPO,RequireSOAck,SalesSourceID,SalesTerritoryID,ShipComplete,ShipDays,ShipLabelFormID
			,ShipMethID,ShipPriority,SOAckFormID,SperID,StateID,Status,STaxSchdID,StdIndusCodeID
			,StmtCycleID,StmtFormID,Title,TradeDiscPct,UserFld1,UserFld2,UserFld3,UserFld4
			,VendID,WhseID,BOLReqd,InvoiceReqd,PackListReqd,PackListContentsReqd,Residential,ShipLabelsReqd
			,CarrierAcctNo,CarrierBillMeth,FreightMethod,ProcessStatus,coalesce(PriceAdj,0),SessionKey, Action,CRMContactID,CRMAddrID
			FROM 	StgCustomer WITH (NOLOCK)
			WHERE 	ProcessStatus = @NOT_PROCESSED AND SessionKey = @iSessionKey
			OPTION(KEEP PLAN)			

			SET IDENTITY_INSERT #StgCustomer OFF
			
			SET ROWCOUNT 0
		END --	IF @iUseStageTable = 1

		--UPDATE Action flag to 'Insert' if it is null
		UPDATE 	#StgCustomer
		SET	Action = @REC_INSERT
		WHERE Action IS NULL
		AND SessionKey = @iSessionKey
		OPTION(KEEP PLAN)

		--UPDATE Action flag if it is specified as Automatic
		--Set Action to 'Update' if record already exists in database
		--Otherwise, set the Action to 'Insert'.
		UPDATE 	#StgCustomer
		SET	Action = CASE WHEN c.CustID IS NOT NULL THEN @REC_UPDATE
					ELSE @REC_INSERT END
		FROM	#StgCustomer
		LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
		ON	#StgCustomer.CustID = c.CustID
		AND	c.CompanyID = @iCompanyID
		WHERE #StgCustomer.Action = @REC_AUTO
		OPTION(KEEP PLAN)

		--Check for duplication(in #StgCustomer temp table)
		UPDATE #StgCustomer SET ProcessStatus = @GENRL_PROC_ERR
			FROM #StgCustomer a
			WHERE 1 < SOME (SELECT COUNT(*) FROM #StgCustomer GROUP BY CustID, SessionKey
					HAVING SessionKey = @iSessionKey AND CustID = a.CustID)
				AND a.RowKey  <> (SELECT MIN(RowKey) FROM #StgCustomer WHERE CustID = a.CustID AND SessionKey = @iSessionKey)
				AND a.SessionKey = @iSessionKey
				OPTION (KEEP PLAN)

		--REPORT Duplicates if print warnings
		IF @iPrintWarnings = 1 AND EXISTS(SELECT * FROM #StgCustomer WHERE ProcessStatus = @GENRL_PROC_ERR AND SessionKey = @iSessionKey)
		BEGIN
			-- set up strings
			SELECT @StringNo = @RPT_STRINGNO_DUPLICATE

			EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT

			-- insert to error temp table
			INSERT #tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Comment,Duplicate,
				EntityID,SessionKey,Status)
			SELECT 'CustID', CustID, @Message, 1,
				COALESCE(CustID,''), @iSessionKey,@MIG_STAT_WARNING
			FROM #StgCustomer  WITH (NOLOCK) WHERE
				ProcessStatus = @GENRL_PROC_ERR
				 AND SessionKey = @iSessionKey
				OPTION (KEEP PLAN)

			-- call to create log entries and truncate temp table when finished
			EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT
			TRUNCATE TABLE #tdmMigrationLogEntryWrk

			-- reset variables
			SELECT @StringNo = NULL, @Message = NULL
		END

		IF @iUseStageTable = 1
		BEGIN

			-- Update the status for the Perm StgCustomer table
			UPDATE StgCustomer SET ProcessStatus = @GENRL_PROC_ERR
			FROM StgCustomer C WITH (NOLOCK)
			INNER JOIN #StgCustomer SC WITH (NOLOCK)
			ON C.RowKey = SC.RowKey
			WHERE SC.ProcessStatus = @GENRL_PROC_ERR
			AND SC.SessionKey = @iSessionKey
			OPTION (KEEP PLAN)
	
			-- #StgCustAddr -- Join #StgCustomer to bring in only children
			-- DfltBillToAddr AND DfltShipToAddr
			TRUNCATE TABLE #StgCustAddr

			SET IDENTITY_INSERT #StgCustAddr ON

			INSERT #StgCustAddr
			(RowKey,AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName,AllowInvtSubst,City
			,CloseSOLineOnFirstShip, CloseSOOnFirstShip,CommPlanID,CountryID,CurrExchSchdID,CurrID,CustAddrID,CustID,CustPriceGroupID,EMailAddr,Fax
			,FaxExt,FOBID,InvcFormID,InvcMsg,LanguageID,Name,PackListFormID,Phone,PhoneExt,PmtTermsID
			,PostalCode,PriceBase,PrintOrderAck,RequireSOAck,SalesTerritoryID,ShipComplete,ShipDays,ShipLabelFormID
			,ShipMethID,SOAckFormID,SperID,StateID,STaxSchdID,Title,WhseID
			,BOLReqd,InvoiceReqd,PackListReqd,PackListContentsReqd,Residential,ShipLabelsReqd,CarrierAcctNo
			,CarrierBillMeth,FreightMethod,ProcessStatus,PriceAdj,SessionKey, Action, CRMContactID, CRMAddrID)
			SELECT CA.RowKey,CA.AddrLine1,CA.AddrLine2,CA.AddrLine3,CA.AddrLine4,CA.AddrLine5,CA.AddrName,CA.AllowInvtSubst,CA.City
			,CA.CloseSOLineOnFirstShip, CA.CloseSOOnFirstShip,CA.CommPlanID,CA.CountryID,CA.CurrExchSchdID,CA.CurrID,CA.CustAddrID,CA.CustID,CA.CustPriceGroupID,CA.EMailAddr,CA.Fax
			,CA.FaxExt,CA.FOBID,CA.InvcFormID,CA.InvcMsg,CA.LanguageID,CA.Name,CA.PackListFormID,CA.Phone,CA.PhoneExt,CA.PmtTermsID
			,CA.PostalCode,CA.PriceBase,CA.PrintOrderAck,CA.RequireSOAck,CA.SalesTerritoryID,CA.ShipComplete,CA.ShipDays,CA.ShipLabelFormID
			,CA.ShipMethID,CA.SOAckFormID,CA.SperID,CA.StateID,CA.STaxSchdID,CA.Title,CA.WhseID
			,CA.BOLReqd,CA.InvoiceReqd,CA.PackListReqd,CA.PackListContentsReqd,CA.Residential,CA.ShipLabelsReqd,CA.CarrierAcctNo
			,CA.CarrierBillMeth,CA.FreightMethod,CA.ProcessStatus,Coalesce(CA.PriceAdj,0),CA.SessionKey, CA.Action, CA.CRMContactID, CA.CRMAddrID
			FROM StgCustAddr CA WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON CA.CustID = C.CustID
			WHERE CA.SessionKey = @iSessionKey
			AND C.SessionKey = @iSessionKey
			AND CA.ProcessStatus = @NOT_PROCESSED
			AND C.ProcessStatus = @NOT_PROCESSED
			AND((CA.CustAddrID = C.DfltBillToAddrID AND C.DfltBillToAddrID <> C.CustID)
			OR (CA.CustAddrID = C.DfltShipToAddrID AND C.DfltShipToAddrID <> C.CustID))
			OPTION (KEEP PLAN)

			SET IDENTITY_INSERT #StgCustAddr OFF

			-- #StgCustDocTrnsmit -- Join #StgCustomer to bring in only children
			TRUNCATE TABLE #StgCustDocTrnsmit

			SET IDENTITY_INSERT #StgCustDocTrnsmit ON

			INSERT #StgCustDocTrnsmit
			(RowKey,CustID,EMail,EMailFormat,Fax,HardCopy,TranType,ProcessStatus,SessionKey,Action)
			SELECT CDT.RowKey,CDT.CustID,CDT.EMail,CDT.EMailFormat,CDT.Fax,CDT.HardCopy
			,CDT.TranType,CDT.ProcessStatus,CDT.SessionKey, Action
			FROM StgCustDocTrnsmit CDT WITH (NOLOCK)
			INNER JOIN (SELECT DISTINCT CustID, SessionKey, ProcessStatus
				FROM #StgCustomer WITH (NOLOCK) WHERE Action = @REC_INSERT) C
			ON CDT.CustID = C.CustID
			WHERE CDT.SessionKey = @iSessionKey
			AND C.SessionKey = @iSessionKey
			AND C.ProcessStatus = @NOT_PROCESSED
			OPTION (KEEP PLAN)

			SET IDENTITY_INSERT #StgCustDocTrnsmit OFF

		END

		--UPDATE Action flag to 'Insert' if it is null
		UPDATE 	#StgCustAddr
		SET	Action = @REC_INSERT
		WHERE Action IS NULL
		AND SessionKey = @iSessionKey
		OPTION(KEEP PLAN)

		--UPDATE Action flag if it is specified as Automatic
		--Set Action to 'Update' if record already exists in database
		--Otherwise, set the Action to 'Insert'.
		UPDATE 	#StgCustAddr
		SET	Action = CASE WHEN ca.CustAddrID IS NOT NULL THEN @REC_UPDATE
					ELSE @REC_INSERT END
		FROM	#StgCustAddr
		LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
		ON	#StgCustAddr.CustID = c.CustID
		AND	c.CompanyID = @iCompanyID
		LEFT OUTER JOIN tarCustAddr ca WITH (NOLOCK)
		ON	c.CustKey = ca.CustKey
		AND	#StgCustAddr.CustAddrID = ca.CustAddrID		
		WHERE 	#StgCustAddr.Action = @REC_AUTO
		OPTION (KEEP PLAN)

		-- populate the StgCustDocTrnsmit temp table with the migrated keys
		-- to keep track of which rows are being migrated vs. generated
		TRUNCATE TABLE #tarCustDocTrnsmitWrk
		INSERT #tarCustDocTrnsmitWrk (RowKey, ProcessStatus)
		SELECT RowKey,ProcessStatus FROM #StgCustDocTrnsmit

		IF @SourceRowCount <> 0
		BEGIN
	
			----------------------------------------------------------------------------
			-- Start of primary logic.
			-- We will process Customer in Chunks.
	
			--
			-- Process the rows until we're "done", where done either means we ran out
			-- of rows or we were told to stop.  If the original data was obtained
			-- from staging tables, then we will only process up to the maximum number
			-- of rows that will result in decent response time.  If the data was
			-- passed in the temporary tables directly, then we will process all rows.
			-----------------------------------------------------------------------------
	
			-- Insert default values for the temporary table columns containing null, where the
			-- corresponding permanent table column has a default rule.
			EXEC spDMTmpTblDfltUpd '#StgCustomer', 'tciAddress', @RetVal OUTPUT, @ACTION_COL_USED
			EXEC spDMTmpTblDfltUpd '#StgCustAddr', 'tciAddress', @RetVal OUTPUT, @ACTION_COL_USED

			-- Call to setup and Validate DocTrnsmit table
			EXEC spARCustDocTrnsmitVal @iSessionKey, @iCompanyID, @LanguageID, @RetVal OUTPUT

	
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- convert the languageID from description to ID
			UPDATE #StgCustomer
			SET LanguageID = CONVERT(VARCHAR(25),L.LanguageID)
			FROM #StgCustomer C WITH (NOLOCK)
			INNER JOIN tsmLanguage L WITH (NOLOCK)
			ON LTRIM(RTRIM(C.LanguageID)) = LTRIM(RTRIM(L.LanguageDesc))
			WHERE NULLIF(RTRIM(LTRIM(C.LanguageID)),'') IS NOT NULL
			OPTION(KEEP PLAN)

			-- have to validate LanguageID now
			IF EXISTS(SELECT * FROM #StgCustomer WHERE
				LanguageID NOT IN (SELECT CONVERT(VARCHAR(25),LanguageID) FROM tsmLanguage)
				AND NULLIF(LTRIM(RTRIM(LanguageID)),'') IS NOT NULL)
			BEGIN

				IF @iPrintWarnings = 1
				BEGIN


					--SELECT @StringNo = @RPT_STRINGNO_DUPLICATE
	
					--EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT--, 'CustAddrID'

					SELECT @Message = 'LanguageID was invalid and was replaced with English [1033]'
	
					-- insert into temp log table
					INSERT #tdmMigrationLogEntryWrk
						(ColumnID,ColumnValue,Comment,Duplicate,
						EntityID,SessionKey,Status)
					SELECT 'LanguageID', LanguageID, @Message, 0,
						LTRIM(RTRIM(CustID)), @iSessionKey,@MIG_STAT_WARNING
					FROM #StgCustomer  WITH (NOLOCK) WHERE
						 LanguageID NOT IN (SELECT CONVERT(VARCHAR(25),LanguageID) FROM tsmLanguage)
						AND NULLIF(LTRIM(RTRIM(LanguageID)),'') IS NOT NULL
						OPTION (KEEP PLAN)
	
					-- call to add log entries
					EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

					TRUNCATE TABLE #tdmMigrationLogEntryWrk
					-- reset variables
					SELECT @StringNo = NULL, @Message = NULL

				END
	
				UPDATE #StgCustomer SET LanguageID = NULL WHERE
				LanguageID NOT IN(SELECT CONVERT(VARCHAR(25),LanguageID) FROM tsmLanguage)
				AND NULLIF(LTRIM(RTRIM(LanguageID)),'') IS NOT NULL
		
			END

			-- convert the languageID from description to ID
			UPDATE #StgCustAddr SET LanguageID = CONVERT(VARCHAR(25),L.LanguageID)
			FROM #StgCustAddr C WITH (NOLOCK)
			INNER JOIN tsmLanguage L WITH (NOLOCK)
			ON LTRIM(RTRIM(C.LanguageID)) = LTRIM(RTRIM(L.LanguageDesc))
			WHERE NULLIF(LTRIM(RTRIM(C.LanguageID)),'') IS NOT NULL
			OPTION(KEEP PLAN)			

			-- have to validate LanguageID
			IF EXISTS(SELECT * FROM #StgCustAddr WHERE
			LanguageID NOT IN (SELECT CONVERT(VARCHAR(25),LanguageID) FROM tsmLanguage)
			AND NULLIF(LTRIM(RTRIM(LanguageID)),'') IS NOT NULL)
			BEGIN

				IF @iPrintWarnings = 1
				BEGIN

					--SELECT @StringNo = @RPT_STRINGNO_DUPLICATE
	
					--EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT--, 'CustAddrID'

					SELECT @Message = 'LanguageID was invalid and was replaced with English [1033]'
	
					-- insert into temp log table
					INSERT #tdmMigrationLogEntryWrk
						(ColumnID,ColumnValue,Comment,Duplicate,
						EntityID,SessionKey,Status)
					SELECT 'LanguageID', LanguageID, @Message, 0,
						COALESCE(LTRIM(RTRIM(CustID)),'') + '|' + LTRIM(RTRIM(CustAddrID)), @iSessionKey,@MIG_STAT_WARNING
					FROM #StgCustAddr  WITH (NOLOCK) WHERE
						 LanguageID NOT IN (SELECT CONVERT(VARCHAR(25),LanguageID) FROM tsmLanguage)
						AND NULLIF(LTRIM(RTRIM(LanguageID)),'') IS NOT NULL
						OPTION (KEEP PLAN)
	
					-- call to add log entries
					EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

					TRUNCATE TABLE #tdmMigrationLogEntryWrk
					-- reset variables
					SELECT @StringNo = NULL, @Message = NULL

				END
	
				UPDATE #StgCustAddr SET LanguageID = NULL WHERE
				LanguageID NOT IN(SELECT CONVERT(VARCHAR(25),LanguageID) FROM tsmLanguage)
				AND NULLIF(LTRIM(RTRIM(LanguageID)),'') IS NOT NULL
		
			END
			

			-- Replace yes/no fields with 1/0
			EXEC spDMYesNoCodeUpd '#StgCustomer','StgCustomer', @RetVal  OUTPUT

			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END	
				
			-------------------------------------------------------------------------------
			-- update null/blank fields from tarcustclass before DB default values
			-------------------------------------------------------------------------------
			-- update yes/no, numeric, and static data fields in StgCustomer	
			UPDATE #StgCustomer SET
			AllowCustRefund = COALESCE(NULLIF(LTRIM(RTRIM(SC.AllowCustRefund)),''),CONVERT(VARCHAR(1),CC.AllowCustRefund)),
			AllowInvtSubst = COALESCE(NULLIF(LTRIM(RTRIM(SC.AllowInvtSubst)),''),CONVERT(VARCHAR(1),CC.AllowInvtSubst)),
			AllowWriteOff = COALESCE(NULLIF(LTRIM(RTRIM(SC.AllowWriteOff)),''),CONVERT(VARCHAR(1),CC.AllowWriteOff)),
			BillingType = COALESCE(NULLIF(LTRIM(RTRIM(SC.BillingType)),''),CONVERT(VARCHAR(1),CC.BillingType)),
			CreditLimit = CASE WHEN SC.CreditLimitUsed = 1 OR @ChkCreditLimit = 1
					THEN COALESCE(SC.CreditLimit, CC.CreditLimit)
					ELSE 0 END,
			CreditLimitAgeCat = CASE WHEN @ChkCreditLimit = 1
					THEN COALESCE(LTRIM(RTRIM(SC.CreditLimitAgeCat)),CONVERT(VARCHAR(25),CC.CreditLimitAgeCat))
					ELSE '0' END,
			CreditLimitUsed = CASE WHEN @ChkCreditLimit = 1 AND SC.CreditLimitUsed IS NULL
					THEN 1
					ELSE SC.CreditLimitUsed END,
			CurrID = COALESCE(SC.CurrID,CC.CurrID),
			FinChgFlatAmt = COALESCE(SC.FinChgFlatAmt,CC.FinChgFlatAmt),
			FinChgPct = COALESCE(SC.FinChgPct,CONVERT(DECIMAL(5,2),CC.FinChgPct * 100)),
			InvcMsg = COALESCE(SC.InvcMsg, CC.InvcMsg),
			LanguageID = COALESCE(SC.LanguageID, CC.LanguageID),
			PrintDunnMsg = COALESCE(NULLIF(LTRIM(RTRIM(SC.PrintDunnMsg)),''),CONVERT(VARCHAR(1),CC.PrintDunnMsg)),
			ReqPO = COALESCE(NULLIF(LTRIM(RTRIM(SC.ReqPO)),''),CONVERT(VARCHAR(1),CC.ReqPO)),
			RequireSOAck = COALESCE(NULLIF(LTRIM(RTRIM(SC.RequireSOAck)),''),CONVERT(VARCHAR(1),CC.RequireSOAck)),
			ShipComplete = COALESCE(NULLIF(LTRIM(RTRIM(SC.ShipComplete)),''), CONVERT(VARCHAR(1),CC.ShipComplete)),
			TradeDiscPct = COALESCE(SC.TradeDiscPct,CONVERT(DECIMAL(5,2),CC.TradeDiscPct * 100)),
			UserFld1 = COALESCE(SC.UserFld1, CC.UserFld1),
			UserFld2 = COALESCE(SC.UserFld2, CC.UserFld2),
			UserFld3 = COALESCE(SC.UserFld3, CC.UserFld3),
			UserFld4 = COALESCE(SC.UserFld4, CC.UserFld4)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			WHERE CC.CompanyID = @iCompanyID
			AND  SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			AND SC.ProcessStatus = @NOT_PROCESSED
			OPTION (KEEP PLAN)

			-- update yes/no, numeric, and static data fields in StgCustAddr
			-- first from the primary address, then from the CustClass
			UPDATE #StgCustAddr SET
			AllowInvtSubst = COALESCE(NULLIF(LTRIM(RTRIM(SC.AllowInvtSubst)),''),NULLIF(LTRIM(RTRIM(C.AllowInvtSubst)),''),CONVERT(VARCHAR(1),CC.AllowInvtSubst)),
			CurrID = COALESCE(SC.CurrID,C.CurrID,CC.CurrID),
			LanguageID = COALESCE(SC.LanguageID, C.LanguageID, CC.LanguageID),
			RequireSOAck = COALESCE(NULLIF(LTRIM(RTRIM(SC.RequireSOAck)),''),NULLIF(LTRIM(RTRIM(C.RequireSOAck)),''),CONVERT(VARCHAR(1),CC.RequireSOAck)),
			ShipComplete = COALESCE(NULLIF(LTRIM(RTRIM(SC.ShipComplete)),''), NULLIF(LTRIM(RTRIM(C.ShipComplete)),''), CONVERT(VARCHAR(1),CC.ShipComplete))
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			WHERE CC.CompanyID = @iCompanyID
			AND SC.Action = @REC_INSERT
			AND  SC.SessionKey = @iSessionKey
			OPTION (KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			--------------------------------------------------------------
			-- Update IDs in StgCustomer from keys tarCustClass
			--------------------------------------------------------------
			-- update DfltSalesAcct No from tarCustClass		
			UPDATE #StgCustomer SET DfltSalesAcctNo =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.DfltSalesAcctNo)),''),A.GLAcctNo)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tglAccount A WITH (NOLOCK)
			ON CC.DfltSalesAcctKey = A.GLAcctKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

	
			-- Update StmtFormID from tarCustClass 		
			UPDATE #StgCustomer SET StmtFormID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.StmtFormID)),''),BF.BusinessFormID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciBusinessForm BF
			ON CC.StmtFormKey = BF.BusinessFormKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END


			-- Update InvoiceFormID from tarCustClass 		
			UPDATE #StgCustomer SET InvcFormID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.InvcFormID)),''),BF.BusinessFormID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciBusinessForm BF
			ON CC.InvcFormKey = BF.BusinessFormKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update ShipLabelID from tarCustClass 		
			UPDATE #StgCustomer SET ShipLabelFormID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.ShipLabelFormID)),''),BF.BusinessFormID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciBusinessForm BF
			ON CC.ShipLabelFormKey = BF.BusinessFormKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update SOAckFormID from tarCustClass 		
			UPDATE #StgCustomer SET SOAckFormID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.SOAckFormID)),''),BF.BusinessFormID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciBusinessForm BF
			ON CC.SOAckFormKey = BF.BusinessFormKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update PmtTermsID from tarCustClass 		
			UPDATE #StgCustomer SET PmtTermsID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.PmtTermsID)),''),PT.PmtTermsID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciPaymentTerms PT
			ON CC.PmtTermsKey = PT.PmtTermsKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update FOBID from tarCustClass 		
			UPDATE #StgCustomer SET FOBID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.FOBID)),''),F.FOBID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciFOB F
			ON CC.FOBKey = F.FOBKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update ShipMeth from tarCustClass		
			UPDATE #StgCustomer SET ShipMethID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.ShipMethID)),''),SM.ShipMethID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciShipMethod SM
			ON CC.ShipMethKey = SM.ShipMethKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update STaxSchd from tarCustClass		
			UPDATE #StgCustomer SET STaxSchdID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.STaxSchdID)),''),ST.STaxSchdID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciSTaxSchedule ST
			ON CC.STaxSchdKey = ST.STaxSchdKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)	
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END


			-- Update Statement Cycle from tarCustClass		
			UPDATE #StgCustomer SET StmtCycleID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.StmtCycleID)),''),PC.ProcCycleID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tciProcCycle PC
			ON CC.StmtCycleKey = PC.ProcCycleKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)	
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END
	
			--Update SperID from tarCustClass
			UPDATE #StgCustomer SET SperID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.SperID)),''),S.SperID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = CC.CustClassID
			INNER JOIN tarSalesperson S WITH (NOLOCK)
			ON CC.SperKey = S.SperKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
		
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			--------------------------------------------------------------
			-- Update IDs in StgCustAddr from keys tarCustClass
			--------------------------------------------------------------
			-- Update FOBID from tarCustClass 		
			UPDATE #StgCustAddr SET FOBID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.FOBID)),''),F.FOBID)
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			INNER JOIN tciFOB F
			ON CC.FOBKey = F.FOBKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END


			-- Update InvoiceFormID from tarCustClass 		
			UPDATE #StgCustAddr SET InvcFormID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.InvcFormID)),''),BF.BusinessFormID)
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			INNER JOIN tciBusinessForm BF
			ON CC.InvcFormKey = BF.BusinessFormKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update PmtTermsID from tarCustClass 		
			UPDATE #StgCustAddr SET PmtTermsID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.PmtTermsID)),''),PT.PmtTermsID)
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			INNER JOIN tciPaymentTerms PT
			ON CC.PmtTermsKey = PT.PmtTermsKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update ShipLabelID from tarCustClass 		
			UPDATE #StgCustAddr SET ShipLabelFormID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.ShipLabelFormID)),''),BF.BusinessFormID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			INNER JOIN tciBusinessForm BF
			ON CC.ShipLabelFormKey = BF.BusinessFormKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END


			-- Update ShipMeth from tarCustClass		
			UPDATE #StgCustAddr SET ShipMethID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.ShipMethID)),''),SM.ShipMethID)
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			INNER JOIN tciShipMethod SM
			ON CC.ShipMethKey = SM.ShipMethKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- Update SOAckFormID from tarCustClass 		
			UPDATE #StgCustAddr SET SOAckFormID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.SOAckFormID)),''),BF.BusinessFormID)
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			INNER JOIN tciBusinessForm BF
			ON CC.SOAckFormKey = BF.BusinessFormKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			--Update SperID from tarCustClass
			UPDATE #StgCustAddr SET SperID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.SperID)),''),S.SperID)
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON C.CustClassID = CC.CustClassID
			INNER JOIN tarSalesperson S WITH (NOLOCK)
			ON CC.SperKey = S.SperKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)
		
			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END


			-- Update STaxSched from #StgCustomer/tarCustClass tarCustAddr
			UPDATE #StgCustAddr SET STaxSchdID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.STaxSchdID)),''), NULLIF(RTRIM(LTRIM(C.STaxSchdID)),''), ST.STaxSchdID)
			FROM #StgCustomer SC WITH (NOLOCK)
			INNER JOIN #StgCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			AND C.ProcessStatus = @NOT_PROCESSED
			INNER JOIN tarCustClass CC WITH (NOLOCK)
			ON SC.CustClassID = C.CustClassID
			INNER JOIN tciSTaxSchedule ST
			ON CC.STaxSchdKey = ST.STaxSchdKey
			WHERE CC.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			AND SC.Action = @REC_INSERT
			OPTION(KEEP PLAN)

			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-------------------------------------------------------------
			-- add database defaults for the #StgCustomer table
			-------------------------------------------------------------
			-- add StgCustomer defaults
			EXEC spDMTmpTblDfltUpd '#StgCustomer', 'tarCustomer', @RetVal OUTPUT, @ACTION_COL_USED
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- add StgCustAddr defaults
			EXEC spDMTmpTblDfltUpd '#StgCustomer', 'tarCustAddr', @RetVal OUTPUT, @ACTION_COL_USED
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END	
	
			-- Strip out common Phone Number formatting
			EXEC spDMStrMaskStrip  '#StgCustomer', 'tciAddress',  @RetVal  OUTPUT
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END	
	
			-- Remove masks on the GL accts
			EXEC spDMGLAcctMaskStrip '#StgCustomer', 'StgCustomer', @RetVal  OUTPUT
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			------------------------------------------------------------------
			-- Replace list values with DBValue from tsmListValidation
			------------------------------------------------------------------

			-- StgCustomer.BillingType
			EXEC spDMListReplacement '#StgCustomer', 'BillingType',
			'tarCustomer', 'BillingType', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END
			-- StgCustomer.CreditLimitAgeCat
			EXEC spDMListReplacement '#StgCustomer', 'CreditLimitAgeCat',
			'tarCustomer', 'CreditLimitAgeCat', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END	
	
			-- StgCustomer.Status
			EXEC spDMListReplacement '#StgCustomer', 'Status',
			'tarCustomer', 'Status', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END
	
			-- StgCustomer.PriceBase
			EXEC spDMListReplacement '#StgCustomer', 'PriceBase',
			'tarCustAddr', 'PriceBase', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- StgCustomer.Action
			EXEC spDMListReplacement '#StgCustomer', 'Action',
			'StgCustomer', 'Action', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
	
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- StgCustAddr.Action
			EXEC spDMListReplacement '#StgCustAddr', 'Action',
			'StgCustAddr', 'Action', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
	
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END


		-- StgCustomer.CarrierBillMeth
			EXEC spDMListReplacement '#StgCustomer', 'CarrierBillMeth',
			'tarCustAddr', 'CarrierBillMeth', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT

			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

		-- StgCustomer.FreightMethod
			EXEC spDMListReplacement '#StgCustomer', 'FreightMethod',
			'tarCustAddr', 'FreightMethod', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
	
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- must update the pct fields to match the Storage method
			--UPDATE #StgCustomer SET FinChgPct =  COALESCE(FinChgPct, 0)/100, TradeDiscPct = COALESCE(TradeDiscPct, 0)/100
	
			-- Validate user fields
			--1=Success, 0=Unknown Failure, 2=Validation Error, 6=Invalid Table
			EXEC spDMValidateUserFields   @iSessionKey, '#StgCustomer', @LanguageID,
			1, @iRptOption, @iPrintWarnings ,@RetVal OUTPUT
	
			IF @RetVal <> @RET_SUCCESS
	      		AND @RetVal <> @RET_VALIDATION_ERROR AND @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR
				RETURN
			END /* End @RetVal <> @RET_SUCCESS */

			TRUNCATE TABLE #tdmMigrationLogEntryWrk

			-- mark records for invalid user field
			IF @iUseStageTable = 1
			BEGIN

				UPDATE StgCustomer SET ProcessStatus = @GENRL_PROC_ERR
				FROM StgCustomer C
				INNER JOIN #StgCustomer SC
				ON C.RowKey = SC.RowKey
				WHERE SC.ProcessStatus = @GENRL_PROC_ERR
				AND SC.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)
			END

			-- update StgCustAddr for failed StgCustomer rows
			-- there could be a failure from user fields or from a duplicate
			UPDATE #StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
			FROM #StgCustAddr CA
			INNER JOIN #StgCustomer SC
			ON CA.CustID = SC.CustID
			WHERE SC.ProcessStatus = @GENRL_PROC_ERR
			AND SC.SessionKey = @iSessionKey
			AND CA.SessionKey = @iSessionKey
			AND (CA.CustAddrID = SC.DfltBillToAddrID OR CA.CustAddrID = SC.DfltShipToAddrID)
			AND NOT EXISTS(SELECT * FROM #StgCustomer WHERE CustID = CA.CustID AND ProcessStatus = @NOT_PROCESSED)
			OPTION(KEEP PLAN)

			IF @iUseStageTable = 1
			BEGIN
				-- update the real staging table if used
				UPDATE StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
				FROM StgCustAddr CA
				INNER JOIN #StgCustAddr SC
				ON CA.RowKey = SC.RowKey
				WHERE SC.ProcessStatus = @GENRL_PROC_ERR
				AND SC.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)

			END

			-- update StgCustDocTrnsmit process status for parent fails
			-- there could be a failure from user fields or from a duplicate
			UPDATE #StgCustDocTrnsmit SET ProcessStatus = @GENRL_PROC_ERR
			FROM #StgCustDocTrnsmit CDT
			INNER JOIN #StgCustomer C
			ON CDT.CustID = C.CustID
			WHERE C.ProcessStatus = @GENRL_PROC_ERR
			AND C.SessionKey = @iSessionKey
			AND NOT EXISTS(SELECT * FROM #StgCustomer WHERE CustID = CDT.CustID AND ProcessStatus = @NOT_PROCESSED)
			OPTION(KEEP PLAN)
			
			IF @iUseStageTable = 1
			BEGIN
				-- update the real staging table if used
				UPDATE StgCustDocTrnsmit SET ProcessStatus = @GENRL_PROC_ERR
				FROM StgCustDocTrnsmit CDT
				INNER JOIN #StgCustDocTrnsmit C
				ON CDT.RowKey = C.RowKey
				WHERE C.ProcessStatus = @GENRL_PROC_ERR
				AND C.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)
			END

			-------------------------------------------------------------
			-- replacements for StgCustAddr
			-------------------------------------------------------------
			EXEC spARCustAddrUpd @iSessionKey, @RetVal OUTPUT

			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RetVal, @oContinue = 0
				RETURN
			END	
	
			-- replace blank/null values in DfltShipToAddrID and DfltBillToAddrID
			UPDATE #StgCustomer SET DfltBillToAddrID = CustID WHERE
			NULLIF(LTRIM(RTRIM(DfltBillToAddrID)),'') IS NULL
			AND Action = @REC_INSERT
			OPTION(KEEP PLAN)

			UPDATE #StgCustomer SET DfltShipToAddrID = CustID WHERE
			NULLIF(LTRIM(RTRIM(DfltShipToAddrID)),'') IS NULL
			AND Action = @REC_INSERT
			OPTION(KEEP PLAN)

			-- update th currency to the home currncy of the target company if null or blank	
			UPDATE #StgCustomer SET CurrID = @HomeCurrID WHERE
			NULLIF(LTRIM(RTRIM(CurrID)),'') IS NULL
			AND Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			UPDATE #StgCustAddr SET CurrID = @HomeCurrID WHERE
			NULLIF(LTRIM(RTRIM(CurrID)),'') IS NULL
			AND Action = @REC_INSERT
			OPTION(KEEP PLAN)
	
			----------------------------------------------------------------------------------
			-- General validation of business rules occurs before the insert loop
			-- If validation fails, then the insert loop logic is skipped, this prevents
			-- the code from processing needlessly, and also prevents double-logging of
			-- warnings or failures. NOTE: Log warnings and errors as they are found, this
			-- will allow the user to see all known issues in the report (vs. ending
			-- validation on the first error encountered).
			----------------------------------------------------------------------------------
			SELECT @Validated = 1, @ProcessStatus = @MIG_STAT_SUCCESSFUL -- start out as true
	
			-- populate customer hdr validation table from the Staging table
			-- #StgCustomer
			TRUNCATE TABLE #tarCustHdrValWrk
		
			-- first insert the row with 'INSERT' Action
			INSERT #tarCustHdrValWrk
			(RowKey,ABANo,AllowCustRefund,AllowInvtSubst,AllowWriteOff,BillingType
			,CreditLimit,CreditLimitAgeCat,CreditLimitUsed,CRMCustID,CurrExchSchdID
			,CurrID,DfltSalesReturnAcctNo,CustClassID,CustID,BOLReqd,InvoiceReqd
		,PackListReqd,PackListContentsReqd,ShipLabelsReqd,CustName
			,CustRefNo,DateEstab,DfltBillToAddrID,DfltItemID,DfltSalesAcctNo,DfltShipToAddrID
			,FinChgFlatAmt,FinChgPct,Hold,PmtTermsID,PrintDunnMsg,PrintOrderAck
			,ReqPO,RequireSOAck,SalesSourceID,ShipPriority,Status,StdIndusCodeID,StmtCycleID,StmtFormID
			,TradeDiscPct,UserFld1,UserFld2,UserFld3,UserFld4
			,VendID,CarrierAcctNo,CarrierBillMeth,FreightMethod
			,ProcessStatus,SessionKey,PriceAdj,Action, Name)
			SELECT RowKey,ABANo,AllowCustRefund,AllowInvtSubst,AllowWriteOff,BillingType
			,CreditLimit,CreditLimitAgeCat,CreditLimitUsed,CRMCustID,CurrExchSchdID
			,CurrID,DfltSalesReturnAcctNo,CustClassID,CustID,BOLReqd,InvoiceReqd
		,PackListReqd,PackListContentsReqd,ShipLabelsReqd,CustName
			,CustRefNo,COALESCE(DateEstab,@UserBusinessDate),DfltBillToAddrID,DfltItemID,DfltSalesAcctNo,DfltShipToAddrID
			,FinChgFlatAmt,FinChgPct,Hold,PmtTermsID,PrintDunnMsg,PrintOrderAck
			,ReqPO,RequireSOAck,SalesSourceID,ShipPriority,Status,StdIndusCodeID,StmtCycleID,StmtFormID
			,TradeDiscPct,UserFld1,UserFld2,UserFld3,UserFld4
			,VendID,CarrierAcctNo,CarrierBillMeth,FreightMethod
			,ProcessStatus,SessionKey,coalesce(PriceAdj,0),Action, Name
			FROM #StgCustomer WITH (NOLOCK)
			WHERE SessionKey = @iSessionKey
			AND ProcessStatus = @NOT_PROCESSED
			AND Action = @REC_INSERT
			OPTION(KEEP PLAN)

			--Now insert the rows and columns with 'UPDATE' Action

			-- Getting primary address related data
			-- Save the updatecolumnlist first
			TRUNCATE TABLE #CustAddrUpdColumnList

			INSERT INTO #CustAddrUpdColumnList
						(SessionKey, ColumnName)
			SELECT		@iSessionKey, tc.InternalColumnName
			FROM	tdiDataTrnsfrColumnMap tc WITH (NOLOCK)
				JOIN	(SELECT DISTINCT DataTrnsfrjobDefStepKey
								FROM tdiDataTrnsfrJobDefStep tjs WITH (NOLOCK)
									JOIN 	tdiDataTrnsfrJobExecute tj WITH (NOLOCK)	ON	tjs.DataTrnsfrJobDefKey = tj.DataTrnsfrJobDefKey
								WHERE tj.SessionKey = @iSessionkey AND tjs.InternalTableName = 'StgCustomer') derv
							ON	tc.DataTrnsfrjobDefStepKey = derv.DataTrnsfrjobDefStepKey
			WHERE	tc.AllowUpdate = 1
			AND		tc.InternalColumnName IN (
				'AddrLine1','AddrLine2','AddrLine3','AddrLine4','AddrLine5','AddrName','AllowInvtSubst','City','CloseSOLineOnFirstShip',
				'CloseSOOnFirstShip','CommPlanID','CountryID','CurrExchSchdID','CurrID','CustAddrID','CustID','CustPriceGroupID',
				'EMailAddr','Fax','FaxExt','FOBID','InvcFormID','InvcMsg','LanguageID','Name','PackListFormID','Phone',
				'PhoneExt','PmtTermsID','PostalCode','PriceBase','PrintOrderAck','RequireSOAck','SalesTerritoryID','ShipComplete',
				'ShipDays','ShipLabelFormID','ShipMethID','SOAckFormID','SperID','StateID','STaxSchdID','Title','WhseID','ProcessStatus',
				'CommPlanKey','CRMAddrID','CRMContactID','PriceAdj','BOLReqd','InvoiceReqd','PackListReqd','PackListContentsReqd',
				'Residential','ShipLabelsReqd','CarrierAcctNo','CarrierBillMeth','FreightMethod')
	
			SELECT	@UpdColListAddr = ''
			SELECT	@UpdColListAddr = @UpdColListAddr + list.ColumnName,
					@UpdColListAddr = @UpdColListAddr + ', '
			FROM 	#CustAddrUpdColumnList list
			
			-- If there are updatable column specified
			IF LEN(RTRIM(LTRIM(@UpdColListAddr))) > 0
			BEGIN
				SELECT @UpdColListAddr = SUBSTRING(@UpdColListAddr, 1, LEN(@UpdColListAddr) - 1)
				SELECT @UpdColList1 = 'NULL, CustID, CustID, ProcessStatus, Action, ' + @UpdColListAddr
				SELECT @UpdColListAddr = 'RowKey, CustID, CustAddrID, ProcessStatus, Action, ' + @UpdColListAddr

				SELECT @SQLStmt = ''
				SELECT @SQLStmt = 'INSERT INTO #tarCustAddrValWrk (' + RTRIM(LTRIM(@UpdColListAddr)) + ') SELECT ' + RTRIM(LTRIM(@UpdColList1)) + ' FROM #StgCustomer WHERE SessionKey = ' + CONVERT(VARCHAR(10), @iSessionKey) + ' AND ProcessStatus = ' + CONVERT(VARCHAR(1), @NOT_PROCESSED) + ' AND Action = 2 OPTION(KEEP PLAN)'

				EXECUTE (@SQLStmt)	
			END	

			--Getting customer header related data
			TRUNCATE TABLE #CustHdrUpdColumnList
			INSERT INTO #CustHdrUpdColumnList
						(SessionKey, ColumnName)
			SELECT		@iSessionKey, tc.InternalColumnName
			FROM	tdiDataTrnsfrColumnMap tc WITH (NOLOCK)
				JOIN	(SELECT DISTINCT DataTrnsfrjobDefStepKey
								FROM tdiDataTrnsfrJobDefStep tjs WITH (NOLOCK)
									JOIN 	tdiDataTrnsfrJobExecute tj WITH (NOLOCK)	ON	tjs.DataTrnsfrJobDefKey = tj.DataTrnsfrJobDefKey
								WHERE tj.SessionKey = @iSessionkey AND tjs.InternalTableName = 'StgCustomer') derv
							ON	tc.DataTrnsfrjobDefStepKey = derv.DataTrnsfrjobDefStepKey
			WHERE	tc.AllowUpdate = 1
			AND		tc.InternalColumnName IN
					('ABANo','AllowCustRefund','AllowInvtSubst','AllowWriteOff','BillingType','BOLReqd','CarrierAcctNo','CarrierBillMeth',
					'CreditLimit','CreditLimitAgeCat','CreditLimitUsed','CRMCustID','CurrExchSchdID','CurrID','CustClassID','CustID',
					'CustName','CustRefNo','DateEstab','DfltBillToAddrID','DfltItemID','DfltItemKey','DfltSalesAcctNo','DfltSalesReturnAcctNo',
					'DfltShipToAddrID','FinChgFlatAmt','FinChgPct','FreightMethod','Hold','InvoiceReqd','Name','PackListContentsReqd',
					'PackListReqd','PmtTermsID','PriceAdj','PrintDunnMsg','PrintOrderAck','ReqPO','RequireSOAck','Residential','SalesSourceID',
					'ShipLabelsReqd','ShipPriority','Status','StdIndusCodeID','StmtCycleID','StmtFormID','TradeDiscPct',
					'UserFld1','UserFld2','UserFld3','UserFld4','VendID')

			SELECT	@UpdColListHdr = ''
			SELECT	@UpdColListHdr = @UpdColListHdr + list.ColumnName,
					@UpdColListHdr = @UpdColListHdr + ', '
			FROM 	#CustHdrUpdColumnList list

			-- If there are updatable column specified
			IF LEN(RTRIM(LTRIM(@UpdColListHdr))) > 0
			BEGIN
				SELECT @UpdColListHdr = SUBSTRING(@UpdColListHdr, 1, LEN(@UpdColListHdr) - 1)
				SELECT @UpdColListHdr = 'RowKey, CustID, ProcessStatus, SessionKey, Action, ' + @UpdColListHdr

				SELECT @SQLStmt = ''
				SELECT @SQLStmt = 'INSERT INTO #tarCustHdrValWrk (' + RTRIM(LTRIM(@UpdColListHdr)) + ') SELECT ' + RTRIM(LTRIM(@UpdColListHdr)) + ' FROM #StgCustomer WHERE SessionKey = ' + CONVERT(VARCHAR(10), @iSessionKey) + ' AND ProcessStatus = ' + CONVERT(VARCHAR(1), @NOT_PROCESSED) + ' AND Action = 2 OPTION(KEEP PLAN)'

				EXECUTE (@SQLStmt)	
			END		
			ELSE
			BEGIN
				-- If only address related fields allow update, create one entry into for #tarCustHdrValWrk each primary address found. 	
				INSERT INTO #tarCustHdrValWrk(RowKey, CustID, ProcessStatus, SessionKey, Action)	
				SELECT	RowKey, CustID, ProcessStatus, SessionKey, Action
				FROM	#StgCustomer
				WHERE Action = 2 OPTION(KEEP PLAN)	
			END
			
			IF LEN(RTRIM(LTRIM(@UpdColListHdr))) = 0 AND LEN(RTRIM(LTRIM(@UpdColListAddr))) = 0
			BEGIN
			-- If there is no updatable column specified but there are records with the 'Update' flag, mark the records
			-- to "Fail"

				-- Check for count of unverified records with the 'Update' flag
				SELECT	@UpdRecCount = COUNT(*) FROM #StgCustomer
				WHERE	SessionKey = @iSessionKey
				AND		ProcessStatus = @NOT_PROCESSED
				AND		Action = @REC_UPDATE
			
				-- Record found, run validation
				IF @UpdRecCount > 0
				BEGIN

					SELECT @EntityColName = 'COALESCE(CustID, '''')'
			
					EXEC spDMValidateUpdColforUpdRec @iSessionKey, 0 /*@iUseStageTable*/,'StgCustomer', @EntityColName,
							@LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @ErrorCount OUTPUT, @RetVal OUTPUT

					-- Cleanup #tdmMigrationLogEntryWrk
					TRUNCATE TABLE #tdmMigrationLogEntryWrk

					IF @RetVal <> @RET_SUCCESS
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						RETURN
					END
				END
			END

			--DELETE FROM #tarCustHdrValWrk WHERE ProcessStatus = @GENRL_PROC_ERR
			-- Insert Primary CustAddr and tciAddress info into temp table
			INSERT #tarCustAddrValWrk (
			AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName
			,AllowInvtSubst,City,CommPlanID,CountryID,CloseSOLineOnFirstShip, CloseSOOnFirstShip, CurrExchSchdID
			,CurrID,CustAddrID,CustID,CustPriceGroupID,EMailAddr,Fax,FaxExt
			,FOBID,InvcFormID,InvcMsg,Name,PackListFormID
			,Phone,PhoneExt,PmtTermsID,PostalCode
			,PriceBase,PrintOrderAck,ProcessStatus,RequireSOAck
			,SalesTerritoryID,ShipComplete,ShipDays
			,ShipLabelFormID,ShipMethID
			,SOAckFormID,SperID,StateID
			,STaxSchdID,Title,WhseID,BOLReqd,InvoiceReqd,PackListReqd
			,PackListContentsReqd,ShipLabelsReqd,Residential,CarrierAcctNo
			,CarrierBillMeth,FreightMethod,PriceAdj,Action,CRMAddrID,CRMContactID)
			SELECT
			NULLIF(AddrLine1,''),NULLIF(AddrLine2,''),NULLIF(AddrLine3,''),NULLIF(AddrLine4,''),NULLIF(AddrLine5,''),NULLIF(CustName,'')-- Primary AddrName is always Cust Name
			,NULLIF(AllowInvtSubst,''),NULLIF(City,''),NULLIF(CommPlanID,''),NULLIF(CountryID,'')
			,NULLIF(CloseSOLineOnFirstShip, ''), NULLIF(CloseSOOnFirstShip, ''),NULLIF(CurrExchSchdID,'')
			,NULLIF(CurrID,''),CustID,CustID,NULLIF(CustPriceGroupID,''),NULLIF(EMailAddr,''),NULLIF(Fax,''),NULLIF(FaxExt,'')
			,NULLIF(FOBID,''),NULLIF(InvcFormID,''),NULLIF(InvcMsg,''),COALESCE(Name,''),NULLIF(PackListFormID,'')
			,NULLIF(Phone,''),NULLIF(PhoneExt,''),NULLIF(PmtTermsID,''),NULLIF(PostalCode,'')
			,NULLIF(PriceBase,''),NULLIF(PrintOrderAck,''),COALESCE(ProcessStatus, @NOT_PROCESSED),NULLIF(RequireSOAck,'')
			,NULLIF(SalesTerritoryID,''),NULLIF(ShipComplete,''),ShipDays
			,NULLIF(ShipLabelFormID,''),NULLIF(ShipMethID,'')
			,NULLIF(SOAckFormID,''),NULLIF(SperID,''),NULLIF(StateID,'')
			,NULLIF(STaxSchdID,''),NULLIF(Title,''),NULLIF(WhseID,''),NULLIF(BOLReqd,''),NULLIF(InvoiceReqd,'')
			,NULLIF(PackListReqd,''),NULLIF (PackListContentsReqd,''),NULLIF(ShipLabelsReqd,''),NULLIF(Residential,'')
			,NULLIF(CarrierAcctNo,''),NULLIF(CarrierBillMeth,''),NULLIF(FreightMethod,''),coalesce(PriceAdj,0),Action, NULLIF(CRMAddrID,''), NULLIF(CRMContactID,'')
			FROM #StgCustomer WITH (NOLOCK)
			WHERE SessionKey = @iSessionKey
			AND ProcessStatus = @NOT_PROCESSED
			AND Action = @REC_INSERT
			AND	CustID IS NOT NULL
			OPTION(KEEP PLAN)

			-- If the DfltBillToAddrID is different than the primary
			-- Add it to the temp table
			INSERT #tarCustAddrValWrk (RowKey,
			AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName
			,AllowInvtSubst,City,CommPlanID,CountryID,CloseSOLineOnFirstShip, CloseSOOnFirstShip, CurrExchSchdID
			,CurrID,CustAddrID,CustID,CustPriceGroupID,EMailAddr,Fax,FaxExt
			,FOBID,InvcFormID,InvcMsg,Name,PackListFormID
			,Phone,PhoneExt,PmtTermsID,PostalCode
			,PriceBase,PrintOrderAck,ProcessStatus,RequireSOAck
			,SalesTerritoryID,ShipComplete,ShipDays
			,ShipLabelFormID,ShipMethID
			,SOAckFormID,SperID,StateID
			,STaxSchdID,Title,WhseID,BOLReqd,InvoiceReqd,PackListReqd
			,PackListContentsReqd,ShipLabelsReqd,Residential
			,CarrierAcctNo,CarrierBillMeth,FreightMethod,PriceAdj,Action,CRMAddrID,CRMContactID)
			SELECT CA.RowKey,
			NULLIF(CA.AddrLine1,''),NULLIF(CA.AddrLine2,''),NULLIF(CA.AddrLine3,''),NULLIF(CA.AddrLine4,'')
			,NULLIF(CA.AddrLine5,''),NULLIF(CA.AddrName,'')	,NULLIF(CA.AllowInvtSubst,''),NULLIF(CA.City,'')
			,NULLIF(CA.CommPlanID,''),NULLIF(CA.CountryID,''),NULLIF(CA.CloseSOLineOnFirstShip, ''), NULLIF(CA.CloseSOOnFirstShip, '')
			,NULLIF(CA.CurrExchSchdID,''),NULLIF(CA.CurrID,'')
			,NULLIF(CA.CustAddrID,''),C.CustID,NULLIF(CA.CustPriceGroupID,''),NULLIF(CA.EMailAddr,''),NULLIF(CA.Fax,'')
			,NULLIF(CA.FaxExt,''),NULLIF(CA.FOBID,''),NULLIF(CA.InvcFormID,''),NULLIF(CA.InvcMsg,''),NULLIF(RTRIM(LTRIM(CA.Name)),'')
			,NULLIF(CA.PackListFormID,''),NULLIF(CA.Phone,''),NULLIF(CA.PhoneExt,''),NULLIF(CA.PmtTermsID,'')
			,NULLIF(CA.PostalCode,''),NULLIF(CA.PriceBase,''),NULLIF(CA.PrintOrderAck,''),COALESCE(CA.ProcessStatus,@NOT_PROCESSED)
			,CA.RequireSOAck,NULLIF(CA.SalesTerritoryID,''),NULLIF(CA.ShipComplete,''),CA.ShipDays
			,NULLIF(CA.ShipLabelFormID,''),NULLIF(CA.ShipMethID,'')
			,NULLIF(CA.SOAckFormID,''),NULLIF(CA.SperID,''),NULLIF(CA.StateID,'')
			,NULLIF(CA.STaxSchdID,''),NULLIF(CA.Title,''),NULLIF(CA.WhseID,''),NULLIF(CA.BOLReqd,''),NULLIF(CA.InvoiceReqd,'')
			,NULLIF(CA.PackListReqd,''),NULLIF (CA.PackListContentsReqd,''),NULLIF(CA.ShipLabelsReqd,''),NULLIF(CA.Residential,'')
			,NULLIF(CA.CarrierAcctNo,'') ,NULLIF(CA.CarrierBillMeth,''),NULLIF(CA.FreightMethod,''),coalesce(ca.priceadj,0),CA.Action
			,NULLIF(CA.CRMAddrID,''), NULLIF(CA.CRMContactID,'')
			FROM #StgCustAddr CA WITH (NOLOCK)
			INNER JOIN (SELECT DISTINCT CustID,DfltBillToAddrID FROM #tarCustHdrValWrk) C-- Need to use derived table because of possible duplication in Header table
			ON LTRIM(RTRIM(CA.CustAddrID)) = C.DfltBillToAddrID
AND LTRIM(RTRIM(CA.CustID)) = C.CustID
			WHERE CA.ProcessStatus = @NOT_PROCESSED
			AND CA.SessionKey = @iSessionKey
			AND COALESCE(C.DfltBillToAddrID,'') <> C.CustID
			AND NULLIF(LTRIM(RTRIM(C.DfltBillToAddrID)),'') IS NOT NULL
			AND CA.Action = @REC_INSERT
			OPTION(KEEP PLAN)

			-- If the DfltShipToAddrID is different than the primary
			-- Add it to the temp table
			INSERT #tarCustAddrValWrk (RowKey,
			AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName
			,AllowInvtSubst,City,CommPlanID,CountryID,CloseSOLineOnFirstShip, CloseSOOnFirstShip, CurrExchSchdID
			,CurrID,CustAddrID,CustID,CustPriceGroupID,EMailAddr,Fax,FaxExt
			,FOBID,InvcFormID,InvcMsg,Name,PackListFormID
			,Phone,PhoneExt,PmtTermsID,PostalCode
			,PriceBase,PrintOrderAck,ProcessStatus,RequireSOAck
			,SalesTerritoryID,ShipComplete,ShipDays
			,ShipLabelFormID,ShipMethID
			,SOAckFormID,SperID,StateID
			,STaxSchdID,Title,WhseID,BOLReqd,InvoiceReqd,PackListReqd
			,PackListContentsReqd,ShipLabelsReqd,Residential
			,CarrierAcctNo,CarrierBillMeth,FreightMethod,PriceAdj,Action,CRMAddrID,CRMContactID)
			SELECT CA.RowKey,
			NULLIF(CA.AddrLine1,''),NULLIF(CA.AddrLine2,''),NULLIF(CA.AddrLine3,''),NULLIF(CA.AddrLine4,''),NULLIF(CA.AddrLine5,''),NULLIF(CA.AddrName,'')
			,NULLIF(CA.AllowInvtSubst,''),NULLIF(CA.City,''),NULLIF(CA.CommPlanID,''),NULLIF(CA.CountryID,'')
			,NULLIF(CA.CloseSOLineOnFirstShip, ''), NULLIF(CA.CloseSOOnFirstShip, '')
			,NULLIF(CA.CurrExchSchdID,'')
			,NULLIF(CA.CurrID,''),NULLIF(CA.CustAddrID,''),C.CustID,NULLIF(CA.CustPriceGroupID,''),NULLIF(CA.EMailAddr,''),NULLIF(CA.Fax,''),NULLIF(CA.FaxExt,'')
			,NULLIF(CA.FOBID,''),NULLIF(CA.InvcFormID,''),NULLIF(CA.InvcMsg,''),NULLIF(RTRIM(LTRIM(CA.Name)),''),NULLIF(CA.PackListFormID,'')
			,NULLIF(CA.Phone,''),NULLIF(CA.PhoneExt,''),NULLIF(CA.PmtTermsID,''),NULLIF(CA.PostalCode,'')
			,NULLIF(CA.PriceBase,''),NULLIF(CA.PrintOrderAck,''),COALESCE(CA.ProcessStatus,@NOT_PROCESSED),CA.RequireSOAck
			,NULLIF(CA.SalesTerritoryID,''),NULLIF(CA.ShipComplete,''),CA.ShipDays
			,NULLIF(CA.ShipLabelFormID,''),NULLIF(CA.ShipMethID,'')
			,NULLIF(CA.SOAckFormID,''),NULLIF(CA.SperID,''),NULLIF(CA.StateID,'')
			,NULLIF(CA.STaxSchdID,''),NULLIF(CA.Title,''),NULLIF(CA.WhseID,''),NULLIF(CA.BOLReqd,''),NULLIF(CA.InvoiceReqd,'')
			,NULLIF(CA.PackListReqd,''),NULLIF (CA.PackListContentsReqd,''),NULLIF(CA.ShipLabelsReqd,''),NULLIF(Residential,'')
			,NULLIF(CA.CarrierAcctNo,''),NULLIF(CA.CarrierBillMeth,''),NULLIF(CA.FreightMethod,''),coalesce(CA.PriceAdj,0),CA.Action
			,NULLIF(CA.CRMAddrID,''), NULLIF(CA.CRMContactID,'')
			FROM #StgCustAddr CA WITH (NOLOCK)
			INNER JOIN (SELECT DISTINCT CustID,DfltShipToAddrID,DfltBillToAddrID FROM #tarCustHdrValWrk) C-- Need to use derived table because of possible duplication in Header table
			ON LTRIM(RTRIM(CA.CustAddrID)) = C.DfltShipToAddrID
AND LTRIM(RTRIM(CA.CustID)) = C.CustID
			WHERE CA.ProcessStatus = @NOT_PROCESSED
			AND CA.SessionKey = @iSessionKey
			AND COALESCE(C.DfltShipToAddrID,'') <> C.CustID
			AND NULLIF(LTRIM(RTRIM(C.DfltShipToAddrID)),'') <> NULLIF(LTRIM(RTRIM(C.DfltBillToAddrID)),'')
			AND NULLIF(LTRIM(RTRIM(C.DfltShipToAddrID)),'') IS NOT NULL
			AND CA.Action = @REC_INSERT
			OPTION(KEEP PLAN)

			------------------------------------------------
			-- check for duplicate CustID in the Address TempTable
			------------------------------------------------
			--Check for duplication of Primary Addr(in CustAddr temp table)
			UPDATE #tarCustAddrValWrk SET ProcessStatus = @GENRL_PROC_ERR
				FROM #tarCustAddrValWrk a
				WHERE 1 < SOME (SELECT COUNT(*) FROM #tarCustAddrValWrk
					GROUP BY CustAddrID,CustID
					HAVING CustID = a.CustID AND CustAddrID = a.CustAddrID)
					AND a.CtrlRowKey  <>
					(SELECT MIN(CtrlRowKey) FROM #tarCustAddrValWrk WHERE CustID = a.CustID AND CustAddrID = a.CustID)
					AND RowKey IS NULL
					OPTION (KEEP PLAN)
	

			--Check for duplication ShipTo/BillTo(in CustAddr temp table)
			UPDATE #tarCustAddrValWrk SET ProcessStatus = @GENRL_PROC_ERR
				FROM #tarCustAddrValWrk a
				WHERE 1 < SOME (SELECT COUNT(*) FROM #tarCustAddrValWrk
					GROUP BY CustAddrID,CustID
					HAVING CustID = a.CustID AND CustAddrID = a.CustAddrID)
					AND a.CtrlRowKey  <>
					(SELECT MIN(CtrlRowKey) FROM #tarCustAddrValWrk WHERE CustID = a.CustID AND CustAddrID = a.CustAddrID)
					AND RowKey IS NOT NULL
					OPTION (KEEP PLAN)


			-- if any duplicates in StgCustAddr validation table then update them and report as requested			
			IF EXISTS(SELECT COUNT(*) FROM #tarCustAddrValWrk WHERE ProcessStatus = @GENRL_PROC_ERR)
			BEGIN
	
				IF @iPrintWarnings = 1
				BEGIN
					--SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,
					SELECT @StringNo = @RPT_STRINGNO_DUPLICATE
	
					EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT--, 'CustAddrID'
	
					-- insert into temp log table
					INSERT #tdmMigrationLogEntryWrk
						(ColumnID,ColumnValue,Comment,Duplicate,
						EntityID,SessionKey,Status)
					SELECT 'CustAddrID', CustAddrID, @Message, 1,
						COALESCE(LTRIM(RTRIM(CustID)),'') + '|' + LTRIM(RTRIM(CustAddrID)), @iSessionKey,@MIG_STAT_WARNING
					FROM #tarCustAddrValWrk  WITH (NOLOCK) WHERE
						ProcessStatus = @GENRL_PROC_ERR
						OPTION (KEEP PLAN)
	
					-- call to add log entries
					EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

					TRUNCATE TABLE #tdmMigrationLogEntryWrk
					-- reset variables
					SELECT @StringNo = NULL, @Message = NULL
				END
	
				-- mark the duplicate(s) in the staging temp table
				UPDATE #StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
				FROM #StgCustAddr CA WITH (NOLOCK)
				INNER JOIN #tarCustAddrValWrk TCA WITH (NOLOCK)
				ON CA.RowKey = TCA.RowKey
				WHERE TCA.ProcessStatus = @GENRL_PROC_ERR
				OPTION(KEEP PLAN)
	
				IF @iUseStageTable = 1
				BEGIN
	
					-- mark the duplicate(s) in the staging perm table
					UPDATE StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
					FROM StgCustAddr CA WITH (NOLOCK)
					INNER JOIN #tarCustAddrValWrk TCA WITH (NOLOCK)
					ON CA.RowKey = TCA.RowKey
					WHERE  TCA.ProcessStatus = @GENRL_PROC_ERR	
					OPTION(KEEP PLAN)
	
				END
				
			END

			-----------------------------------------------------------------------
			--Validations for tarCustomer
			-----------------------------------------------------------------------
			EXEC spARCustHdrVal @iPrintWarnings, @LanguageID,
			@iSessionKey, @iUseStageTable, @BlankInvalidReference,
			@InvalidGLUseSuspense, @iCompanyID,@iRptOption,
			@HomeCurrID, @RetVal OUTPUT

			----------------------------------------
			-- Validate StgCustAddr Columns/get keys
			----------------------------------------
			EXEC spARCustAddrVal @iPrintWarnings, @LanguageID,
			@iSessionKey, @iUseStageTable, @BlankInvalidReference,@iCompanyID,
			@iRptOption, @RetVal OUTPUT


			TRUNCATE TABLE #tdmMigrationLogEntryWrk

			---------------------------------------------------------------------- 	
			-- update the header if any of the addresses are marked bad
			----------------------------------------------------------------------
			-- check for the DfltBillTo 	
			UPDATE #tarCustHdrValWrk SET ProcessStatus = @GENRL_PROC_ERR
			FROM #tarCustHdrValWrk a WHERE
			NULLIF(LTRIM(RTRIM(a.DfltBillToAddrID)),'') IS NOT NULL
			AND LTRIM(RTRIM(a.DfltBillToAddrID)) <> LTRIM(RTRIM(a.CustID))
			AND EXISTS
			(SELECT CustAddrID from #tarCustAddrValWrk
				WHERE CustID = a.CustID
				AND CustAddrID = a.DfltBillToAddrID
				AND CustID IS NOT NULL
				AND ProcessStatus = @GENRL_PROC_ERR)
				OPTION (KEEP PLAN)

			--report the header failure
			IF (@iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL) AND @@ROWCOUNT > 0
			BEGIN
				-- insert into temp log table
				INSERT #tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Comment,Duplicate,
				EntityID,SessionKey,Status)
				SELECT 'DfltBillToAddrID',LTRIM(RTRIM(COALESCE(a.DfltBillToAddrID,''))),'DfltBillToAddrID failed validation.',0,
				CustID,@iSessionKey,@MIG_STAT_FAILURE
				FROM #tarCustHdrValWrk a WHERE
				NULLIF(LTRIM(RTRIM(a.DfltBillToAddrID)),'') IS NOT NULL
				AND EXISTS(SELECT CustAddrID from #tarCustAddrValWrk
					WHERE CustID = a.CustID
					AND CustAddrID = a.DfltBillToAddrID
					AND CustID IS NOT NULL
					AND ProcessStatus = @GENRL_PROC_ERR)
					OPTION (KEEP PLAN)

			END		
	
			-- check for DfltShipTo
			UPDATE #tarCustHdrValWrk SET ProcessStatus = @GENRL_PROC_ERR
			FROM #tarCustHdrValWrk a WHERE
			NULLIF(LTRIM(RTRIM(a.DfltShipToAddrID)),'') IS NOT NULL
			AND LTRIM(RTRIM(a.DfltShipToAddrID)) <> LTRIM(RTRIM(a.CustID))
			AND EXISTS
			(SELECT CustAddrID FROM #tarCustAddrValWrk
				WHERE CustID = a.CustID
				AND CustAddrID = a.DfltShipToAddrID
				AND CustID IS NOT NULL
				AND ProcessStatus = @GENRL_PROC_ERR)
				OPTION (KEEP PLAN)

			--report the header failure
			IF (@iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL) AND @@ROWCOUNT > 0
			BEGIN
				-- insert into temp log table
				INSERT #tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Comment,Duplicate,
				EntityID,SessionKey,Status)
				SELECT 'DfltShipToAddrID',LTRIM(RTRIM(COALESCE(a.DfltShipToAddrID,''))),'DfltShipToAddrID failed validation.',0,
				CustID,@iSessionKey,@MIG_STAT_FAILURE
				FROM #tarCustHdrValWrk a WHERE
				NULLIF(LTRIM(RTRIM(a.DfltShipToAddrID)),'') IS NOT NULL
				AND NOT EXISTS
				(SELECT CustAddrID FROM #tarCustAddrValWrk
					WHERE CustID = a.CustID
					AND CustAddrID = a.DfltShipToAddrID
					AND CustID IS NOT NULL
					AND ProcessStatus = @NOT_PROCESSED)
					OPTION (KEEP PLAN)

			END
	
			-- check for Primary Addr
			UPDATE #tarCustHdrValWrk SET ProcessStatus = @GENRL_PROC_ERR
			FROM #tarCustHdrValWrk a WHERE
			EXISTS(SELECT CustAddrID FROM #tarCustAddrValWrk
				WHERE CustID = a.CustID
				AND CustAddrID = a.CustID
				AND CustID IS NOT NULL
				AND ProcessStatus = @GENRL_PROC_ERR)
				OPTION (KEEP PLAN)

			--report the header failure
			IF (@iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL) AND @@ROWCOUNT > 0
			BEGIN
				-- insert into temp log table
				INSERT #tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Comment,Duplicate,
				EntityID,SessionKey,Status)
				SELECT 'CustAddrID','','Primary Address failed validation.',0,
				COALESCE(a.CustID,''),@iSessionKey,@MIG_STAT_FAILURE
				FROM #tarCustHdrValWrk a WHERE
				NOT EXISTS(SELECT CustAddrID FROM #tarCustAddrValWrk
				WHERE CustID = a.CustID
				AND CustAddrID = a.CustID
				AND CustID IS NOT NULL
				AND ProcessStatus = @NOT_PROCESSED)
				OPTION (KEEP PLAN)

			END

			IF (@iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL)
			AND EXISTS (SELECT * FROM #tdmMigrationLogEntryWrk)
			BEGIN

				EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

			END


			TRUNCATE TABLE #tdmMigrationLogEntryWrk

			------------------------------------------------------
			-- Mark the child record failed if parent failed
			------------------------------------------------------
			-- First report the failures
			IF @iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL
			BEGIN			

				TRUNCATE TABLE #tdmMigrationLogEntryWrk

				SELECT @Message = NULL, @Message1 = NULL

				EXEC ciBuildString @RPT_STRINGNO_PARENTCUST, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
				
				EXEC ciBuildString @RPT_STRINGNO_UNSUCCESSFUL, @LanguageID, @Message1 OUTPUT, @RetVal OUTPUT

				INSERT #tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Comment,Duplicate,
				EntityID,SessionKey,Status)
				SELECT 'CustID',a.CustID,@Message + ' ' + @Message1,0,
				LTRIM(RTRIM(a.CustID)) + '|' + LTRIM(RTRIM(a.CustAddrID)),@iSessionKey,@MIG_STAT_FAILURE
				FROM #tarCustAddrValWrk a
				INNER JOIN #tarCustHdrValWrk b
				ON a.CustID = b.CustID
				AND b.ProcessStatus = @GENRL_PROC_ERR
				AND (a.CustAddrID = b.DfltBillToAddrID OR a.CustAddrID = b.DfltShipToAddrID)
				AND a.ProcessStatus <> @GENRL_PROC_ERR
				OPTION(KEEP PLAN)

				EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

				TRUNCATE TABLE #tdmMigrationLogEntryWrk

			END

			-- update the children with the header failure
			UPDATE a
			SET a.ProcessStatus = @GENRL_PROC_ERR
			FROM #tarCustAddrValWrk a
			JOIN #tarCustHdrValWrk b
			ON a.CustID = b.CustID
			AND b.ProcessStatus = @GENRL_PROC_ERR
			AND (a.CustAddrID = b.DfltBillToAddrID OR a.CustAddrID = b.DfltShipToAddrID)
			OPTION(KEEP PLAN)
		
			-- update #StgCustomer from the validation temp table
			UPDATE #StgCustomer SET ProcessStatus = @GENRL_PROC_ERR
			FROM #StgCustomer C WITH (NOLOCK)
			INNER JOIN #tarCustHdrValWrk CHV WITH (NOLOCK)
			ON C.RowKey = CHV.RowKey
			WHERE CHV.ProcessStatus = @GENRL_PROC_ERR
			OPTION(KEEP PLAN)

			-- update #StgCustAddr from the validation temp table
			UPDATE #StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
			FROM #StgCustAddr C WITH (NOLOCK)
			INNER JOIN #tarCustAddrValWrk CHV WITH (NOLOCK)
			ON C.RowKey = CHV.RowKey
			WHERE CHV.ProcessStatus = @GENRL_PROC_ERR
			OPTION(KEEP PLAN)

			-- Update #DocTrnsmit
			UPDATE #StgCustDocTrnsmit SET ProcessStatus = @GENRL_PROC_ERR
			FROM #StgCustDocTrnsmit CDT WITH (NOLOCK)
			INNER JOIN #tarCustHdrValWrk C WITH (NOLOCK)
			ON CDT.CustID = C.CustID
			WHERE C.ProcessStatus = @GENRL_PROC_ERR
			AND C.SessionKey = @iSessionKey
			OPTION(KEEP PLAN)

			UPDATE #tarCustDocTrnsmitWrk SET ProcessStatus = @GENRL_PROC_ERR
			FROM #tarCustDocTrnsmitWrk CDT WITH (NOLOCK)
			INNER JOIN #StgCustDocTrnsmit C WITH (NOLOCK)
			ON CDT.RowKey = C.RowKey
			WHERE C.ProcessStatus = @GENRL_PROC_ERR
			AND C.SessionKey = @iSessionKey
			OPTION(KEEP PLAN)

			-- update the real staging tables if used	
			IF @iUseStageTable = 1
			BEGIN
				
				UPDATE StgCustomer SET ProcessStatus = @GENRL_PROC_ERR
				FROM StgCustomer C WITH (NOLOCK)
				INNER JOIN #StgCustomer SC WITH (NOLOCK)
				ON C.RowKey = SC.RowKey
				WHERE SC.ProcessStatus = @GENRL_PROC_ERR
				AND SC.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)

				UPDATE StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
				FROM StgCustAddr C WITH (NOLOCK)
				INNER JOIN #StgCustAddr SC WITH (NOLOCK)
				ON C.RowKey = SC.RowKey
				WHERE SC.ProcessStatus = @GENRL_PROC_ERR
				AND SC.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)
				
				UPDATE StgCustDocTrnsmit SET ProcessStatus = @GENRL_PROC_ERR
				FROM StgCustDocTrnsmit CDT WITH (NOLOCK)
				INNER JOIN #StgCustDocTrnsmit C WITH (NOLOCK)
				ON CDT.RowKey = C.RowKey
				WHERE C.ProcessStatus = @GENRL_PROC_ERR
				AND C.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)

	
			END

			-- delete failures from the validation temp tables
			DELETE FROM #tarCustHdrValWrk WHERE ProcessStatus = @GENRL_PROC_ERR

			DELETE FROM #tarCustAddrValWrk WHERE ProcessStatus = @GENRL_PROC_ERR

			--Check for duplication(already in database)	
			IF EXISTS (SELECT * FROM #tarCustHdrValWrk WITH (NOLOCK)
					   WHERE CustKey IS NOT NULL AND Action = @REC_INSERT) OR
				EXISTS (SELECT * FROM #tarCustHdrValWrk WITH (NOLOCK)
					   WHERE CustKey IS NULL AND Action IN (@REC_UPDATE, @REC_DELETE))
			BEGIN
				--if report is necessary
				IF @iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL
				BEGIN
					SELECT @StringNo = @RPT_STRINGNO_DUPLICATE
			
					EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
					-- insert to temp log table	
					INSERT #tdmMigrationLogEntryWrk
						(ColumnID,ColumnValue,Comment,Duplicate,
						EntityID,SessionKey,Status)
					SELECT 'CustID', CustID, @Message, 1,
						CustID, @iSessionKey,@MIG_STAT_FAILURE
					FROM #tarCustHdrValWrk  WITH (NOLOCK)
					WHERE CustKey IS NOT NULL
					AND Action = @REC_INSERT
					OPTION(KEEP PLAN)
	
					-- Missing for Updates
					SELECT @StringNo = @RPT_STRINGNO_UPDATE_FAILED
			
					EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
					-- insert to temp log table	
					INSERT #tdmMigrationLogEntryWrk
						(ColumnID,ColumnValue,Comment,Duplicate,
						EntityID,SessionKey,Status)
					SELECT 'CustID', CustID, @Message, 1,
						CustID, @iSessionKey,@MIG_STAT_FAILURE
					FROM #tarCustHdrValWrk  WITH (NOLOCK)
					WHERE CustKey IS NULL
					AND Action = @REC_UPDATE
					OPTION(KEEP PLAN)

					-- Missing for Deletions
					SELECT @StringNo = @RPT_STRINGNO_DELETE_FAILED
			
					EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
					-- insert to temp log table	
					INSERT #tdmMigrationLogEntryWrk
						(ColumnID,ColumnValue,Comment,Duplicate,
						EntityID,SessionKey,Status)
					SELECT 'CustID', CustID, @Message, 1,
						CustID, @iSessionKey,@MIG_STAT_FAILURE
					FROM #tarCustHdrValWrk  WITH (NOLOCK)
					WHERE CustKey IS NULL
					AND Action = @REC_DELETE
					OPTION(KEEP PLAN)

					EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT
					TRUNCATE TABLE #tdmMigrationLogEntryWrk
				END

				-- update the work validation tables with the duplicate 		
				UPDATE #tarCustHdrValWrk SET ProcessStatus = @GENRL_PROC_ERR
				FROM #tarCustHdrValWrk WITH (NOLOCK)
				WHERE (CustKey IS NOT NULL AND
				Action = @REC_INSERT)
				OR (CustKey IS NULL AND
				Action IN (@REC_UPDATE, @REC_DELETE))
					OPTION(KEEP PLAN)
				
				UPDATE #tarCustAddrValWrk SET ProcessStatus = @GENRL_PROC_ERR
				FROM #tarCustAddrValWrk CAV WITH (NOLOCK)
				INNER JOIN #tarCustHdrValWrk CHV WITH (NOLOCK)
				ON CAV.CustID = CHV.CustID
				WHERE (CHV.CustKey IS NOT NULL AND
				CHV.Action = @REC_INSERT)
				OR (CHV.CustKey IS NULL AND CAV.AddrKey IS NULL AND
				CHV.Action IN (@REC_UPDATE, @REC_DELETE))
				OPTION(KEEP PLAN)
		
				SELECT @StringNo = NULL
	
				------------------------------------------------------
				-- Mark the failed records in Staging tables
				------------------------------------------------------
	 	
				-- Mark failed record #StgCustomer
				UPDATE a
				SET a.ProcessStatus = @GENRL_PROC_ERR
				FROM #StgCustomer a
				JOIN #tarCustHdrValWrk b
				ON a.RowKey = b.RowKey	
				AND a.SessionKey = @iSessionKey
				AND b.ProcessStatus = @GENRL_PROC_ERR
				OPTION(KEEP PLAN)
	
				-- Mark failed record #StgCustAddr	
				UPDATE a
				SET a.ProcessStatus = @GENRL_PROC_ERR
				FROM #StgCustAddr a
				JOIN #tarCustAddrValWrk b
				ON a.RowKey = b.RowKey	
				AND a.SessionKey = @iSessionKey
				AND b.ProcessStatus = @GENRL_PROC_ERR
				OPTION(KEEP PLAN)
	
				-- Mark failed record #StgCustDocTrnsmit
				UPDATE a
				SET a.ProcessStatus = @GENRL_PROC_ERR
				FROM #StgCustDocTrnsmit a
				JOIN #tarCustHdrValWrk b
				ON a.CustID = b.CustID	
				AND a.SessionKey = @iSessionKey
				AND b.ProcessStatus = @GENRL_PROC_ERR
				OPTION(KEEP PLAN)	

				UPDATE a
				SET a.ProcessStatus = @GENRL_PROC_ERR
				FROM #tarCustDocTrnsmitWrk a
				JOIN #StgCustDocTrnsmit b
				ON a.RowKey = b.RowKey	
				AND b.SessionKey = @iSessionKey
				AND b.ProcessStatus = @GENRL_PROC_ERR
				OPTION(KEEP PLAN)	
	
				-- Mark failed record in the real staging tables if used		
				IF @iUseStageTable = 1
				BEGIN
					-- Mark failed record
					UPDATE a
					SET a.ProcessStatus = @GENRL_PROC_ERR
					FROM StgCustomer a
					JOIN #tarCustHdrValWrk b
					ON a.RowKey = b.RowKey	
					AND a.SessionKey = @iSessionKey
					AND b.ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
		
					UPDATE a
					SET a.ProcessStatus = @GENRL_PROC_ERR
					FROM StgCustAddr a
					INNER JOIN #tarCustAddrValWrk b
					ON a.RowKey = b.RowKey	
					AND a.SessionKey = @iSessionKey
					AND b.ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
	
					UPDATE a
					SET a.ProcessStatus = @GENRL_PROC_ERR
					FROM StgCustDocTrnsmit a
					INNER JOIN #StgCustDocTrnsmit b
					ON a.RowKey = b.RowKey	
					AND a.SessionKey = @iSessionKey
					AND b.ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
	
				END
		
			END --
			------------------------------------------------------------------------
			-- Add to failed and processed record count after pre validation
			------------------------------------------------------------------------	
		  	
		   	SELECT @lRecFailed = @lRecFailed + (SELECT COUNT(*)
		   	FROM #StgCustomer 	
		   	WHERE ProcessStatus = @GENRL_PROC_ERR
			AND SessionKey = @iSessionKey	)
			OPTION(KEEP PLAN)

		   	SELECT @lRecFailed = @lRecFailed + (SELECT COUNT(*)
		   	FROM #StgCustAddr 	
		   	WHERE ProcessStatus = @GENRL_PROC_ERR
			AND SessionKey = @iSessionKey)
			OPTION(KEEP PLAN)

		   	SELECT @lRecFailed = @lRecFailed + (SELECT COUNT(*)
		   	FROM #tarCustDocTrnsmitWrk
			WHERE ProcessStatus = @GENRL_PROC_ERR)
			OPTION(KEEP PLAN)
			
			-- update rows processed count
			SELECT @RowsProcThisCall = @RowsProcThisCall +
				(SELECT COUNT(*) FROM #StgCustomer
				WHERE ProcessStatus = @GENRL_PROC_ERR
				AND SessionKey = @iSessionKey)
				OPTION(KEEP PLAN)

			SELECT @RowsProcThisCall = @RowsProcThisCall +
				(SELECT COUNT(*) FROM #StgCustAddr
				WHERE ProcessStatus = @GENRL_PROC_ERR
				AND SessionKey = @iSessionKey)
				OPTION(KEEP PLAN)

			SELECT @RowsProcThisCall = @RowsProcThisCall +  (SELECT COUNT(*)
		   	FROM #tarCustDocTrnsmitWrk
			WHERE ProcessStatus = @GENRL_PROC_ERR)
			OPTION(KEEP PLAN)

			-- Delete the failed records from validation table
			DELETE #tarCustAddrValWrk
			WHERE ProcessStatus = @GENRL_PROC_ERR
			OPTION(KEEP PLAN)
			-- Delete the failed records from validation table	
			DELETE #tarCustHdrValWrk
			WHERE ProcessStatus = @GENRL_PROC_ERR
			OPTION(KEEP PLAN)	

			-- if there are any rows to process ...
			----------------------------------------------
			-- begin insertion/update/deletion process
			----------------------------------------------

			IF EXISTS (SELECT * FROM  #tarCustHdrValWrk)
			BEGIN
				SELECT @RecordHadErrors = 0
	
				-- Get all the necessary keys
				-- get the CustKey
				SELECT 	@lRowCount = 0
				SELECT 	@lRowCount = COUNT(*)
				FROM 	#tarCustHdrValWrk
				WHERE   Action = @REC_INSERT
				OPTION(KEEP PLAN)
			
				SELECT 	@NoOfKeys = @lRowCount + 1	
	
				IF @NoOfKeys > 1
				BEGIN
					SELECT @KeyStart = 0		
					SELECT @KeyEnd = 0
	
					EXEC spGetNextBlockSurrogateKey 'tarCustomer', @NoOfKeys,
					                                @KeyStart OUTPUT, @KeyEnd   OUTPUT
						
				   	UPDATE 	#tarCustHdrValWrk
					SET	CustKey = @KeyStart,
			        	@KeyStart  = @KeyStart + 1
					WHERE   Action = @REC_INSERT
					OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = @@ERROR
	            		END
			
				UPDATE 	#tarCustAddrValWrk
				SET	CustKey = b.CustKey
				FROM	#tarCustAddrValWrk a
				JOIN	#tarCustHdrValWrk b WITH (NOLOCK)
				ON	a.CustID = b.CustID
				OPTION(KEEP PLAN)
				
				IF @@ERROR <> 0
				SELECT @RecordHadErrors = @@ERROR
				
				IF @RecordHadErrors = 0
				BEGIN
				
				-- get the next AddrKey(s) for the  Address temp table

				
				--Get the record count from the  Address temp table
				SELECT @AddrRowCount = COUNT(*) FROM #tarCustAddrValWrk WHERE Action = @REC_INSERT

				SELECT @AddrRowCount = @AddrRowCount + 1

				IF @AddrRowCount > 0
				BEGIN
					EXEC spGetNextBlockSurrogateKey 'tciAddress',@AddrRowCount,@StartKey OUTPUT,@EndKey OUTPUT
					
					IF @@ERROR <> 0 OR @StartKey IS NULL OR @StartKey = 0
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						RETURN
					END
					
					--SELECT @StartKey = @StartKey - 1					
					-- update the AddrKey Field
					UPDATE #tarCustAddrValWrk
					SET AddrKey = @StartKey, @StartKey = @StartKey +1
					WHERE   Action = @REC_INSERT
				END
		
				-- Get the contact keys					
				-- Get the key for the primary contact record	
				SELECT @CntctRowCount = COUNT(*) FROM #tarCustAddrValWrk
				WHERE RowKey IS NULL
				AND   DfltCntctKey IS NULL
				--AND Action = @REC_INSERT

				SELECT @CntctRowCount = @CntctRowCount + 1
					
				IF @CntctRowCount > 0
				BEGIN
					EXEC spGetNextBlockSurrogateKey 'tciContact',@CntctRowCount,@StartKey OUTPUT,@EndKey OUTPUT
	
					IF @@ERROR <> 0 OR @StartKey IS NULL OR @StartKey = 0
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						RETURN
					END
	
					SELECT @StartKey = @StartKey - 1	
	
					-- update for primary address records
					UPDATE 	#tarCustAddrValWrk
						SET	@StartKey = @StartKey +1,
						DfltCntctKey = @StartKey
					FROM 	#tarCustAddrValWrk
					WHERE 	RowKey IS NULL
						AND   DfltCntctKey IS NULL
					--AND Action = @REC_INSERT
					OPTION(KEEP PLAN)

					-- loop through the remaining Address rows to assign CntctKey
					WHILE (SELECT COUNT(*) FROM #tarCustAddrValWrk WHERE DfltCntctKey IS NULL
						AND RowKey IS NOT NULL AND Name IS NOT NULL AND Action = @REC_INSERT) > 0
					BEGIN
						-- initialize the @CntctKey to null
						SELECT @CntctKey = NULL,@CustAddrRowKey = NULL, @Name = NULL
	
						SELECT @CustAddrRowKey = MIN(CtrlRowKey)
						FROM #tarCustAddrValWrk  WITH (NOLOCK) WHERE
						DfltCntctKey IS NULL
						AND RowKey IS NOT NULL
						AND Name IS NOT NULL
						--AND Action = @REC_INSERT
						OPTION(KEEP PLAN)
	
						SELECT @Name = Name FROM #tarCustAddrValWrk
						WHERE CtrlRowKey = @CustAddrRowKey
						OPTION(KEEP PLAN)
	
						-- if the name is the same, as an existing row in the temp table,
						-- must use the same CntctKey
						SELECT @CntctKey = b.DfltCntctKey, @Name = a.Name
						FROM #tarCustAddrValWrk a WITH (NOLOCK)
						INNER JOIN #tarCustAddrValWrk b WITH (NOLOCK)
						ON LTRIM(RTRIM(a.Name)) = LTRIM(RTRIM(b.Name))
						WHERE a.CtrlRowKey = @CustAddrRowKey
						AND b.CtrlRowKey <> @CustAddrRowKey
						AND b.RowKey IS NULL
						AND b.DfltCntctKey IS NOT NULL
						AND b.CustID = a.CustID
						OPTION(KEEP PLAN)						
	
						IF (@CntctKey IS NULL OR @CntctKey = 0) AND @Name IS NOT NULL
						BEGIN
						
							-- get the contact key for DfltCntctKey
							EXEC spGetNextSurrogateKey 'tciContact',@CntctKey OUTPUT
	
							IF @@ERROR <> 0 OR @CntctKey IS NULL OR @CntctKey = 0
							BEGIN
								SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
								RETURN
							END
	
									
						END --IF @CntctKey IS NULL OR @CntctKey = 0
						ELSE
						BEGIN
							UPDATE #tarCustAddrValWrk SET Name = NULL
							WHERE CtrlRowKey = @CustAddrRowKey
							OPTION(KEEP PLAN)
	
						END

						UPDATE 	#tarCustAddrValWrk
						SET 	DfltCntctKey = @CntctKey
						WHERE 	CtrlRowKey = @CustAddrRowKey
						OPTION(KEEP PLAN)

	    					IF @@ERROR <> 0
	    						BEGIN
	    							SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
	    	
	    						END
						END -- WHILE ...
			END -- IF @CntctRowCount > 1
				END -- IF @RecordHadErrors = 0
	
				-- set up the Addr Keys for the customer record
				UPDATE #tarCustHdrValWrk
				SET PrimaryAddrKey = AddrKey
				FROM #tarCustHdrValWrk a WITH (NOLOCK)
				INNER JOIN #tarCustAddrValWrk  b WITH (NOLOCK)
				ON a.CustKey = b.CustKey
				AND b.RowKey IS NULL
				OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = @@ERROR

				-- if billto or shipto addrkeys are the same as the primay the use the primary addrkey
				-- DfltBillToAddrKey
				UPDATE a
				SET a.DfltBillToAddrKey = CASE
					WHEN a.DfltBillToAddrID = a.CustID THEN a.PrimaryAddrKey
					ELSE b.AddrKey
					END
				FROM #tarCustHdrValWrk a WITH (NOLOCK)
				INNER JOIN #tarCustAddrValWrk  b WITH (NOLOCK)
				ON b.CustAddrID =  a.DfltBillToAddrID
				AND a.CustKey = b.CustKey
				OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = @@ERROR
				-- DfltShipToAddrKey
				UPDATE a
				SET a.DfltShipToAddrKey = CASE
					WHEN a.DfltShipToAddrID = a.CustID THEN a.PrimaryAddrKey
					ELSE b.AddrKey
					END
				FROM #tarCustHdrValWrk a WITH (NOLOCK)
				INNER JOIN #tarCustAddrValWrk  b WITH (NOLOCK)
				ON b.CustAddrID =  a.DfltShipToAddrID
				AND a.CustKey = b.CustKey
				OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = @@ERROR

				-- update the header with the primary contact key
				UPDATE a
				SET a.PrimaryCntctKey = b.DfltCntctKey
				FROM #tarCustHdrValWrk a WITH (NOLOCK)
				INNER JOIN #tarCustAddrValWrk  b WITH (NOLOCK)
				ON a.CustKey = b.CustKey
				AND b.RowKey IS NULL
				OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = @@ERROR
				
				BEGIN TRAN	
	
				IF @RecordHadErrors = 0
				BEGIN
	
				-- insert the address(es) into tciAddress from the temptable
					INSERT tciAddress (AddrKey, AddrLine1, AddrLine2, AddrLine3, AddrLine4,
					AddrLine5, AddrName, City, CountryID, CRMAddrID, PostalCode, StateID,Residential)
					SELECT AddrKey, COALESCE(AddrLine1,''), COALESCE(AddrLine2,''), COALESCE(AddrLine3,''), COALESCE(AddrLine4,''),
					COALESCE(AddrLine5,''), AddrName, City, CountryID, CRMAddrID, PostalCode, StateID,Residential
					FROM #tarCustAddrValWrk WITH (NOLOCK)
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_INSERT
					OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
	
					-- update the address(es) into tciAddress from the temptable
					UPDATE tciAddress
					SET 	AddrLine1 = COALESCE(#tarCustAddrValWrk.AddrLine1,''),
						AddrLine2 = COALESCE(#tarCustAddrValWrk.AddrLine2,''),
						AddrLine3 = COALESCE(#tarCustAddrValWrk.AddrLine3,''),
						AddrLine4 = COALESCE(#tarCustAddrValWrk.AddrLine4,''),
						       AddrLine5 = COALESCE(#tarCustAddrValWrk.AddrLine5,''),
						AddrName = #tarCustAddrValWrk.AddrName,
						City = #tarCustAddrValWrk.City,
						CountryID = #tarCustAddrValWrk.CountryID,
						CRMAddrID = #tarCustAddrValWrk.CRMAddrID,
						PostalCode = #tarCustAddrValWrk.PostalCode,
						StateID = #tarCustAddrValWrk.StateID,
						Residential = #tarCustAddrValWrk.Residential,
						UpdateCounter = UpdateCounter + 1
					FROM tciAddress WITH (NOLOCK)
					JOIN #tarCustAddrValWrk WITH (NOLOCK)
					ON (tciAddress.AddrKey = #tarCustAddrValWrk.AddrKey)
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_UPDATE
					OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
				
					-- delete the address(es) from tciAddress from the temptable
					DELETE tciAddress
					FROM tciAddress WITH (NOLOCK)
						  JOIN #tarCustAddrValWrk WITH (NOLOCK)
					ON (tciAddress.AddrKey = #tarCustAddrValWrk.AddrKey)
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_DELETE
	 	  			 OPTION(KEEP PLAN)

			 IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
				END    -- IF @RecordHadErrors = 0


				-- insert the primary contact
				IF @RecordHadErrors = 0
				BEGIN	
	
					INSERT tciContact(
					CntctKey, CntctOwnerKey, CreateType,
					EMailAddr, EntityType, Fax, FaxExt,
					ImportLogKey, Name, Phone, PhoneExt, Title,
					CreateDate, CreateUserID, UpdateDate, UpdateUserID, CRMContactID)
					SELECT DfltCntctKey, CustKey, @CREATE_TYPE_MIGRATE,
					EMailAddr, @ENTITYTYPE_CUST, Fax, FaxExt,
					NULL, Name, Phone, PhoneExt, Title,
					@CreateDate, @UserID, @CreateDate, @UserID, CRMContactID
					FROM #tarCustAddrValWrk  WITH (NOLOCK) WHERE
					DfltCntctKey IS NOT NULL
					AND Name IS NOT NULL
					AND ProcessStatus = @NOT_PROCESSED
					AND DfltCntctKey NOT IN (SELECT CntctKey FROM tciContact)
					--AND Action = @REC_INSERT
					OPTION(KEEP PLAN)
					
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

			-- update the primary contact
					UPDATE tciContact
					SET EMailAddr = avw.EMailAddr,
						Fax = avw.Fax,
						FaxExt = avw.FaxExt,
						Phone = avw.Phone,
						PhoneExt = avw.PhoneExt,
						Title = avw.Title,
						CRMContactID = avw.CRMContactID,
						UpdateDate = @CreateDate,
						UpdateUserID = @UserID,
						UpdateCounter = UpdateCounter + 1
					FROM tciContact WITH (NOLOCK)
					JOIN #tarCustAddrValWrk  as avw WITH (NOLOCK)
					ON (DfltCntctKey = CntctKey)
					WHERE avw.DfltCntctKey IS NOT NULL
					AND avw.Name IS NOT NULL
					AND avw.ProcessStatus = @NOT_PROCESSED
					AND avw.Action = @REC_UPDATE
					 OPTION(KEEP PLAN)
					
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

					-- delete the primary contact
					DELETE tciContact
					FROM #tarCustAddrValWrk  WITH (NOLOCK)
					WHERE DfltCntctKey = CntctKey
					AND ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_DELETE
					OPTION(KEEP PLAN)
					
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
				END	        -- IF @RecordHadErrors = 0
	
				IF @RecordHadErrors = 0
				BEGIN
	
					-- insert the customer record
	
					INSERT tarCustomer(
					ABANo, AllowCustRefund, AllowWriteOff, BillingType,
					CompanyID, CreateType, CreditLimit, CreditLimitAgeCat,DfltSalesReturnAcctKey,
					CreditLimitUsed, CRMCustID, CurrExchSchdKey, CustClassKey, CustID, CustKey, CustName,
				    CustRefNo, DateEstab, DfltBillToAddrKey, DfltItemKey, DfltSalesAcctKey, DfltShipToAddrKey,
					FinChgFlatAmt, FinChgPct, Hold, ImportLogKey, NationalAcctLevelKey, PmtByNationalAcctParent,
					PrimaryAddrKey, PrimaryCntctKey, PrintDunnMsg, ReqPO, SalesSourceKey,
					ShipPriority, Status, StdIndusCodeID, StmtCycleKey, StmtFormKey, TradeDiscPct,
					UserFld1, UserFld2, UserFld3, UserFld4,
					VendKey, CreateDate, CreateUserID, UpdateDate, UpdateUserID)
					SELECT
					COALESCE(ABANo,''), AllowCustRefund, AllowWriteOff, BillingType,
					@iCompanyID, @CREATE_TYPE_MIGRATE, CreditLimit, CreditLimitAgeCat,DfltSalesReturnAcctKey,
					CreditLimitUsed, CRMCustID, CurrExchSchdKey, CustClassKey, CustID, CustKey,
					COALESCE(CustName,''),
					CustRefNo, DateEstab, DfltBillToAddrKey, DfltItemKey, DfltSalesAcctKey, DfltShipToAddrKey,
					FinChgFlatAmt, COALESCE(FinChgPct,0)/100, Hold, NULL /*ImportLogKey*/, NULL, '0',
					PrimaryAddrKey, PrimaryCntctKey, PrintDunnMsg, ReqPO, SalesSourceKey,
					ShipPriority, Status, StdIndusCodeID, StmtCycleKey, StmtFormKey, COALESCE(TradeDiscPct,0)/100,
					UserFld1, UserFld2, UserFld3, UserFld4,
					VendKey, @CreateDate, @UserID, @CreateDate, @UserID
					FROM #tarCustHdrValWrk
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_INSERT
					OPTION(KEEP PLAN)
			
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
	
-- update the customer record
					UPDATE c
					SET c.ABANo = hdr.ABANo,
					c.AllowCustRefund = hdr.AllowCustRefund,
					c.AllowWriteOff = hdr.AllowWriteOff,
					c.BillingType = hdr.BillingType,
					c.CreditLimit = COALESCE(CONVERT(DECIMAL(12,0), hdr.CreditLimit), 0),
					c.CreditLimitAgeCat = hdr.CreditLimitAgeCat,
					c.CreditLimitUsed = hdr.CreditLimitUsed,
					c.CRMCustID = hdr.CRMCustID,
					c.CurrExchSchdKey = hdr.CurrExchSchdKey,
					c.CustClassKey = hdr.CustClassKey,
					c.CustID = hdr.CustID,
					c.CustName = hdr.CustName,
					c.CustRefNo = hdr.CustRefNo,
					c.DateEstab = hdr.DateEstab,
					c.DfltBillToAddrKey = hdr.DfltBillToAddrKey,
					c.DfltItemKey = hdr.DfltItemKey,
					c.DfltSalesAcctKey = hdr.DfltSalesAcctKey,
					c.DfltSalesReturnAcctKey = hdr.DfltSalesReturnAcctKey,
					c.DfltShipToAddrKey = hdr.DfltShipToAddrKey,
					c.FinChgFlatAmt = hdr.FinChgFlatAmt,
					c.FinChgPct = hdr.FinChgPct/100,
					c.Hold = hdr.Hold,
					c.PrimaryAddrKey = hdr.PrimaryAddrKey,
					c.PrimaryCntctKey = hdr.PrimaryCntctKey,
					c.PrintDunnMsg = hdr.PrintDunnMsg,
					c.ReqPO = hdr.ReqPO,
					c.SalesSourceKey = hdr.SalesSourceKey,
					c.ShipPriority = hdr.ShipPriority,
					c.Status = hdr.Status,
					c.StdIndusCodeID = hdr.StdIndusCodeID,
					c.StmtCycleKey = hdr.StmtCycleKey,
					c.StmtFormKey = hdr.StmtFormKey,
					c.TradeDiscPct = hdr.TradeDiscPct/100,
					c.UpdateDate = @CreateDate,
					c.UpdateUserID = @UserID,
					c.UpdateCounter = c.UpdateCounter + 1,
					c.UserFld1 = hdr.UserFld1,
					c.UserFld2 = hdr.UserFld2,
					c.UserFld3 = hdr.UserFld3,
					c.UserFld4 = hdr.UserFld4,
					c.VendKey = hdr.VendKey
					FROM tarCustomer c WITH (NOLOCK)
					JOIN #tarCustHdrValWrk as hdr WITH (NOLOCK)
					ON (c.CustKey = hdr.CustKey)
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_UPDATE
					OPTION(KEEP PLAN)
			
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

					-- update the customer record
					DELETE tarCustomer
					FROM #tarCustHdrValWrk WITH (NOLOCK)
					WHERE tarCustomer.CustKey = #tarCustHdrValWrk.CustKey
					AND ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_DELETE
					OPTION(KEEP PLAN)
			
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
				END	     -- IF @RecordHadErrors = 0
				
				IF @RecordHadErrors = 0
				BEGIN					
					-- insert the tarCustomerRecord
					INSERT tarCustAddr(
					AddrKey, AllowInvtSubst, BackOrdPrice, CommPlanKey, CreateType,
						    CloseSOLineOnFirstShip,CloseSOOnFirstShip,
					CurrExchSchdKey, CurrID, CustAddrID, CustKey, CustPriceGroupKey,
					DfltCntctKey, FOBKey, ImportLogKey, InvcFormKey, InvcMsg, LanguageID,
					PackListFormKey, PmtTermsKey,
PriceAdj,
PriceBase, PrintOrderAck, RequireSOAck,
					SalesTerritoryKey, ShipComplete, ShipDays, ShipLabelFormKey, ShipMethKey,
					SOAckFormKey, SperKey, STaxSchdKey, WhseKey,BOLReqd,InvoiceReqd,PackListReqd,
						    PackListContentsReqd,ShipLabelsReqd,CarrierAcctNo,CarrierBillMeth,FreightMethod,
					CreateDate, CreateUserID, UpdateDate, UpdateUserID)
					SELECT
					AddrKey, AllowInvtSubst, 0, CommPlanKey, @CREATE_TYPE_MIGRATE,
		        			CloseSOLineOnFirstShip, CloseSOOnFirstShip, 	
						    CurrExchSchdKey, CurrID, CustAddrID, CustKey, CustPriceGroupKey,
					DfltCntctKey, FOBKey, ImportLogKey, InvcFormKey, InvcMsg, @LanguageID,
					PackListFormKey, PmtTermsKey,
Case
When PriceAdj =0 then 0
Else PriceAdj /100
End,
PriceBase, PrintOrderAck, RequireSOAck,
					SalesTerritoryKey, ShipComplete, ShipDays, ShipLabelFormKey, ShipMethKey,
					SOAckFormKey, SperKey, STaxSchdKey, WhseKey,BOLReqd,InvoiceReqd,PackListReqd,
						    PackListContentsReqd,ShipLabelsReqd,CarrierAcctNo,CarrierBillMeth,FreightMethod,
					@CreateDate, @UserID, @CreateDate, @UserID
					FROM #tarCustAddrValWrk WITH (NOLOCK)
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_INSERT
					OPTION(KEEP PLAN)
	
			IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

					-- update the tarCustAddr Record
					UPDATE ca
					SET ca.AllowInvtSubst = avw.AllowInvtSubst,
					ca.BOLReqd = avw.BOLReqd,
					ca.CarrierAcctNo = avw.CarrierAcctNo,
					ca.CarrierBillMeth = avw.CarrierBillMeth,
					ca.CloseSOLineOnFirstShip = avw.CloseSOLineOnFirstShip,
					ca.CloseSOOnFirstShip = avw.CloseSOOnFirstShip,
					ca.CommPlanKey = avw.CommPlanKey,
					ca.CurrExchSchdKey = avw.CurrExchSchdKey,
					ca.CurrID = avw.CurrID,
					ca.CustAddrID = avw.CustAddrID,
					ca.CustPriceGroupKey = avw.CustPriceGroupKey,
					ca.DfltCntctKey = avw.DfltCntctKey,
					ca.FOBKey = avw.FOBKey,
					ca.FreightMethod = avw.FreightMethod,
					ca.InvcFormKey = avw.InvcFormKey,
					ca.InvcMsg = avw.InvcMsg,
					ca.InvoiceReqd = avw.InvoiceReqd,
					ca.PackListContentsReqd = avw.PackListContentsReqd,
					ca.PackListFormKey = avw.PackListFormKey,
					ca.PackListReqd = avw.PackListReqd,
					ca.PmtTermsKey = avw.PmtTermsKey,
					ca.PriceAdj = case
when avw.PriceAdj =0 then 0
else avw.PriceAdj/100
end,
					ca.PriceBase = avw.PriceBase,
					ca.PrintOrderAck = avw.PrintOrderAck,
					ca.RequireSOAck = avw.RequireSOAck,
					ca.SalesTerritoryKey = avw.SalesTerritoryKey,
					ca.ShipComplete = avw.ShipComplete,
					ca.ShipDays = avw.ShipDays,
					ca.ShipLabelFormKey = avw.ShipLabelFormKey,
					ca.ShipLabelsReqd = avw.ShipLabelsReqd,
					ca.ShipMethKey = avw.ShipMethKey,
					ca.SOAckFormKey = avw.SOAckFormKey,
					ca.SperKey = avw.SperKey,
					ca.STaxSchdKey = avw.STaxSchdKey,
					ca.UpdateDate = @CreateDate,
					ca.UpdateUserID = @UserID,
					ca.WhseKey = avw.WhseKey
					FROM tarCustAddr ca WITH (NOLOCK)
					JOIN #tarCustAddrValWrk as avw WITH (NOLOCK)
					ON (ca.AddrKey = avw.AddrKey)
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_UPDATE
					OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

					-- DELETE the tarCustAddr Record
					DELETE tarCustAddr
					FROM #tarCustAddrValWrk as avw WITH (NOLOCK)
					WHERE tarCustAddr.AddrKey = avw.AddrKey
					AND ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_DELETE
					OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
				END    -- IF @RecordHadErrors = 0
	
				-- Insert a row into tarCustStatus  (Nothing to Update, just insert/delete
				IF @RecordHadErrors = 0
				BEGIN					
	
					INSERT tarCustStatus(
					AgeCat1Amt, AgeCat2Amt, AgeCat3Amt, AgeCat4Amt, AgeCurntAmt, AgeFutureAmt,
					AgingDate, AvgDaysPastDue, AvgDaysToPay, AvgInvcAmt, CustKey, FinChgBal,
					HighestBal, HighestInvcKey, LastStmtAmt, LastStmtAmtNC, LastStmtDate,
					NoInvcsInInvcAvg, NoInvcsInPmtAvg, OnSalesOrdAmt, RetntBal)
					SELECT
					0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, CustKey, 0,
					0, NULL, 0, 0, 0,
					0, 0, 0, 0
					FROM #tarCustHdrValWrk
					WHERE ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_INSERT
					OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
	
					DELETE tarCustStatus
					FROM #tarCustHdrValWrk
					WHERE tarCustStatus.CustKey = #tarCustHdrValWrk.CustKey
					AND ProcessStatus = @NOT_PROCESSED
					AND Action = @REC_DELETE
					OPTION(KEEP PLAN)
	
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
				END    -- IF @RecordHadErrors = 0

				-- insert the doc transmit records
				IF @RecordHadErrors = 0
				BEGIN					
					INSERT tarCustDocTrnsmit
					(CustKey, EMail, EMailFormat, Fax, HardCopy, TranType)
					SELECT b.CustKey, EMail, EMailFormat, a.Fax, HardCopy, a.TranType
					FROM #StgCustDocTrnsmit a WITH (NOLOCK)
					INNER JOIN #tarCustHdrValWrk b WITH (NOLOCK)
					ON a.CustID = b.CustID
					WHERE b.ProcessStatus = @NOT_PROCESSED
					AND a.Action = @REC_INSERT
					OPTION(KEEP PLAN)
					
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

			-- update the doc transmit records	
					UPDATE tarCustDocTrnsmit
					SET 	EMail = a.EMail,               EMailFormat = a.EMailFormat,
						Fax = a.Fax,                   HardCopy = a.HardCopy
					FROM tarCustDocTrnsmit doc WITH (NOLOCK)
					INNER JOIN #tarCustHdrValWrk b WITH (NOLOCK)
					ON (b.CustKey = doc.CustKey)
					INNER JOIN #StgCustDocTrnsmit a WITH (NOLOCK)
					ON (a.CustID = b.CustID AND
					a.TranType = doc.TranType)
					WHERE b.ProcessStatus = @NOT_PROCESSED
					AND a.Action = @REC_UPDATE
					OPTION(KEEP PLAN)
					
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

					-- delete the doc transmit records	
					DELETE tarCustDocTrnsmit
					FROM tarCustDocTrnsmit doc WITH (NOLOCK)
					INNER JOIN #tarCustHdrValWrk b WITH (NOLOCK)
					ON (b.CustKey = doc.CustKey)
					INNER JOIN #StgCustDocTrnsmit a WITH (NOLOCK)
					ON (a.CustID = b.CustID AND
					a.TranType = doc.TranType)
					WHERE b.ProcessStatus = @NOT_PROCESSED
					AND a.Action = @REC_DELETE
					OPTION(KEEP PLAN)
					
					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
				END    -- IF @RecordHadErrors = 0
			
				IF @RecordHadErrors = 1-- an error occured
				   BEGIN
					ROLLBACK TRANSACTION
					SELECT @ProcessStatus = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
				   END
				ELSE			-- an error did not occur
				   BEGIN
					COMMIT TRANSACTION -- We commit the transaction here
					SELECT @ProcessStatus = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
				   END
	
			END -- Validation was TRUE
	
				--------------------------------------------
				-- Write Migration Log record if appropriate
				--------------------------------------------
			IF 	(@iRptOption = 	@RPT_PRINT_ALL OR
				@iRptOption =  @RPT_PRINT_SUCCESSFUL) AND @ProcessStatus = @MIG_STAT_SUCCESSFUL
	
			BEGIN
				-- Default @Message to NULL string				
				SELECT @Message = NULL
						
				-- Lookup the comment if one was specified
					IF @StringNo IS NOT NULL
						BEGIN
							EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
						END
	
				
					TRUNCATE TABLE #tdmMigrationLogEntryWrk
				
					
				-- Write out to the final temp table for permenent error log table
				-- Header record	
				INSERT	#tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Duplicate,Comment,
				EntityID,Status,SessionKey)
				SELECT '','',0, CASE WHEN ProcessStatus <> @GENRL_PROC_ERR AND Action = 1 THEN 'Record has been inserted.'
								WHEN ProcessStatus <> @GENRL_PROC_ERR AND Action = 2 THEN 'Record has been updated.'
								WHEN ProcessStatus <> @GENRL_PROC_ERR AND Action = 3 THEN 'Record has been deleted.'
								ELSE @Message END ,
				CustID,	@ProcessStatus, @iSessionKey
				FROM	#tarCustHdrValWrk
				OPTION(KEEP PLAN)

				-- Address record
				INSERT	#tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Duplicate,Comment,
				EntityID,Status,SessionKey)
				SELECT '','',0,  CASE WHEN ProcessStatus <> @GENRL_PROC_ERR AND Action = 1 THEN 'Record has been inserted.'
								WHEN ProcessStatus <> @GENRL_PROC_ERR AND Action = 2 THEN 'Record has been updated.'
								WHEN ProcessStatus <> @GENRL_PROC_ERR AND Action = 3 THEN 'Record has been deleted.'
								ELSE @Message END ,
				COALESCE(RTRIM(LTRIM(CustID)), '') + '|' + COALESCE(RTRIM(LTRIM(CustAddrID)), ''),@ProcessStatus, @iSessionKey
				FROM	#tarCustAddrValWrk WHERE RowKey IS NOT NULL
				OPTION(KEEP PLAN)

				INSERT	#tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Duplicate,Comment,
				EntityID,Status,SessionKey)
				SELECT '','',0, CASE WHEN c.Action = 1 THEN 'Record has been inserted.'
								WHEN c.Action = 2 THEN 'Record has been updated.'
								WHEN c.Action = 3 THEN 'Record has been deleted.'
								END ,
				COALESCE(RTRIM(LTRIM(a.CustID)),'') + '|' + RTRIM(LTRIM(a.TranType)),@ProcessStatus, @iSessionKey
				FROM	#StgCustDocTrnsmit a
				INNER JOIN #tarCustDocTrnsmitWrk b
				ON a.RowKey = b.RowKey
				INNER JOIN #tarCustHdrValWrk c
				ON a.CustID = C.CustID
				WHERE a.ProcessStatus <> @GENRL_PROC_ERR
				OPTION(KEEP PLAN)	
						
				EXEC spdmCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

				TRUNCATE TABLE #tdmMigrationLogEntryWrk
	
			END -- Reporting was requested
			
			IF @ProcessStatus = @MIG_STAT_FAILURE
			BEGIN
				SELECT @lRecFailed = @lRecFailed +
					(SELECT COUNT(*) FROM #tarCustHdrValWrk)
					OPTION(KEEP PLAN)
				
				SELECT @lRecFailed = @lRecFailed +
					(SELECT COUNT(*) FROM #tarCustAddrValWrk
					WHERE RowKey IS NOT NULL)
					OPTION(KEEP PLAN)


				SELECT @lRecFailed = @lRecFailed +
					(SELECT COUNT(*) FROM #StgCustDocTrnsmit a
					INNER JOIN #tarCustDocTrnsmitWrk b
					ON a.RowKey = b.RowKey WHERE
					a.ProcessStatus = @NOT_PROCESSED
					AND a.SessionKey = @iSessionKey)
					OPTION(KEEP PLAN)	
			END
	
			SELECT @RowsProcThisCall = @RowsProcThisCall +
				(SELECT COUNT(*) FROM #tarCustHdrValWrk)
				OPTION(KEEP PLAN)
	
			SELECT @RowsProcThisCall = @RowsProcThisCall +
				(SELECT COUNT(*) FROM #tarCustAddrValWrk WHERE RowKey IS NOT NULL)
				OPTION(KEEP PLAN)			
			
			SELECT @RowsProcThisCall = @RowsProcThisCall +
				(SELECT COUNT(*) FROM #StgCustDocTrnsmit a
				INNER JOIN #tarCustDocTrnsmitWrk b
				ON a.RowKey = b.RowKey WHERE
				a.ProcessStatus = @NOT_PROCESSED
				AND a.SessionKey = @iSessionKey)
				OPTION(KEEP PLAN)	

			------------------------------------------------------------------------------------
			-- If using staging tables, then mark failed records with the failed
			-- record processing status and delete successful rows.
			------------------------------------------------------------------------------------
			IF @iUseStageTable = 1
			BEGIN -- using Staging tables
			
				IF @ProcessStatus = @MIG_STAT_FAILURE
				BEGIN
				
					UPDATE StgCustomer SET ProcessStatus = @GENRL_PROC_ERR-- did not process
					FROM StgCustomer WITH (NOLOCK)
					INNER JOIN #tarCustHdrValWrk tmp
					ON StgCustomer.RowKey = tmp.RowKey
					WHERE StgCustomer.SessionKey = @iSessionKey
					OPTION(KEEP PLAN)
	
					UPDATE StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR-- did not process
					FROM StgCustAddr WITH (NOLOCK)
					INNER JOIN #tarCustAddrValWrk TCA
					ON StgCustAddr.RowKey = TCA.RowKey
					WHERE TCA.ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
					
					UPDATE StgCustDocTrnsmit SET ProcessStatus = @GENRL_PROC_ERR-- did not process
					FROM StgCustDocTrnsmit WITH (NOLOCK)
					INNER JOIN #tarCustHdrValWrk tmp
					ON StgCustDocTrnsmit.CustID = tmp.CustID
					WHERE StgCustDocTrnsmit.SessionKey = @iSessionKey
					AND tmp.ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
				
			   	END -- using staging table
				ELSE-- @MIG_STAT_SUCCESS
				
				BEGIN
	
					DELETE StgCustomer
					FROM StgCustomer C WITH (NOLOCK)
					INNER JOIN #tarCustHdrValWrk tmp WITH (NOLOCK)
					ON C.RowKey = tmp.RowKey
					WHERE C.SessionKey = @iSessionKey
					AND tmp.ProcessStatus <> @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
				
					DELETE StgCustAddr
					FROM StgCustAddr C WITH (NOLOCK)
					INNER JOIN #tarCustAddrValWrk TCA WITH (NOLOCK)
					ON C.RowKey = TCA.RowKey
					WHERE TCA.ProcessStatus <> @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
		
					DELETE StgCustDocTrnsmit
					FROM StgCustDocTrnsmit C
					INNER JOIN #tarCustHdrValWrk tmp WITH (NOLOCK)
					ON C.CustID = tmp.CustID
					WHERE C.SessionKey = @iSessionKey
					AND tmp.ProcessStatus <> @GENRL_PROC_ERR
					AND C.ProcessStatus <> @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
	
				END -- Sucessful
	
			END-- IF using Staging tables
					
			-------------------------------------------------------------------
			-- Process the temp tables
			-------------------------------------------------------------------
	
			IF @ProcessStatus = @MIG_STAT_FAILURE-- did not process
			BEGIN
		
				UPDATE #StgCustomer SET ProcessStatus = @GENRL_PROC_ERR
				FROM #StgCustomer  C WITH (NOLOCK)
				INNER JOIN #tarCustHdrValWrk tmp
				ON C.RowKey = tmp.RowKey
				WHERE C.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)
	
				UPDATE #StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
				FROM #StgCustAddr C WITH (NOLOCK)
				INNER JOIN #tarCustAddrValWrk TCA WITH (NOLOCK)
				ON C.RowKey = TCA.RowKey
				WHERE TCA.ProcessStatus = @GENRL_PROC_ERR
				AND C.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)
	
				UPDATE #StgCustDocTrnsmit SET ProcessStatus = @GENRL_PROC_ERR
				FROM #StgCustDocTrnsmit  C WITH (NOLOCK)
				INNER JOIN #tarCustHdrValWrk tmp
				ON C.CustID = tmp.CustID
				WHERE C.SessionKey = @iSessionKey
				OPTION(KEEP PLAN)
	
			END
			ELSE
			BEGIN
				DELETE #StgCustomer
				FROM #StgCustomer WITH (NOLOCK)
				INNER JOIN #tarCustHdrValWrk tmp
				ON #StgCustomer.RowKey = tmp.RowKey
				WHERE #StgCustomer.SessionKey = @iSessionKey
				AND tmp.ProcessStatus <> @GENRL_PROC_ERR
				OPTION(KEEP PLAN)
	
				DELETE #StgCustAddr
				FROM #StgCustAddr WITH (NOLOCK)
				INNER JOIN #tarCustAddrValWrk TCA WITH (NOLOCK)
				ON #StgCustAddr.RowKey = TCA.RowKey
				WHERE TCA.ProcessStatus <> @GENRL_PROC_ERR
				OPTION(KEEP PLAN)
	
				DELETE #StgCustDocTrnsmit
				FROM #StgCustDocTrnsmit  C WITH (NOLOCK)
				INNER JOIN #tarCustHdrValWrk tmp
				ON C.CustID = tmp.CustID
				WHERE C.SessionKey = @iSessionKey
				AND tmp.ProcessStatus <> @GENRL_PROC_ERR
				OPTION(KEEP PLAN)
	
			END -- IF @SourceRowCount = 0
	
			--Clear the tempTable
			TRUNCATE TABLE #tarCustAddrValWrk
			TRUNCATE TABLE #tarCustHdrValWrk
			
		END


		-- Determine if any remaining rows exist for insertion.
		--        If not, then consider this successful.
		SELECT @SourceRowCount = COUNT(*)
			FROM #StgCustomer WITH (NOLOCK)
			WHERE ProcessStatus = @NOT_PROCESSED	
		OPTION (KEEP PLAN)

	END -- WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS
	
	-- Keep SP caller informed of how many rows were processed.
	SELECT @oRecsProcessed = @oRecsProcessed + @RowsProcThisCall,
		@oFailedRecs = @oFailedRecs + @lRecFailed

	SELECT @_oRetVal = @RET_SUCCESS,@RowsProcThisCall =0, @lRecFailed = 0


	IF @iUseStageTable = 1
	BEGIN
		IF (SELECT COUNT(*) FROM StgCustomer WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED) > 0
		

			SELECT @oContinue = 1
		ELSE
			SELECT @oContinue = 0

	END
	ELSE
	BEGIN
		IF (SELECT COUNT(*) FROM #StgCustomer WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED) > 0
		

			SELECT @oContinue = 1
		ELSE
			SELECT @oContinue = 0
	END

	IF @oContinue = 1
	BEGIN
		
		SELECT @_oRetVal = @RET_SUCCESS
		RETURN
	END

	SELECT @RowsProcThisCall = 0, @lRecFailed = 0

	-----------------------------------------------------------
	-- Start Insert for other Other StgCustAddr Records
	-----------------------------------------------------------
	SELECT @StartDate = GetDate()

	-- determine if there are any rows in (#)StgCustAddr to process
	IF @iUseStageTable = 1
	BEGIN
		SELECT @SourceRowCount = COUNT(*) FROM StgCustAddr WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
	END
	ELSE
	BEGIN
		SELECT @SourceRowCount = COUNT(*) FROM #StgCustAddr WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
	END	

	WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS AND @SourceRowCount > 0
	BEGIN
	
		IF @iUseStageTable = 1
		BEGIN
			TRUNCATE TABLE #StgCustAddr

			SET IDENTITY_INSERT #StgCustAddr ON

			SET ROWCOUNT @MAX_ROWS_PER_RUN
		
			INSERT #StgCustAddr
			(RowKey,AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName,AllowInvtSubst,City
			,CloseSOLineOnFirstShip, CloseSOOnFirstShip,CommPlanID,CountryID,CurrExchSchdID,CurrID,CustAddrID,CustID,CustPriceGroupID,EMailAddr,Fax
			,FaxExt,FOBID,InvcFormID,InvcMsg,LanguageID,Name,PackListFormID,Phone,PhoneExt,PmtTermsID
			,PostalCode,PriceBase,PrintOrderAck,RequireSOAck,SalesTerritoryID,ShipComplete,ShipDays,ShipLabelFormID
			,ShipMethID,SOAckFormID,SperID,StateID,STaxSchdID,Title,WhseID,BOLReqd
			,InvoiceReqd,PackListReqd,PackListContentsReqd,ShipLabelsReqd,Residential,CarrierAcctNo,CarrierBillMeth,FreightMethod
			,ProcessStatus,SessionKey,PriceAdj,Action,CRMContactID,CRMAddrID)

			SELECT RowKey,AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName,AllowInvtSubst,City
			,CloseSOLineOnFirstShip, CloseSOOnFirstShip,CommPlanID,CountryID,CurrExchSchdID,CurrID,CustAddrID,CustID,CustPriceGroupID,EMailAddr,Fax
			,FaxExt,FOBID,InvcFormID,InvcMsg,LanguageID,Name,PackListFormID,Phone,PhoneExt,PmtTermsID
			,PostalCode,PriceBase,PrintOrderAck,RequireSOAck,SalesTerritoryID,ShipComplete,ShipDays,ShipLabelFormID
			,ShipMethID,SOAckFormID,SperID,StateID,STaxSchdID,Title,WhseID,BOLReqd
			,InvoiceReqd,PackListReqd,PackListContentsReqd,ShipLabelsReqd,Residential,CarrierAcctNo,CarrierBillMeth,FreightMethod
			,ProcessStatus,SessionKey,coalesce(PriceAdj,0),Action,CRMContactID,CRMAddrID
			FROM StgCustAddr WITH (NOLOCK)
			WHERE SessionKey = @iSessionKey
			AND ProcessStatus = @NOT_PROCESSED
			OPTION(KEEP PLAN)

			SET ROWCOUNT 0
	
			SET IDENTITY_INSERT #StgCustAddr OFF
		END

		--UPDATE Action flag to 'insert' if it is null
		UPDATE 	#StgCustAddr
		SET	Action = @REC_INSERT
		WHERE 	Action IS NULL
		AND SessionKey = @iSessionKey
		OPTION (KEEP PLAN)

		--UPDATE Action flag if it is specified as Automatic
		--Set Action to 'Update' if record already exists in database
		--Otherwise, set the Action to 'Insert'.
		UPDATE 	#StgCustAddr
		SET	Action = CASE WHEN ca.CustAddrID IS NOT NULL THEN @REC_UPDATE
					ELSE @REC_INSERT END
		FROM	#StgCustAddr
		LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
		ON	#StgCustAddr.CustID = c.CustID
		AND	c.CompanyID = @iCompanyID
		LEFT OUTER JOIN tarCustAddr ca WITH (NOLOCK)
		ON	c.CustKey = ca.CustKey
		AND	#StgCustAddr.CustAddrID = ca.CustAddrID		
		WHERE 	#StgCustAddr.Action = @REC_AUTO
		OPTION (KEEP PLAN)

		--make sure there are rows to process
		IF EXISTS (SELECT * FROM #StgCustAddr WHERE SessionKey = @iSessionKey)
		BEGIN
			-- Update STaxSched from #StgCustomer/tarCustClass tarCustAddr
			UPDATE #StgCustAddr SET STaxSchdID =
			COALESCE(NULLIF(RTRIM(LTRIM(SC.STaxSchdID)),''), ST.STaxSchdID)
			FROM #StgCustAddr SC
			INNER JOIN tarCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			AND C.CompanyID = @iCompanyID
			INNER JOIN tarCustAddr TCA WITH (NOLOCK)
			ON C.PrimaryAddrKey = TCA.AddrKey
			INNER JOIN tciSTaxSchedule ST
			ON TCA.STaxSchdKey = ST.STaxSchdKey
			WHERE C.CompanyID = @iCompanyID
			AND SC.SessionKey = @iSessionKey
			OPTION(KEEP PLAN)

			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			UPDATE #StgCustAddr SET
			AllowInvtSubst = COALESCE(NULLIF(LTRIM(RTRIM(SC.AllowInvtSubst)),''),CONVERT(VARCHAR(1),TCA.AllowInvtSubst)),
			CurrID = COALESCE(SC.CurrID,TCA.CurrID),
			LanguageID = COALESCE(SC.LanguageID, TCA.LanguageID),
			RequireSOAck = COALESCE(NULLIF(LTRIM(RTRIM(SC.RequireSOAck)),''),CONVERT(VARCHAR(1),TCA.RequireSOAck)),
			ShipComplete = COALESCE(NULLIF(LTRIM(RTRIM(SC.ShipComplete)),''), CONVERT(VARCHAR(1),TCA.ShipComplete))
			FROM #StgCustAddr SC WITH (NOLOCK)
			INNER JOIN tarCustomer C WITH (NOLOCK)
			ON SC.CustID = C.CustID
			INNER JOIN tarCustAddr TCA WITH (NOLOCK)
			ON C.PrimaryAddrKey = TCA.AddrKey
			WHERE C.CompanyID = @iCompanyID
			AND  SC.SessionKey = @iSessionKey
			OPTION (KEEP PLAN)

			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			-- converts yes/no fields, defaults and
			EXEC spDMTmpTblDfltUpd '#StgCustAddr', 'tciAddress', @RetVal OUTPUT, @ACTION_COL_USED
			EXEC spARCustAddrUpd @iSessionKey, @RetVal OUTPUT
	
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RetVal, @oContinue = 0
				GOTO CloseAndDeallocateCursors
				RETURN
			END
	
			-- Clear the validation work table
			TRUNCATE TABLE #tarCustAddrValWrk

			-- Populate the CustAddr Temp table for validation
			INSERT #tarCustAddrValWrk (RowKey,
			AddrLine1,AddrLine2,AddrLine3,AddrLine4,AddrLine5,AddrName
			,AllowInvtSubst,City,CommPlanID,CountryID,CloseSOLineOnFirstShip, CloseSOOnFirstShip, CurrExchSchdID
			,CurrID,CustAddrID,CustID,CustPriceGroupID,EMailAddr,Fax,FaxExt
			,FOBID,InvcFormID,InvcMsg,Name,PackListFormID
			,Phone,PhoneExt,PmtTermsID,PostalCode
			,PriceBase,PrintOrderAck,ProcessStatus,RequireSOAck
			,SalesTerritoryID,ShipComplete,ShipDays
			,ShipLabelFormID,ShipMethID
			,SOAckFormID,SperID,StateID
			,STaxSchdID,Title,WhseID,BOLReqd,Residential
			,InvoiceReqd,PackListReqd,PackListContentsReqd,ShipLabelsReqd
			,CarrierAcctNo,CarrierBillMeth,FreightMethod,PriceAdj,Action,CRMAddrID,CRMContactID)
			SELECT RowKey,
			NULLIF(AddrLine1,''),NULLIF(AddrLine2,''),NULLIF(AddrLine3,''),NULLIF(AddrLine4,''),NULLIF(AddrLine5,''),NULLIF(AddrName,'')
			,NULLIF(AllowInvtSubst,''),NULLIF(City,''),NULLIF(CommPlanID,''),NULLIF(CountryID,'')
			,NULLIF(CloseSOLineOnFirstShip,''),NULLIF(CloseSOOnFirstShip,'')
			,NULLIF(CurrExchSchdID,'')
			,NULLIF(CurrID,''),NULLIF(CustAddrID,''),NULLIF(CustID,''),NULLIF(CustPriceGroupID,''),NULLIF(EMailAddr,''),NULLIF(Fax,''),NULLIF(FaxExt,'')
			,NULLIF(FOBID,''),NULLIF(InvcFormID,''),NULLIF(InvcMsg,''),Name,NULLIF(PackListFormID,'')
			,NULLIF(Phone,''),NULLIF(PhoneExt,''),NULLIF(PmtTermsID,''),NULLIF(PostalCode,'')
			,NULLIF(PriceBase,''),NULLIF(PrintOrderAck,''),COALESCE(ProcessStatus,0),NULLIF(RequireSOAck,'')
			,NULLIF(SalesTerritoryID,''),NULLIF(ShipComplete,''),ShipDays
			,NULLIF(ShipLabelFormID,''),NULLIF(ShipMethID,'')
			,NULLIF(SOAckFormID,''),NULLIF(SperID,''),NULLIF(StateID,'')
			,NULLIF(STaxSchdID,''),NULLIF(Title,''),NULLIF(WhseID,''),NULLIF(BOLReqd,''),NULLIF(Residential,'')
			,NULLIF(InvoiceReqd,''),NULLIF(PackListReqd,''),NULLIF(PackListContentsReqd,'')
			,NULLIF(ShipLabelsReqd,''),NULLIF(CarrierAcctNo,''),NULLIF(CarrierBillMeth,''),NULLIF(FreightMethod,''),coalesce(PriceAdj,0),Action
			,NULLIF(CRMAddrID,''),NULLIF(CRMContactID,'') 			
			FROM #StgCustAddr WITH (NOLOCK)
			WHERE Action = @REC_INSERT
			OPTION (KEEP PLAN) 	

			--Build insert statement for the updated record
			TRUNCATE TABLE #CustAddrUpdColumnList

			INSERT INTO #CustAddrUpdColumnList
						(SessionKey, ColumnName)
			SELECT		@iSessionKey, tc.InternalColumnName
			FROM	tdiDataTrnsfrColumnMap tc WITH (NOLOCK)
				JOIN	(SELECT DISTINCT DataTrnsfrjobDefStepKey
								FROM tdiDataTrnsfrJobDefStep tjs WITH (NOLOCK)
									JOIN 	tdiDataTrnsfrJobExecute tj WITH (NOLOCK)	ON	tjs.DataTrnsfrJobDefKey = tj.DataTrnsfrJobDefKey
								WHERE tj.SessionKey = @iSessionkey AND tjs.InternalTableName = 'StgCustAddr') derv
							ON	tc.DataTrnsfrjobDefStepKey = derv.DataTrnsfrjobDefStepKey
			WHERE	tc.AllowUpdate = 1
			AND		tc.InternalColumnName IN (
				'AddrLine1','AddrLine2','AddrLine3','AddrLine4','AddrLine5','AddrName','AllowInvtSubst','City','CloseSOLineOnFirstShip',
				'CloseSOOnFirstShip','CommPlanID','CountryID','CurrExchSchdID','CurrID','CustAddrID','CustID','CustPriceGroupID',
				'EMailAddr','Fax','FaxExt','FOBID','InvcFormID','InvcMsg','LanguageID','Name','PackListFormID','Phone',
				'PhoneExt','PmtTermsID','PostalCode','PriceBase','PrintOrderAck','RequireSOAck','SalesTerritoryID','ShipComplete',
				'ShipDays','ShipLabelFormID','ShipMethID','SOAckFormID','SperID','StateID','STaxSchdID','Title','WhseID','ProcessStatus',
				'CommPlanKey','CRMAddrID','CRMContactID','PriceAdj','BOLReqd','InvoiceReqd','PackListReqd','PackListContentsReqd',
				'Residential','ShipLabelsReqd','CarrierAcctNo','CarrierBillMeth','FreightMethod')

			SELECT @UpdColList = ''

			SELECT	@UpdColList = @UpdColList + List.ColumnName,
					@UpdColList = @UpdColList + ', '
			FROM 	#CustAddrUpdColumnList List
			
			-- If there are updatable column specified
			IF LEN(RTRIM(LTRIM(@UpdColList))) > 0
			BEGIN
				SELECT @UpdColList = SUBSTRING(@UpdColList, 1, LEN(@UpdColList) - 1)
				SELECT @UpdColList = 'RowKey, CustID, CustAddrID, ProcessStatus, Action, ' + @UpdColList

				SELECT @SQLStmt = 'INSERT INTO #tarCustAddrValWrk (' + RTRIM(LTRIM(@UpdColList)) + ') SELECT ' + RTRIM(LTRIM(@UpdColList)) + ' FROM #StgCustAddr WHERE SessionKey = ' + CONVERT(VARCHAR(10), @iSessionKey) + ' AND ProcessStatus = ' + CONVERT(VARCHAR(1), @NOT_PROCESSED) + ' AND Action = 2 OPTION(KEEP PLAN)'

				EXECUTE (@SQLStmt)	

			END
			ELSE
			BEGIN
			-- If there is no updatable column specified but there are records with the 'Update' flag, mark the records
			-- to "Fail"
				SELECT	@UpdRecCount = 0

				-- Check for count of unverified records with the 'Update' flag
				SELECT  @UpdRecCount = COUNT(*) FROM #StgCustAddr
				WHERE	SessionKey = @iSessionKey
				AND		ProcessStatus = @NOT_PROCESSED
				AND		Action = @REC_UPDATE
			
				-- Record found, run validation
				IF @UpdRecCount > 0
				BEGIN
					SELECT @EntityColName = 'COALESCE(CustID, '''')' + '+''|''+' + 'COALESCE(CustAddrID, '''')'

					EXEC spDMValidateUpdColforUpdRec @iSessionKey, 0/*@iUseStageTable*/,'StgCustAddr', @EntityColName,
							@LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @ErrorCount OUTPUT, @RetVal OUTPUT

					-- Update record count
					SELECT	@oFailedRecs = @oFailedRecs + @ErrorCount
					SELECT	@oRecsProcessed = @oRecsProcessed + @ErrorCount

					-- Cleanup #tdmMigrationLogEntryWrk
					TRUNCATE TABLE #tdmMigrationLogEntryWrk

					IF @iUseStageTable = 1
					BEGIN
						-- Sync Validation table to StgCustAddr
						UPDATE	StgCustAddr
						SET		ProcessStatus = addr.ProcessStatus
						FROM	StgCustAddr
						JOIN	#StgCustAddr addr
						ON		StgCustAddr.RowKey = addr.RowKey
						WHERE	addr.ProcessStatus = @GENRL_PROC_ERR
						OPTION(KEEP PLAN)	

						-- Sync Validation table to StgCustAddr
						DELETE	#StgCustAddr
						WHERE	ProcessStatus = @GENRL_PROC_ERR
						OPTION(KEEP PLAN)
					END

					IF @RetVal <> @RET_SUCCESS
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						RETURN
					END
				END
			END

			-- validate and get keys
			EXEC spARCustAddrVal @iPrintWarnings, @LanguageID,
			@iSessionKey, @iUseStageTable, @BlankInvalidReference,@iCompanyID,
			@iRptOption, @RetVal OUTPUT

			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				GOTO CloseAndDeallocateCursors
				RETURN
			END	

			-- Update the temp table with the CustKey
			UPDATE #tarCustAddrValWrk SET CustKey = C.CustKey
			FROM #tarCustAddrValWrk CA WITH (NOLOCK)
			INNER JOIN tarCustomer C WITH (NOLOCK)
			ON CA.CustID = C.CustID
			WHERE C.CompanyID = @iCompanyID
			--AND CA.ProcessStatus = @NOT_PROCESSED
			OPTION(KEEP PLAN)
	
			UPDATE #tarCustAddrValWrk SET ProcessStatus = @GENRL_PROC_ERR
			WHERE CustKey IS NULL AND ProcessStatus = @NOT_PROCESSED
			OPTION(KEEP PLAN)
	
			-- update the #StgCustAddr with validation failures
			UPDATE #StgCustAddr SET ProcessStatus = TCA.ProcessStatus
			FROM #StgCustAddr CA WITH (NOLOCK)
			INNER JOIN #tarCustAddrValWrk TCA WITH (NOLOCK)
			ON CA.RowKey = COALESCE(TCA.RowKey, 0)
			AND CA.CustID = TCA.CustID
			WHERE  TCA.ProcessStatus = @GENRL_PROC_ERR
			AND CA.SessionKey = @iSessionKey
			OPTION(KEEP PLAN)

			-- report for blank CustKey

			IF EXISTS(SELECT * FROM #tarCustAddrValWrk WHERE CustKey IS NULL)
			BEGIN
				
				IF @iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL
				BEGIN
				-- where Customer ID is invalid

					EXEC spciGetMenuTitle @TASKID_CUSTOMER, @TaskString OUTPUT

					EXEC ciBuildString @RPT_STRINGNO_SEEFORVALID, @LanguageID, @Message OUTPUT, @RetVal OUTPUT, 'CustID', @TaskString
					
						
					INSERT #tdmMigrationLogEntryWrk
					(ColumnID,ColumnValue,Comment,Duplicate,
					EntityID,SessionKey,Status)
					SELECT 'CustID', COALESCE(CustID, ''), @Message, 0,
					LTRIM(RTRIM(COALESCE(CustID,''))) + '|' + LTRIM(RTRIM(COALESCE(CustAddrID,''))),
					@iSessionKey,@MIG_STAT_FAILURE
					FROM #tarCustAddrValWrk  WITH (NOLOCK) WHERE
					CustKey IS NULL AND NULLIF(LTRIM(RTRIM(CustID)),'') IS NOT NULL
					OPTION(KEEP PLAN)

					-- where Customer ID is blank
					EXEC ciBuildString @RPT_STRINGNO_CANTBEBLANK,@LanguageID,@Message OUTPUT,@RetVal OUTPUT,'CustID'

					INSERT #tdmMigrationLogEntryWrk
					(ColumnID,ColumnValue,Comment,Duplicate,
					EntityID,SessionKey,Status)
					SELECT 'CustID', COALESCE(CustID, ''), @Message, 0,
					COALESCE(LTRIM(RTRIM(CustID)),'') + '|' + LTRIM(RTRIM(COALESCE(CustAddrID,''))),
					@iSessionKey,@MIG_STAT_FAILURE
					FROM #tarCustAddrValWrk  WITH (NOLOCK) WHERE
					CustKey IS NULL AND NULLIF(LTRIM(RTRIM(CustID)),'') IS NULL
					OPTION(KEEP PLAN)
					
					EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

					TRUNCATE TABLE #tdmMigrationLogEntryWrk


				END

			END

			-- Now declare the CustAddr cursor (if necessary).
			IF CURSOR_STATUS ('global', 'curCustAddr') = @STATUS_CURSOR_DOES_NOT_EXIST -- Cursor Not Allocated
			BEGIN
		
				DECLARE curCustAddr INSENSITIVE CURSOR FOR
				SELECT	RowKey
				FROM #StgCustAddr
				WHERE SessionKey = @iSessionKey
				ORDER BY Action                    -- This is Insert/Update/Delete flag (Ins/Upd first, Del 2nd)
			END
			-- open the CustAddr Cursor
			OPEN curCustAddr
		
			FETCH curCustAddr INTO @RowKey

			WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL)
			BEGIN -- begin of validation/insertion loop
			
				-- Reset  variables 	
				SELECT	@CurrExchSchdKey	= NULL	,@CustKey		= NULL	
					,@AddrKey		= NULL	,@CntctKey		= NULL
					,@STaxSchdKey		= NULL	,@AddrRowCount		= NULL
					,@RecordHadErrors 	= 0	,@Validated 		= 1
					,@Duplicate 		= 0	,@CustAddrID		= NULL
					,@CustID 		= NULL  ,@Name   		= NULL
,@StringNo              = NULL
	
				SELECT @CustAddrID = CustAddrID, @Name = Name, @CustID = CustID, @CustKey = CustKey
				,@EntityID = LTRIM(RTRIM(COALESCE(CustID, ''))) + '|' + LTRIM(RTRIM(COALESCE(CustAddrID, '')))
				,@CustAddrRowKey = RowKey
				,@Action = Action
				FROM #tarCustAddrValWrk WITH (NOLOCK)
				WHERE RowKey = @RowKey
				OPTION(KEEP PLAN)

				IF EXISTS(SELECT * FROM #tarCustAddrValWrk WHERE ProcessStatus = @GENRL_PROC_ERR
					AND RowKey = @RowKey )

				SELECT @ProcessStatus = @MIG_STAT_FAILURE, @Validated = 0
	
				--check that record exists for update/delete
				--check that record is not a duplicate for insert
		        IF @Action IN (@REC_UPDATE, @REC_DELETE)
				BEGIN
					IF @CustAddrID NOT IN (SELECT CustAddrID FROM tarCustAddr WITH (NOLOCK)
		     				WHERE CustKey = @CustKey)
					BEGIN
						IF @Action = @REC_UPDATE
						    SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,@Duplicate = 0,
								               @StringNo = @RPT_STRINGNO_UPDATE_FAILED
						ELSE
						    SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,@Duplicate = 1,
							               @StringNo = @RPT_STRINGNO_DELETE_FAILED
					END
		                END
		                ELSE
		                BEGIN        --@REC_INSERT
					IF @CustAddrID IN (SELECT CustAddrID FROM tarCustAddr WITH (NOLOCK)
					                    WHERE CustKey = @CustKey)
					BEGIN
						SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,@Duplicate = 1,
						@StringNo = @RPT_STRINGNO_DUPLICATE
					END 					
		                END

				-- IF @StringNo shows error, write error log and mark rows with error
				IF @StringNo IN (@RPT_STRINGNO_DUPLICATE, @RPT_STRINGNO_UPDATE_FAILED, @RPT_STRINGNO_DELETE_FAILED)
				BEGIN
					IF @iRptOption = @RPT_PRINT_ALL OR @iRptOption = @RPT_PRINT_UNSUCCESSFUL
					BEGIN
						EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
					
						EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
						@EntityID, 'CustAddrID', @CustAddrID, @ProcessStatus, @Duplicate,
						@Message, @RetVal OUTPUT
					END

					UPDATE #tarCustAddrValWrk SET ProcessStatus = @GENRL_PROC_ERR
					WHERE RowKey = @RowKey
					OPTION(KEEP PLAN)

					UPDATE #StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
					WHERE RowKey = @RowKey
					OPTION(KEEP PLAN)

					IF @iUseStageTable = 1
					BEGIN
						UPDATE StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
						FROM StgCustAddr
						WHERE RowKey = @RowKey

					END
				END

				IF @Validated = 1
				BEGIN

					IF @Action = @REC_INSERT
					BEGIN
	     				--Get the next AddrKey
	     				EXEC spGetNextSurrogateKey 'tciAddress', @AddrKey OUTPUT
	
	 					IF @AddrKey = 0 OR @AddrKey IS NULL			
	 					BEGIN
	 						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
	 						GOTO CloseAndDeallocateCursors
	 						RETURN
	 					END
					END

					-- Primary Contact for this address
					IF @Name IS NULL
					BEGIN
						SELECT @NeedsCntctIns = 0
						
					END
					ELSE
					BEGIN
						SELECT @NeedsCntctIns = 1
						-- get the cntctkey if it is in the database already
		
						SELECT @CntctKey = CntctKey, @NeedsCntctIns = 0
						FROM tciContact WHERE CntctOwnerKey = @CustKey
						AND EntityType = @ENTITYTYPE_CUST
						AND Name = @Name 		

						-- if insert is needed, get a new CntctKey
						IF @NeedsCntctIns = 1
						BEGIN
							
							EXEC spGetNextSurrogateKey 'tciContact', @CntctKey OUTPUT
	
							IF @CntctKey = 0 OR @CntctKey IS NULL			
							BEGIN
								SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
								GOTO CloseAndDeallocateCursors
								RETURN
							END	
					
	    						UPDATE 	#tarCustAddrValWrk
							SET 	DfltCntctKey = @CntctKey
	    						WHERE 	RowKey = @CustAddrRowKey
	    						OPTION(KEEP PLAN)

						END
					END 	

					BEGIN TRAN 	
	
					-- insert the contact when needed
					IF @RecordHadErrors = 0 AND @NeedsCntctIns = 1
					BEGIN
	
						INSERT tciContact(
						CntctKey, CntctOwnerKey, CreateType,
						EMailAddr, EntityType, Fax, FaxExt,
						ImportLogKey, Name, Phone, PhoneExt, Title,
						CreateDate, CreateUserID, UpdateDate, UpdateUserID, CRMContactID)
						SELECT @CntctKey, @CustKey, @CREATE_TYPE_MIGRATE,
						EMailAddr, @ENTITYTYPE_CUST, Fax, FaxExt,
						NULL, Name, Phone, PhoneExt, Title,
						@CreateDate, @UserID, @CreateDate, @UserID, CRMContactID
						FROM #tarCustAddrValWrk  WITH (NOLOCK)
						WHERE RowKey = @RowKey
						--AND Action = @REC_INSERT
						AND DfltCntctKey IS NOT NULL
						AND Name IS NOT NULL
						AND ProcessStatus = @NOT_PROCESSED
						AND DfltCntctKey NOT IN (SELECT CntctKey FROM tciContact)
						OPTION(KEEP PLAN)
						
		
						IF @@ERROR <> 0
							SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
					
					END

					IF @RecordHadErrors = 0
					BEGIN
						IF @Action = @REC_DELETE
						BEGIN
							-- delete the address(es) from tciAddress from the temptable
							DELETE tciAddress
							FROM tciAddress WITH (NOLOCK)
									  JOIN #tarCustAddrValWrk WITH (NOLOCK)
							ON (tciAddress.AddrKey = #tarCustAddrValWrk.AddrKey)
							WHERE RowKey = @RowKey
							OPTION(KEEP PLAN)
						
							IF @@ERROR <> 0
								SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
						END    -- IF @Action = @REC_DELETE
						ELSE   -- Not a deletion
						BEGIN  -- Update Action
							IF @Action = @REC_UPDATE
							BEGIN
							-- update the address(es) into tciAddress from the temptable
								UPDATE tciAddress
								   SET 	AddrLine1 = COALESCE(#tarCustAddrValWrk.AddrLine1,''),
								       	AddrLine2 = COALESCE(#tarCustAddrValWrk.AddrLine2,''),
								       	AddrLine3 = COALESCE(#tarCustAddrValWrk.AddrLine3,''),
								       	AddrLine4 = COALESCE(#tarCustAddrValWrk.AddrLine4,''),
									AddrLine5 = COALESCE(#tarCustAddrValWrk.AddrLine5,''),
								       	AddrName = #tarCustAddrValWrk.AddrName,
								       	City = #tarCustAddrValWrk.City,
								       	CountryID = #tarCustAddrValWrk.CountryID,
								       	CRMAddrID = #tarCustAddrValWrk.CRMAddrID,
								       	PostalCode = #tarCustAddrValWrk.PostalCode,
									StateID = #tarCustAddrValWrk.StateID,
									Residential = #tarCustAddrValWrk.Residential,
									UpdateCounter = UpdateCounter + 1		
								FROM tciAddress WITH (NOLOCK)
								JOIN #tarCustAddrValWrk WITH (NOLOCK)
								ON (tciAddress.AddrKey = #tarCustAddrValWrk.AddrKey)
								WHERE RowKey = @RowKey
								OPTION(KEEP PLAN)

								IF @@ERROR <> 0
									SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
							END    --IF @Action = @REC_UPDATE
							ELSE   --Not an Update
							BEGIN  --Attempt to Insert the record
								-- insert the address(es) into tciAddress from the temptable
								INSERT tciAddress (AddrKey, AddrLine1, AddrLine2, AddrLine3, AddrLine4,
								AddrLine5, AddrName, City, CountryID, CRMAddrID, PostalCode, StateID,Residential)
								SELECT @AddrKey, COALESCE(AddrLine1,''),COALESCE(AddrLine2,''), COALESCE(AddrLine3,''), COALESCE(AddrLine4,''),
								COALESCE(AddrLine5,''), AddrName, City, CountryID, CRMAddrID, PostalCode, StateID,Residential
								FROM #tarCustAddrValWrk  WITH (NOLOCK) WHERE RowKey = @RowKey
								OPTION(KEEP PLAN)
								
								IF @@ERROR <> 0
									SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
							END    --Record Action was Insertion
						END    --Record Action was not Delete
					END    --IF @RecordHadErrors = 0
	
					IF @RecordHadErrors = 0
					BEGIN					
	                    			IF @Action = @REC_DELETE
			                        BEGIN
							-- DELETE the tarCustAddr Record
	        					DELETE tarCustAddr
	        					FROM #tarCustAddrValWrk as avw WITH (NOLOCK)
	        					WHERE tarCustAddr.AddrKey = avw.AddrKey
							AND avw.RowKey = @RowKey
			        			OPTION(KEEP PLAN)
			
	     						IF @@ERROR <> 0
	     							SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
			                        END    -- IF @Action = @REC_DELETE
			                        ELSE   -- Not a deletion
			                        BEGIN  -- Update Action
							IF @Action = @REC_UPDATE
							BEGIN
			                                -- update the tarCustAddr Record
			            				UPDATE tarCustAddr
								SET AllowInvtSubst = avw.AllowInvtSubst,          	CommPlanKey = avw.CommPlanKey,
								CloseSOLineOnFirstShip = avw.CloseSOLineOnFirstShip,  	CloseSOOnFirstShip = avw.CloseSOOnFirstShip,
								CurrExchSchdKey = avw.CurrExchSchdKey,                	CurrID = avw.CurrID,
								CustAddrID = avw.CustAddrID,                          	CustPriceGroupKey = avw.CustPriceGroupKey,
								DfltCntctKey = avw.DfltCntctKey,                      	FOBKey = avw.FOBKey,
								InvcFormKey = avw.InvcFormKey,                        	InvcMsg = avw.InvcMsg,
								PackListFormKey = avw.PackListFormKey,                	PmtTermsKey = avw.PmtTermsKey,
								PriceBase = avw.PriceBase,                            	PrintOrderAck = avw.PrintOrderAck,
								RequireSOAck = avw.RequireSOAck,                      	SalesTerritoryKey = avw.SalesTerritoryKey,
								ShipComplete = avw.ShipComplete,                      	ShipDays = avw.ShipDays,
								ShipLabelFormKey = avw.ShipLabelFormKey,              	ShipMethKey = avw.ShipMethKey,
								SOAckFormKey = avw.SOAckFormKey,
								SperKey = avw.SperKey,                                	STaxSchdKey = avw.STaxSchdKey,
								WhseKey = avw.WhseKey,                                	BOLReqd = avw.BOLReqd,
								InvoiceReqd = avw.InvoiceReqd,                        	PackListReqd = avw.PackListReqd,
								PackListContentsReqd = avw.PackListContentsReqd,      	ShipLabelsReqd = avw.ShipLabelsReqd,
								CarrierAcctNo = avw.CarrierAcctNo,                    	CarrierBillMeth = avw.CarrierBillMeth,
								FreightMethod = avw.FreightMethod,                    	UpdateDate = @CreateDate ,
								UpdateUserID = @UserID,                                 PriceAdj = Case
When avw.PriceAdj=0 then 0
Else avw.PriceAdj /100
End
								FROM tarCustAddr WITH (NOLOCK)
								JOIN #tarCustAddrValWrk as avw WITH (NOLOCK)
								ON (tarCustAddr.AddrKey = avw.AddrKey)
								WHERE avw.RowKey = @RowKey
								OPTION(KEEP PLAN)
			
	         						IF @@ERROR <> 0
	         							SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
							END    --IF @Action = @REC_UPDATE
							ELSE   --Not an Update
							BEGIN  --Attempt to Insert the record
								INSERT tarCustAddr(
								AddrKey, AllowInvtSubst, BackOrdPrice, CommPlanKey, CreateType,
								CloseSOLineOnFirstShip, CloseSOOnFirstShip,
								CurrExchSchdKey, CurrID, CustAddrID, CustKey, CustPriceGroupKey,
								DfltCntctKey, FOBKey, ImportLogKey, InvcFormKey, InvcMsg, LanguageID,
								PackListFormKey, PmtTermsKey,
PriceAdj,
PriceBase, PrintOrderAck, RequireSOAck,
								SalesTerritoryKey, ShipComplete, ShipDays, ShipLabelFormKey, ShipMethKey,
								SOAckFormKey, SperKey, STaxSchdKey, WhseKey,BOLReqd,InvoiceReqd,PackListReqd,
								PackListContentsReqd,ShipLabelsReqd,CarrierAcctNo ,CarrierBillMeth,FreightMethod,
								CreateDate, CreateUserID, UpdateDate, UpdateUserID)
								SELECT
								@AddrKey, AllowInvtSubst, 0, CommPlanKey, @CREATE_TYPE_MIGRATE,
								CloseSOLineOnFirstShip, CloseSOOnFirstShip,
								CurrExchSchdKey, CurrID, CustAddrID, @CustKey, CustPriceGroupKey,
								@CntctKey, FOBKey, ImportLogKey, InvcFormKey, InvcMsg, @LanguageID,
								PackListFormKey, PmtTermsKey,
Case PriceAdj
When 0 then 0
Else PriceAdj / 100
End ,
PriceBase, PrintOrderAck, RequireSOAck,
								SalesTerritoryKey, ShipComplete, ShipDays, ShipLabelFormKey, ShipMethKey,
								SOAckFormKey, SperKey, STaxSchdKey, WhseKey,BOLReqd,InvoiceReqd,PackListReqd,
								PackListContentsReqd,ShipLabelsReqd,CarrierAcctNo,CarrierBillMeth,FreightMethod,
								@CreateDate, @UserID, @CreateDate, @UserID
								FROM #tarCustAddrValWrk  WITH (NOLOCK) WHERE RowKey = @RowKey
								OPTION(KEEP PLAN)
								
								IF @@ERROR <> 0
									SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
							END    --Record Action was Insertion
						END    --Record Action was not Delete
					END    --IF @RecordHadErrors = 0
	
					IF @RecordHadErrors = 1-- an error occured
					BEGIN
						ROLLBACK TRANSACTION
						SELECT @ProcessStatus = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
					END
					ELSE			-- an error did not occur
					BEGIN
						COMMIT TRANSACTION -- We commit the transaction here
						SELECT @ProcessStatus = @MIG_STAT_SUCCESSFUL,
							   @StringNo = NULL
					END

	 				IF 	(@iRptOption = 	@RPT_PRINT_ALL) OR
	 					(@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL AND @ProcessStatus = @MIG_STAT_FAILURE) OR
	 					(@iRptOption =  @RPT_PRINT_SUCCESSFUL AND @ProcessStatus = @MIG_STAT_SUCCESSFUL)
	 	
	 				BEGIN
	 					-- Default @Message to NULL string
						IF @ProcessStatus = @MIG_STAT_SUCCESSFUL
							SELECT @Message = CASE WHEN @Action = 1 THEN 'Record has been inserted.'
										WHEN @Action = 2 THEN 'Record has been updated.'
										WHEN @Action = 3 THEN 'Record has been deleted.' END	
									
	 					EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
	 					@EntityID, '', '', @ProcessStatus, @Duplicate,
	 					@Message, @RetVal OUTPUT
	 		
	 				END -- Reporting was requested
				END-- IF @Validated = 1

				IF @ProcessStatus = @MIG_STAT_FAILURE
					SELECT @oFailedRecs = @oFailedRecs + 1
	
				SELECT @RowsProcThisCall = @RowsProcThisCall + 1	

	
	
					------------------------------------------------------------------------------------
					-- If using staging tables, then mark failed records with the failed
					-- record processing status and delete successful rows.
					------------------------------------------------------------------------------------
				IF @iUseStageTable = 1
				BEGIN -- using Staging tables
				
					IF @ProcessStatus = @MIG_STAT_FAILURE
					BEGIN
					
		
						UPDATE StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR-- did not process
						WHERE RowKey  = @RowKey
						OPTION(KEEP PLAN)
						
					
				   	END -- using staging table
					ELSE-- @MIG_STAT_SUCCESS
					
					BEGIN
						DELETE StgCustAddr
						WHERE RowKey  = @RowKey
						OPTION(KEEP PLAN)
		
					END -- Sucessful
	
				END-- IF using Staging tables
						
				-------------------------------------------------------------------
				-- Process the temp tables
				-------------------------------------------------------------------
	
				IF @ProcessStatus = @MIG_STAT_FAILURE
				BEGIN
	
					UPDATE #StgCustAddr SET ProcessStatus = @GENRL_PROC_ERR
					WHERE RowKey = @RowKey
					OPTION(KEEP PLAN)
	
				END
				ELSE
				BEGIN
	
					DELETE #StgCustAddr
					WHERE RowKey <> @RowKey
					OPTION(KEEP PLAN)

					DELETE #tarCustAddrValWrk
					WHERE RowKey = @RowKey
					OPTION(KEEP PLAN)
		
				END
		
				FETCH curCustAddr INTO @RowKey
	
			END --WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL)
	
			IF CURSOR_STATUS ('global', 'curCustAddr') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
			BEGIN
				CLOSE curCustAddr
			END -- End [We need to close the CustAddr cursor.]
	
			TRUNCATE TABLE #tarCustAddrValWrk

			
			SELECT @SourceRowCount = COUNT(*)
			FROM #StgCustAddr
			WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED

			
		END --- IF EXISTS (SELECT * FROM #StgCustAddr)
	
		SELECT @oRecsProcessed = @oRecsProcessed + @RowsProcThisCall
	
		SELECT @RowsProcThisCall = 0, @lRecFailed = 0


		-- if using Stage tables then See if there are any more rows to process -- deallocate cursor if no rows
		-- if not using Stage tables then all records are processed at one time -- deallocate cursor
		IF (@iUseStageTable = 1 AND (SELECT COUNT(*) FROM StgCustAddr WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED) = 0)
			OR @iUseStageTable = 0
	    BEGIN

	      	IF CURSOR_STATUS ('global', 'curCustAddr') = @STATUS_CURSOR_CLOSED -- Time to Deallocate the CustAddr Cursor
			BEGIN
				DEALLOCATE curCustAddr
			END
		END

		-- Determine if any remaining rows exist for insertion.
		--        If not, then consider this successful.
		IF @iUseStageTable = 1
		BEGIN
			IF ((SELECT COUNT(*) FROM StgCustAddr WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED) > 0)
			BEGIN

				SELECT @oContinue = 1
				RETURN					
	
			END
		END
		
		
	END -- Timing

	IF @iUseStageTable = 1
	BEGIN
		SELECT @SourceRowCount = COUNT(*) FROM StgContact WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
	END
	ELSE
	BEGIN
		SELECT @SourceRowCount = COUNT(*) FROM #StgContact WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
	END

	------------------------------------------------------
	-- Start Processing other Contacts
	------------------------------------------------------
	
	SELECT @RowsProcThisCall = 0, @lRecFailed = 0

	SELECT @StartDate = GetDate()
	-- added AND @SourceRowCount to continue until no more rows need processing
	--select @SourceRowCount = 1
	WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS AND @SourceRowCount > 0
	BEGIN

		--UPDATE Action flag if it is specified as Automatic
		--Set Action to 'Update' if record already exists in database
		--Otherwise, set the Action to 'Insert'.
		IF @iUseStageTable = 1
		BEGIN
			UPDATE 	StgContact
			SET	Action = @REC_INSERT
			WHERE 	Action IS NULL
			AND SessionKey = @iSessionKey
			OPTION (KEEP PLAN)

			UPDATE 	StgContact
			SET	Action = CASE WHEN ct.CntctOwnerKey IS NOT NULL THEN @REC_UPDATE
						ELSE @REC_INSERT END
			FROM	StgContact
			LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
			ON	StgContact.CntctOwnerID = c.CustID
			AND	c.CompanyID = @iCompanyID
			LEFT OUTER JOIN tciContact ct WITH (NOLOCK)
			ON	c.CustKey = ct.CntctOwnerKey
			AND	StgContact.Name = ct.Name
			WHERE StgContact.Action = @REC_AUTO
			OPTION(KEEP PLAN)
		END
		ELSE
		BEGIN
			UPDATE 	#StgContact
			SET	Action = @REC_INSERT
			WHERE 	Action IS NULL
			AND SessionKey = @iSessionKey
			OPTION (KEEP PLAN)

			UPDATE 	#StgContact
			SET	Action = CASE WHEN ct.CntctOwnerKey IS NOT NULL THEN @REC_UPDATE
						ELSE @REC_INSERT END
			FROM	#StgContact
			LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
			ON	#StgContact.CntctOwnerID = c.CustID
			AND	c.CompanyID = @iCompanyID
			LEFT OUTER JOIN tciContact ct WITH (NOLOCK)
			ON	c.CustKey = ct.CntctOwnerKey
			AND	#StgContact.Name = ct.Name
			WHERE #StgContact.Action = @REC_AUTO
			OPTION(KEEP PLAN)
		END

		IF @iUseStageTable = 1
		BEGIN
			--populate  #StgContact
			
			TRUNCATE TABLE #StgContact
	
			SET ROWCOUNT @MAX_ROWS_PER_RUN

			SET IDENTITY_INSERT #StgContact ON
	
			INSERT #StgContact
			(RowKey,CntctOwnerID,EMailAddr,Fax,FaxExt,Name,Phone,PhoneExt,Title
			,ProcessStatus,SessionKey,Action,CRMContactID)
			SELECT RowKey,CntctOwnerID,EMailAddr,Fax,FaxExt,Name,
			Phone,PhoneExt,Title,ProcessStatus,SessionKey,Action,CRMContactID
			FROM StgContact WITH (NOLOCK)
			WHERE ProcessStatus = @NOT_PROCESSED
			AND SessionKey = @iSessionKey
			AND	Action = @REC_INSERT
			OPTION(KEEP PLAN)

			SET IDENTITY_INSERT #StgContact OFF

			-- SET ROWCOUNT 0
		END

		--Build insert statement for the updated record
		TRUNCATE TABLE #CustContactUpdColumnList

		INSERT INTO #CustContactUpdColumnList
					(SessionKey, ColumnName)
		SELECT		@iSessionKey, tc.InternalColumnName
		FROM	tdiDataTrnsfrColumnMap tc WITH (NOLOCK)
			JOIN	(SELECT DISTINCT DataTrnsfrjobDefStepKey
							FROM tdiDataTrnsfrJobDefStep tjs WITH (NOLOCK)
								JOIN 	tdiDataTrnsfrJobExecute tj WITH (NOLOCK)	ON	tjs.DataTrnsfrJobDefKey = tj.DataTrnsfrJobDefKey
							WHERE tj.SessionKey = @iSessionkey AND tjs.InternalTableName = 'StgContact') derv
						ON	tc.DataTrnsfrjobDefStepKey = derv.DataTrnsfrjobDefStepKey
		WHERE	tc.AllowUpdate = 1

		SELECT	@UpdColList = ''

		SELECT	@UpdColList = @UpdColList + List.ColumnName,
				@UpdColList = @UpdColList + ', '
		FROM 	#CustContactUpdColumnList List WITH (NOLOCK)

		-- If there are updatable column specified
		IF LEN(RTRIM(LTRIM(@UpdColList))) > 0
		BEGIN
			SELECT @UpdColList = SUBSTRING(@UpdColList, 1, LEN(@UpdColList) - 1)
			SELECT @UpdColList = 'RowKey, SessionKey, CntctOwnerID, Name, ProcessStatus, Action, ' + @UpdColList
			SELECT @SQLStmt = 'SET IDENTITY_INSERT #StgContact ON '
			SELECT @SQLStmt = @SQLStmt + 'INSERT INTO #StgContact (' + RTRIM(LTRIM(@UpdColList)) + ') SELECT ' + RTRIM(LTRIM(@UpdColList)) + ' FROM StgContact WHERE SessionKey = ' + CONVERT(VARCHAR(10), @iSessionKey) + ' AND ProcessStatus = ' + CONVERT(VARCHAR(1), @NOT_PROCESSED) + ' AND Action = 2 OPTION(KEEP PLAN)'
			SELECT @SQLStmt = @SQLStmt + ' SET IDENTITY_INSERT #StgContact OFF'
			EXECUTE (@SQLStmt)	
		END
		
		IF LEN(RTRIM(LTRIM(@UpdColList))) = 0
		BEGIN
		-- If there is no updatable column specified but there are records with the 'Update' flag, mark the records
		-- to "Fail"
			SELECT	@UpdRecCount = 0

			-- Check for count of unverified records with the 'Update' flag
			SELECT  @UpdRecCount = COUNT(*) FROM #StgContact
			WHERE	SessionKey = @iSessionKey
			AND		ProcessStatus = @NOT_PROCESSED
			AND		Action = @REC_UPDATE
			OPTION(KEEP PLAN)
		
			-- Record found, run validation
			IF @UpdRecCount > 0
			BEGIN


				SELECT @EntityColName = 'COALESCE(CntctOwnerID, '''')' + '+''|''+' + 'COALESCE(Name, '''')'

				EXEC spDMValidateUpdColforUpdRec @iSessionKey, 0/*@iUseStageTable*/,'StgContact', @EntityColName,
						@LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @ErrorCount OUTPUT, @RetVal OUTPUT

				-- Update record count
				SELECT	@oFailedRecs = @oFailedRecs + @ErrorCount
				SELECT	@oRecsProcessed = @oRecsProcessed + @ErrorCount

				-- Cleanup #tdmMigrationLogEntryWrk
				TRUNCATE TABLE #tdmMigrationLogEntryWrk

				IF @iUseStageTable = 1
				BEGIN
					-- Sync Validation table to StgCustAddr
					UPDATE	StgContact
					SET		ProcessStatus = a.ProcessStatus
					FROM	StgContact
					JOIN	#StgContact a
					ON		StgContact.RowKey = a.RowKey
					WHERE	a.ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)	

					-- Sync Validation table to StgCustAddr
					DELETE	#StgContact
					WHERE	ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
				END

				IF @RetVal <> @RET_SUCCESS
				BEGIN
					SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
					RETURN
				END
			END
		END

		-- get the rowcount to determine if any rows to process
		IF EXISTS(SELECT * FROM #StgContact)
		BEGIN
			
			SELECT @RowsProcThisCall = 0	
	
			-- Update defaults
			EXEC spDMTmpTblDfltUpd '#StgContact', 'tciContact', @RetVal OUTPUT, @ACTION_COL_USED
		
			IF @RetVal <> @RET_SUCCESS AND @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR
				RETURN
			END
	
			-- Strip out common Phone Number formatting
			EXEC spDMStrMaskStrip  '#StgContact', 'tciAddress',  @RetVal  OUTPUT
	
			IF @RetVal <> @RET_SUCCESS AND @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				GOTO CloseAndDeallocateCursors
				RETURN
			END

			-- StgContact.Action
			EXEC spDMListReplacement '#StgContact', 'Action',
			'StgContact', 'Action', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT
	
			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END
	
			-- Now declare the Contact cursor (if necessary).
			IF CURSOR_STATUS ('global', 'curContact') = @STATUS_CURSOR_DOES_NOT_EXIST -- Cursor Not Allocated
			BEGIN
		
				DECLARE curContact INSENSITIVE CURSOR FOR
				SELECT	RowKey
				FROM #StgContact
				WHERE ProcessStatus = @NOT_PROCESSED
				ORDER BY Action                    -- This is Insert/Update/Delete flag (Ins/Upd first, Del 2nd)
	
			END
			-- open the Contact Cursor
			OPEN curContact
		
			FETCH curContact INTO @RowKey
			
			WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL)
			BEGIN -- begin of validation/insertion loop
	
	
				-- reset variables
				SELECT @CntctKey = NULL, @CustKey = NULL, @CustID = NULL,@EntityID = NULL,
				@Name = NULL, @Validated = 1, @RecordHadErrors = 0, @Duplicate = 0,
				@ProcessStatus = @MIG_STAT_SUCCESSFUL
	
				--Get the Name and CustID ...
--Null Validation Removed since handled below
				SELECT @Name = Name, @CustID = CntctOwnerID,
				@EntityID = COALESCE(RTRIM(LTRIM(CntctOwnerID)), '') + '|' + COALESCE(RTRIM(LTRIM(Name)), ''),
				@Action = Action
				FROM #StgContact WITH (NOLOCK) WHERE RowKey = @RowKey
				OPTION(KEEP PLAN) 	

				-- get/validate the owner
				SELECT @CustKey = CustKey FROM tarCustomer WITH (NOLOCK)
				WHERE CustID = @CustID AND CompanyID = @iCompanyID 	

	
				IF @CustKey IS NULL OR @CustKey = 0
				BEGIN
	
					SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE
					EXEC spciGetMenuTitle @TASKID_CUSTOMER, @TaskString OUTPUT

					EXEC ciBuildString @RPT_STRINGNO_SEEFORVALID, @LanguageID, @Message OUTPUT, @RetVal OUTPUT, 'CustID', @TaskString
					SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE
						

					SELECT @CustID =  COALESCE(@CustID,'')	
					EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
					@EntityID, 'CustID', @CustID, @ProcessStatus, 0,
					@Message, @RetVal OUTPUT
					
	
				END
	
				-- Validate name field
				IF @Name IS NULL
				BEGIN
					SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,@StringNo = @RPT_STRINGNO_X_IS_INVALID
					
					EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT, 'Name'
							
					EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
					@EntityID, 'Name', @CustID, @ProcessStatus, @Duplicate,
					@Message, @RetVal OUTPUT
					
				END
	
				-- look for duplicate in DB for inserts
		                -- check for existence for updates/deletions
		                IF @Action IN (@REC_UPDATE, @REC_DELETE)
		                BEGIN
		     				IF NOT EXISTS(SELECT * FROM tciContact WITH (NOLOCK)
		                                   WHERE CntctOwnerKey = @CustKey
		                 				     AND EntityType = @ENTITYTYPE_CUST
		                 				     AND Name = @Name)
		                    BEGIN
		                        IF @Action = @REC_UPDATE
		                        BEGIN
		                            SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,
		                            	   @Duplicate = 0, @StringNo = @RPT_STRINGNO_UPDATE_FAILED
		                        END
		                        ELSE
		                        BEGIN
		                            SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,
		                            	   @Duplicate = 0, @StringNo = @RPT_STRINGNO_DELETE_FAILED
		                        END
		                    END
		                END
		                ELSE    -- @REC_INSERT
		                BEGIN
				IF EXISTS(SELECT * FROM tciContact WITH (NOLOCK) WHERE
				CntctOwnerKey = @CustKey
				AND EntityType = @ENTITYTYPE_CUST
				AND Name = @Name)
	     				BEGIN
	     	                		SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE,
	    					        @Duplicate = 1,@StringNo = @RPT_STRINGNO_DUPLICATE
					END
		                END

				IF @StringNo IN (@RPT_STRINGNO_DUPLICATE, @RPT_STRINGNO_UPDATE_FAILED, @RPT_STRINGNO_DELETE_FAILED)
				BEGIN
					EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT 					
							
					EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
					@EntityID, 'Name', @Name, @ProcessStatus, @Duplicate,
					@Message, @RetVal OUTPUT
	
					SELECT @Duplicate = 0
	
				END 				

				IF @Validated = 1
				BEGIN 	
					-- get CntctKey
					IF @Action = @REC_INSERT
					BEGIN
						EXEC spGetNextSurrogateKey 'tciContact', @CntctKey OUTPUT
					END
					ELSE
					BEGIN
						SELECT @CntctKey = CntctKey
						  FROM tciContact WITH (NOLOCK)
						 WHERE CntctOwnerKey = @CustKey
								       AND EntityType = @ENTITYTYPE_CUST
								       AND Name = @Name
					END
					
					IF @CntctKey = 0 OR @CntctKey IS NULL			
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						GOTO CloseAndDeallocateCursors
						RETURN
					END 	

					BEGIN TRAN

					IF @@ERROR <> 0
						SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
					
					IF @Action = @REC_DELETE
					BEGIN
						-- delete the primary contact
						DELETE tciContact
						WHERE CntctKey = @CntctKey
						        OPTION(KEEP PLAN)
					
						IF @@ERROR <> 0
							SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
					END    -- IF @Action = @REC_DELETE
					ELSE   -- Not a deletion
					BEGIN  -- Update Action
						IF @Action = @REC_UPDATE
						BEGIN
						    -- update the primary contact
							UPDATE tciContact
							SET EMailAddr = CASE WHEN EXISTS(SELECT 1 FROM #CustContactUpdColumnList WHERE ColumnName ='EMailAddr')
												THEN stg.EMailAddr
											ELSE tciContact.EMailAddr END,
							Fax = CASE WHEN EXISTS(SELECT 1 FROM #CustContactUpdColumnList WHERE ColumnName ='Fax')
												THEN stg.Fax
											ELSE tciContact.Fax END,
							FaxExt = CASE WHEN EXISTS(SELECT 1 FROM #CustContactUpdColumnList WHERE ColumnName ='FaxExt')
												THEN stg.FaxExt
											ELSE tciContact.FaxExt END,
							Phone = CASE WHEN EXISTS(SELECT 1 FROM #CustContactUpdColumnList WHERE ColumnName ='Phone')
												THEN stg.Phone
											ELSE tciContact.Phone END,
							PhoneExt = CASE WHEN EXISTS(SELECT 1 FROM #CustContactUpdColumnList WHERE ColumnName ='PhoneExt')
												THEN stg.PhoneExt
											ELSE tciContact.PhoneExt END,
							Title = CASE WHEN EXISTS(SELECT 1 FROM #CustContactUpdColumnList WHERE ColumnName ='Title')
												THEN stg.Title
											ELSE tciContact.Title END,
							CRMContactID = CASE WHEN EXISTS(SELECT 1 FROM #CustContactUpdColumnList WHERE ColumnName ='CRMContactID')
												THEN stg.CRMContactID
											ELSE tciContact.CRMContactID END,
							UpdateDate = @CreateDate,
							UpdateUserID = @UserID,
							UpdateCounter = tciContact.UpdateCounter								
							FROM tciContact WITH (NOLOCK)
							JOIN tarCustomer as cust WITH (NOLOCK)
							ON (cust.CustKey = tciContact.CntctOwnerKey)
							JOIN #StgContact as stg WITH (NOLOCK)
							ON (stg.Name = tciContact.Name AND
							stg.CntctOwnerID = cust.CustID AND
							cust.CompanyID = @iCompanyID)
							WHERE RowKey = @RowKey
							AND tciContact.CntctKey = @CntctKey
							AND tciContact.EntityType = @ENTITYTYPE_CUST
							OPTION(KEEP PLAN)
						
							IF @@ERROR <> 0
								SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
						END    --IF @Action = @REC_UPDATE
						ELSE   --Not an Update
						BEGIN  --Attempt to Insert the record

							INSERT tciContact(
							CntctKey, CntctOwnerKey, CreateType,
							EMailAddr, EntityType, Fax, FaxExt,
							ImportLogKey, Name, Phone, PhoneExt, Title,
							CreateDate, CreateUserID, UpdateDate, UpdateUserID, CRMContactID )
							SELECT @CntctKey, @CustKey, @CREATE_TYPE_MIGRATE,
							EMailAddr, @ENTITYTYPE_CUST, Fax, FaxExt,			--Getting EMailAddr from #StgContact, not from @EMailAddr - Scopus 37417, CEJ, 6/13/2007
							NULL, @Name, Phone, PhoneExt, Title,
							@CreateDate, @UserID, @CreateDate, @UserID, CRMContactID
							FROM #StgContact WITH (NOLOCK)
							WHERE RowKey = @RowKey
							AND Action = @REC_INSERT
							OPTION(KEEP PLAN) 					
						
							IF @@ERROR <> 0
								SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL 	
						END    --Record Action was Insertion
					END    --Record Action was not Delete
					
					IF @RecordHadErrors = 1-- an error occured
					BEGIN
						ROLLBACK TRANSACTION
						SELECT @ProcessStatus = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
					END
					ELSE			-- an error did not occur
					BEGIN
						COMMIT TRANSACTION -- We commit the transaction here
						SELECT @ProcessStatus = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
					END 	
	
	 				IF 	(@iRptOption = 	@RPT_PRINT_ALL) OR
	 					(@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL AND @ProcessStatus = @MIG_STAT_FAILURE) OR
	 					(@iRptOption =  @RPT_PRINT_SUCCESSFUL AND @ProcessStatus = @MIG_STAT_SUCCESSFUL)
	 	
	 				BEGIN
						-- add log for successful records
						IF @ProcessStatus = @MIG_STAT_SUCCESSFUL
	 						SELECT @Message = CASE WHEN @Action = 1 THEN 'Record has been inserted.'
										WHEN @Action = 2 THEN 'Record has been updated.'
										WHEN @Action = 3 THEN 'Record has been deleted.' END	 	
	 									
	 					EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
	 					@EntityID, '', '', @ProcessStatus, @Duplicate,
	 					@Message, @RetVal OUTPUT
	 		
	 				END -- Reporting was requested

				END --  @Validated = 1

				IF @ProcessStatus = @MIG_STAT_FAILURE
					SELECT @lRecFailed = @lRecFailed + 1

				SELECT @RowsProcThisCall = @RowsProcThisCall + 1 	
	
				IF @iUseStageTable = 1
				BEGIN -- using Staging tables
				
					IF @ProcessStatus = @MIG_STAT_FAILURE
					BEGIN
					
						UPDATE StgContact SET ProcessStatus = @GENRL_PROC_ERR-- did not process
						FROM StgContact WITH (NOLOCK)
						WHERE RowKey = @RowKey
						OPTION(KEEP PLAN)
					
				   	END -- using staging table
					ELSE-- @MIG_STAT_SUCCESSFUL
					
					BEGIN
		
						DELETE StgContact
						WHERE RowKey = @RowKey
		
					END -- Sucessful
	
				END-- IF using Staging tables
						
				-------------------------------------------------------------------
				-- Process the temp tables
				-------------------------------------------------------------------
	
				IF @ProcessStatus = @MIG_STAT_FAILURE
				BEGIN
				
					UPDATE #StgContact SET ProcessStatus = @GENRL_PROC_ERR
					FROM #StgContact WITH (NOLOCK)
					WHERE RowKey = @RowKey
					OPTION(KEEP PLAN)
	
				END
				ELSE
				BEGIN
	
					DELETE #StgContact
					WHERE RowKey = @RowKey
					OPTION(KEEP PLAN)
		
				END
	
				FETCH curContact INTO @RowKey
	
	
			END -- WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL)
	
	
			IF CURSOR_STATUS ('global', 'curContact') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
			BEGIN
				CLOSE curContact
			END -- End [We need to close the Contact cursor.]
	
			SELECT @oRecsProcessed = @oRecsProcessed + @RowsProcThisCall, @oFailedRecs = @oFailedRecs + @lRecFailed
		
		END -- IF EXISTS(SELECT * FROM #StgContact)
	
		IF CURSOR_STATUS ('global', 'curContact') = @STATUS_CURSOR_CLOSED -- Time to Deallocate the Contact Cursor
		BEGIN
			DEALLOCATE curContact
			BREAK
		END --

		-- Determine if any remaining rows exist for insertion.
		--        If not, then consider this successful.
		SELECT @SourceRowCount = COUNT(*)
			FROM #StgContact WITH (NOLOCK)
			WHERE ProcessStatus = @NOT_PROCESSED	
		
	END -- if Timing

	-- Exit if there are no rows to migrate.  Consider this success and tell the caller
	-- not to continue calling this SP.
	IF @SourceRowCount = 0
		BEGIN
			SELECT @_oRetVal = @RET_SUCCESS, @oContinue = 0
		END

	IF @iUseStageTable = 1
	BEGIN
		IF (SELECT COUNT(*) FROM StgContact WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED) > 0
		BEGIN
			SELECT @oContinue = 1
			SELECT @_oRetVal = @RET_SUCCESS
			RETURN	
		END
	END


	------------------------------------------
	-- Start processing StgCustSTaxExmpt
	------------------------------------------
	IF @UseSTax = 1
	BEGIN
		SELECT @RowsProcThisCall = 0, @lRecFailed = 0

		SELECT @StartDate = GetDate()
		-- added AND @SourceRowCount to continue until no more rows need processing
		IF @iUseStageTable = 1
		BEGIN
			SELECT @SourceRowCount = COUNT(*) FROM StgCustSTaxExmpt WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
		END
		ELSE
		BEGIN
			SELECT @SourceRowCount = COUNT(*) FROM #StgCustSTaxExmpt WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
		END
		
		
		WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS AND @SourceRowCount > 0
		BEGIN

			IF @iUseStageTable = 1
			BEGIN
				UPDATE 	StgCustSTaxExmpt
				SET	Action = @REC_AUTO
				WHERE 	Action IS NULL
				AND SessionKey = @iSessionKey
				OPTION (KEEP PLAN)

				--UPDATE Action flag if it is specified as Automatic
				--Set Action to 'Update' if record already exists in database
				--Otherwise, set the Action to 'Insert'.
				UPDATE 	StgCustSTaxExmpt
				SET	Action = CASE WHEN (StgCustSTaxExmpt.STaxCodeID = sc.STaxCodeID)  THEN @REC_UPDATE
							ELSE @REC_INSERT END
				FROM	StgCustSTaxExmpt
				LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
				ON	StgCustSTaxExmpt.CustID = c.CustID
				LEFT OUTER JOIN tarCustAddr ca WITH (NOLOCK)
				ON	c.CustKey = ca.CustKey
				LEFT OUTER JOIN tarCustSTaxExmpt st WITH (NOLOCK)
				ON	ca.AddrKey = st.AddrKey
				LEFT OUTER JOIN tciSTaxCode sc WITH (NOLOCK)
				ON	st.STaxCodeKey = sc.STaxCodeKey
				WHERE 	StgCustSTaxExmpt.Action = @REC_AUTO
				OPTION(KEEP PLAN)
			END
			ELSE
			BEGIN
				UPDATE 	#StgCustSTaxExmpt
				SET	Action = @REC_AUTO
				WHERE 	Action IS NULL
				AND SessionKey = @iSessionKey
				OPTION (KEEP PLAN)

				--UPDATE Action flag if it is specified as Automatic
				--Set Action to 'Update' if record already exists in database
				--Otherwise, set the Action to 'Insert'.
				UPDATE 	#StgCustSTaxExmpt
				SET	Action = CASE WHEN (#StgCustSTaxExmpt.STaxCodeID = sc.STaxCodeID)  THEN @REC_UPDATE
							ELSE @REC_INSERT END
				FROM	#StgCustSTaxExmpt
				LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
				ON	#StgCustSTaxExmpt.CustID = c.CustID
				LEFT OUTER JOIN tarCustAddr ca WITH (NOLOCK)
				ON	c.CustKey = ca.CustKey
				LEFT OUTER JOIN tarCustSTaxExmpt st WITH (NOLOCK)
				ON	ca.AddrKey = st.AddrKey
				LEFT OUTER JOIN tciSTaxCode sc WITH (NOLOCK)
				ON	st.STaxCodeKey = sc.STaxCodeKey
				WHERE 	#StgCustSTaxExmpt.Action = @REC_AUTO
				OPTION(KEEP PLAN)
			END

			IF @iUseStageTable = 1
			BEGIN
			-- Populate #StgCustSTaxExmpt if using staging tables--
				TRUNCATE TABLE #StgCustSTaxExmpt
				
				SET IDENTITY_INSERT #StgCustSTaxExmpt ON
				
				INSERT #StgCustSTaxExmpt
				(CustID, ExmptNo, ProcessStatus, RowKey, SessionKey, STaxCodeID,Action)
				SELECT CustID, ExmptNo, ProcessStatus, RowKey, SessionKey, STaxCodeID ,Action
				FROM StgCustSTaxExmpt WITH (NOLOCK)
				WHERE SessionKey = @iSessionKey
				AND ProcessStatus = @NOT_PROCESSED
				AND	Action = @REC_INSERT
				OPTION(KEEP PLAN)

				SET IDENTITY_INSERT #StgCustSTaxExmpt OFF
				
				SET ROWCOUNT 0

			END

			--Build insert statement for the updated record
			TRUNCATE TABLE #CustSTaxUpdColumnList

			INSERT INTO #CustSTaxUpdColumnList
						(SessionKey, ColumnName)
			SELECT		@iSessionKey, tc.InternalColumnName
			FROM	tdiDataTrnsfrColumnMap tc WITH (NOLOCK)
				JOIN	(SELECT DISTINCT DataTrnsfrjobDefStepKey
								FROM tdiDataTrnsfrJobDefStep tjs WITH (NOLOCK)
									JOIN 	tdiDataTrnsfrJobExecute tj WITH (NOLOCK)	ON	tjs.DataTrnsfrJobDefKey = tj.DataTrnsfrJobDefKey
								WHERE tj.SessionKey = @iSessionkey AND tjs.InternalTableName = 'StgCustSTaxExmpt') derv
							ON	tc.DataTrnsfrjobDefStepKey = derv.DataTrnsfrjobDefStepKey
			WHERE	tc.AllowUpdate = 1

			SELECT	@UpdColList = ''

			SELECT	@UpdColList = @UpdColList + List.ColumnName,
					@UpdColList = @UpdColList + ', '
			FROM 	#CustSTaxUpdColumnList List
			
			-- If there are updatable column specified
			IF LEN(RTRIM(LTRIM(@UpdColList))) > 0
			BEGIN
				SELECT @UpdColList = SUBSTRING(@UpdColList, 1, LEN(@UpdColList) - 1)
				SELECT @UpdColList = 'RowKey, SessionKey, CustID, ProcessStatus, STaxCodeID, Action, ' + @UpdColList
				SELECT @SQLStmt = 'SET IDENTITY_INSERT #StgCustSTaxExmpt ON '
				SELECT @SQLStmt = @SQlStmt + 'INSERT INTO #StgCustSTaxExmpt (' + RTRIM(LTRIM(@UpdColList)) + ') SELECT ' + RTRIM(LTRIM(@UpdColList)) + ' FROM StgCustSTaxExmpt WHERE SessionKey = ' + CONVERT(VARCHAR(10), @iSessionKey) + ' AND ProcessStatus = ' + CONVERT(VARCHAR(1), @NOT_PROCESSED) + ' AND Action = 2 OPTION(KEEP PLAN)'
				SELECT @SQlStmt = @SQlStmt + ' SET IDENTITY_INSERT #StgCustSTaxExmpt OFF'
				EXECUTE (@SQLStmt)	
			END
						
			IF LEN(RTRIM(LTRIM(@UpdColList))) = 0
			BEGIN
			-- If there is no updatable column specified but there are records with the 'Update' flag, mark the records
			-- to "Fail"
				SELECT	@UpdRecCount = 0

				-- Check for count of unverified records with the 'Update' flag
				SELECT  @UpdRecCount = COUNT(*) FROM #StgCustSTaxExmpt
				WHERE	SessionKey = @iSessionKey
				AND		ProcessStatus = @NOT_PROCESSED
				AND		Action = @REC_UPDATE
			
				-- Record found, run validation
				IF @UpdRecCount > 0
				BEGIN
					SELECT @EntityColName = 'COALESCE(CustID, '''')' + '+''|''+' + 'COALESCE(STaxCodeID, '''')'

					EXEC spDMValidateUpdColforUpdRec @iSessionKey, 0/*@iUseStageTable*/,'StgCustSTaxExmpt', @EntityColName,
							@LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @ErrorCount OUTPUT, @RetVal OUTPUT

					-- Update record count
					SELECT	@oFailedRecs = @oFailedRecs + @ErrorCount
					SELECT	@oRecsProcessed = @oRecsProcessed + @ErrorCount

					-- Cleanup #tdmMigrationLogEntryWrk
					TRUNCATE TABLE #tdmMigrationLogEntryWrk

					IF @iUseStageTable = 1
					BEGIN
						-- Sync Validation table to StgCustAddr
						UPDATE	StgCustSTaxExmpt
						SET		ProcessStatus = a.ProcessStatus
						FROM	StgCustSTaxExmpt
						JOIN	#StgCustSTaxExmpt a
						ON		StgCustSTaxExmpt.RowKey = a.RowKey
						WHERE	a.ProcessStatus = @GENRL_PROC_ERR
						OPTION(KEEP PLAN)	

						-- Sync Validation table to StgCustAddr
						DELETE	#StgCustSTaxExmpt
						WHERE	ProcessStatus = @GENRL_PROC_ERR
						OPTION(KEEP PLAN)
					END

					IF @RetVal <> @RET_SUCCESS
					BEGIN
						SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
						RETURN
					END
				END
			END

			-- StgCustSTaxExmpt.Action
			EXEC spDMListReplacement '#StgCustSTaxExmpt', 'Action',
			'StgCustSTaxExmpt', 'Action', @LIST_DBVALUE, @iSessionKey, @RetVal OUTPUT

			IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END
	 	
			IF EXISTS(SELECT * FROM #StgCustSTaxExmpt WHERE SessionKey = @iSessionKey)
			BEGIN
	 		
				SELECT @RowsProcThisCall = 0
	 	
				-- Now declare the Contact cursor (if necessary).
				IF CURSOR_STATUS ('global', 'curCustSTaxExmpt') = @STATUS_CURSOR_DOES_NOT_EXIST -- Cursor Not Allocated
				BEGIN
	 		
					DECLARE curCustSTaxExmpt INSENSITIVE CURSOR FOR
					SELECT	RowKey
					FROM #StgCustSTaxExmpt WITH (NOLOCK)
					WHERE ProcessStatus = @NOT_PROCESSED
					ORDER BY Action
					OPTION(KEEP PLAN)
	 				
		
				END
				-- open the CustSTaxExmpt Cursor
				OPEN curCustSTaxExmpt
	 		
				FETCH curCustSTaxExmpt INTO @RowKey
	 			
				WHILE (@@FETCH_STATUS = @FETCH_SUCCESSFUL)
				BEGIN -- begin of validation/insertion loop
	 	
					--Reset variables
					SELECT @CustID = NULL, @CustKey = NULL
						,@STaxCodeID = NULL, @STaxCodeKey = NULL
						,@ProcessStatus = @MIG_STAT_SUCCESSFUL, @Validated = 1
						,@ExmptNo = NULL,@RecordHadErrors = 0
						,@Duplicate = 0,@EntityID = NULL,@StringNo = NULL
	 			
					-- populate ID variables
					SELECT	@CustID = CustID,
					@STaxCodeID = STaxCodeID, @ExmptNo = ExmptNo,
							@Action = Action
					FROM #StgCustSTaxExmpt WITH (NOLOCK)
					WHERE RowKey = @RowKey
					OPTION(KEEP PLAN)

					-- CustKey From tarCustomer
					SELECT @CustKey = CustKey
					FROM tarCustomer WITH (NOLOCK)
					WHERE LTRIM(RTRIM(CustID)) = LTRIM(RTRIM(@CustID))
					AND CompanyID = @iCompanyID

					SELECT @CustAddrID = CA.CustAddrID FROM tarCustomer C  WITH (NOLOCK)
					INNER JOIN tarCustAddr CA WITH (NOLOCK)
					ON C.PrimaryAddrKey = CA.AddrKey
					WHERE C.CustKey = @CustKey
	 	
					--------------------------
					-- Get/Validate Key Values
					--------------------------
	 				
					SELECT @EntityID = LTRIM(RTRIM(COALESCE(@CustID,''))) + '|' + LTRIM(RTRIM(COALESCE(@CustAddrID,''))) + '|' + LTRIM(RTRIM(COALESCE(@STaxCodeID,'')))
	 	 	
					IF @CustKey IS NULL OR @CustKey = 0 -- Invalid CustKey
					BEGIN
	
						SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE--,@StringNo = @RPT_STRINGNO_TAXEXEMPT


	 					IF 	@iRptOption = 	@RPT_PRINT_ALL OR
	 						@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL
						BEGIN
	
							EXEC spciGetMenuTitle @TASKID_CUSTOMER, @TaskString OUTPUT
		
							EXEC ciBuildString @RPT_STRINGNO_SEEFORVALID, @LanguageID, @Message OUTPUT, @RetVal OUTPUT, 'CustID', @TaskString
		
		 						
	 						EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
	 						@EntityID, 'CustID', @CustID, @ProcessStatus, @Duplicate,
	 						@Message, @RetVal OUTPUT
		 	
	 						SELECT @StringNo = NULL, @Message = NULL

						END
	 	
					END			
	 	
					-- STaxCodeKey FROM tciSTaxCode
					SELECT @STaxCodeKey = STaxCodeKey
					FROM tciSTaxCode WITH (NOLOCK)
					WHERE LTRIM(RTRIM(STaxCodeID)) = @STaxCodeID

					IF @STaxCodeKey IS NULL OR @STaxCodeKey = 0 -- Invalid STaxCodeKey
					BEGIN 	
	
						SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE--,@StringNo = @RPT_STRINGNO_TAXEXEMPT

	 					IF 	@iRptOption = 	@RPT_PRINT_ALL OR
	 						@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL
		 	
	 					BEGIN

							EXEC spciGetMenuTitle @TASKID_STAXCODE, @TaskString OUTPUT

							EXEC ciBuildString @RPT_STRINGNO_SEEFORVALID, @LanguageID, @Message OUTPUT, @RetVal OUTPUT, 'STaxCodeID', @TaskString
	 						
							EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
							@EntityID, 'STaxCodeID', @STaxCodeID, @ProcessStatus, @Duplicate,
							@Message, @RetVal OUTPUT
	 	
							SELECT @StringNo = NULL, @Message = NULL

						END
					END

					-- Validate record exists for UPDATE/DELETE
					IF NOT EXISTS (SELECT * FROM tarCustSTaxExmpt cst WITH (NOLOCK)
							JOIN tarCustAddr ca WITH (NOLOCK)
							ON cst.AddrKey = ca.AddrKey						
							WHERE cst.STaxCodeKey = @STaxCodeKey
							AND	ca.CustKey = @CustKey)
							AND @Action IN (@REC_UPDATE, @REC_DELETE)
					BEGIN

						IF @Action = @REC_UPDATE
							SELECT @StringNo = @RPT_STRINGNO_UPDATE_FAILED
						
						IF @Action = @REC_DELETE
							SELECT @StringNo = @RPT_STRINGNO_DELETE_FAILED

						SELECT @Validated = 0, @ProcessStatus = @MIG_STAT_FAILURE--,@StringNo = @RPT_STRINGNO_TAXEXEMPT

							EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
							EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
							@EntityID, '', @STaxCodeID, @ProcessStatus, @Duplicate,
							@Message, @RetVal OUTPUT
	 	
							SELECT @StringNo = NULL, @Message = NULL

					END
					-- if passed validation then try to insert
					IF @Validated = 1
					BEGIN
						-- check for duplicate
						IF EXISTS (SELECT * FROM tarCustSTaxExmpt cst WITH (NOLOCK)
							JOIN tarCustAddr ca WITH (NOLOCK)
							ON cst.AddrKey = ca.AddrKey						
							WHERE cst.STaxCodeKey = @STaxCodeKey
							AND	ca.CustKey = @CustKey)
							AND (@Action = @REC_INSERT)

						BEGIN
							-- set message/process flags
							SELECT @StringNo = @RPT_STRINGNO_DUPLICATE
							
							--IF @Action = @REC_UPDATE
								--SELECT @StringNo = @RPT_STRINGNO_UPDATE_FAILED
							
							--IF @Action = @REC_DELETE
								--SELECT @StringNo = @RPT_STRINGNO_DELETE_FAILED
							
							SELECT @Duplicate = 1, @ProcessStatus = @MIG_STAT_WARNING -- no message needed for duplicates
							EXEC ciBuildString @StringNo, @LanguageID, @Message OUTPUT, @RetVal OUTPUT
						END
						ELSE
						BEGIN
						-- if not a duplicate then proceed
							BEGIN TRAN

							IF @Action = @REC_DELETE
							BEGIN
								DELETE tarCustSTaxExmpt
								WHERE StaxCodeKey = @STaxCodeKey
								AND	AddrKey IN
									(SELECT AddrKey FROM tarCustAddr WITH (NOLOCK)
										WHERE CustKey = @CustKey)
							
								-- set flags on failure
								IF @@ERROR <> 0
									SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
							END
							ELSE
							BEGIN
							IF @Action = @REC_UPDATE
								BEGIN
								UPDATE tarCustSTaxExmpt
								SET ExmptNo = @ExmptNo
								WHERE StaxCodeKey = @STaxCodeKey
								AND	AddrKey IN
									(SELECT AddrKey FROM tarCustAddr WITH (NOLOCK)
										WHERE CustKey = @CustKey)
								
										-- set flags on failure
											IF @@ERROR <> 0
												SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
								END
								ELSE
								BEGIN
									-- insert the CustSTaxExmp for each address record
									-- for the customer
									INSERT INTO tarCustSTaxExmpt (AddrKey, STaxCodeKey, ExmptNo)
									SELECT AddrKey, @STaxCodeKey, @ExmptNo
									FROM tarCustAddr WITH (NOLOCK)
									WHERE CustKey = @CustKey
							
									-- set flags on failure
									IF @@ERROR <> 0
										SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL
								END
							END
	 		
							IF @RecordHadErrors = 1-- an error occured
							BEGIN
								ROLLBACK TRANSACTION
								SELECT @ProcessStatus = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
							END
							ELSE -- an error did not occur
							BEGIN
								COMMIT TRANSACTION -- We commit the transaction here
								SELECT @ProcessStatus = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
							END
	 		
						END

	 					IF 	(@iRptOption = 	@RPT_PRINT_ALL) OR
	 						(@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL AND @ProcessStatus IN( @MIG_STAT_FAILURE, @MIG_STAT_WARNING)) OR
	 						(@iRptOption =  @RPT_PRINT_SUCCESSFUL AND @ProcessStatus = @MIG_STAT_SUCCESSFUL)
		 	
	 					BEGIN

							IF @ProcessStatus = @MIG_STAT_SUCCESSFUL
								SELECT @Message = CASE WHEN @Action = 1 THEN 'Record has been inserted.'
											WHEN @Action = 2 THEN 'Record has been updated.'
											WHEN @Action = 3 THEN 'Record has been deleted.' END				
		 					
	 						-- report if neccessary				
	 						EXEC spDMCreateMigrationLogEntry @LanguageID, @iSessionKey,
	 						@EntityID, '', '', @ProcessStatus, @Duplicate,
	 						@Message, @RetVal OUTPUT
		 		
	 					END -- Reporting was requested
					END

					-- update failed count
					IF @ProcessStatus IN( @MIG_STAT_FAILURE, @MIG_STAT_WARNING)
						SELECT @lRecFailed = @lRecFailed + 1
					-- update rows processed
					SELECT @RowsProcThisCall = @RowsProcThisCall + 1
	 	
					IF @iUseStageTable = 1
					BEGIN -- using Staging tables
	 				
						IF @ProcessStatus  IN( @MIG_STAT_FAILURE, @MIG_STAT_WARNING)
						BEGIN
							
					 		-- update the process status in the real stage table
							UPDATE StgCustSTaxExmpt SET ProcessStatus = @GENRL_PROC_ERR-- did not process
							FROM StgCustSTaxExmpt WITH (NOLOCK)
							WHERE RowKey = @RowKey
	 					
				   		END -- using staging table
						ELSE-- @MIG_STAT_SUCCESSFUL
						BEGIN
							DELETE StgCustSTaxExmpt
							WHERE RowKey = @RowKey
	 		
						END -- Sucessful
	 	
					END-- IF using Staging tables
	 						
					-------------------------------------------------------------------
					-- Process the temp tables
					-------------------------------------------------------------------
	 	
					IF @ProcessStatus IN( @MIG_STAT_FAILURE, @MIG_STAT_WARNING)
					BEGIN
						-- update the temp table rrow if failed
						UPDATE #StgCustSTaxExmpt SET ProcessStatus = @GENRL_PROC_ERR
						FROM #StgCustSTaxExmpt WITH (NOLOCK)
						WHERE RowKey = @RowKey
						OPTION(KEEP PLAN)
	 	
					END
					ELSE
					BEGIN
						-- delete the temp table row if successful
						DELETE #StgCustSTaxExmpt
						WHERE RowKey = @RowKey
						OPTION(KEEP PLAN)
	 		
					END
	 	
					FETCH curCustSTaxExmpt INTO @RowKey
	
				END
	 	
				-- -- deal with the cursor when finished
				IF CURSOR_STATUS ('global', 'curCustSTaxExmpt') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
				BEGIN
					CLOSE curCustSTaxExmpt
				END -- End [We need to close the CustSTaxExmpt cursor.]
	 	
	 	
				-- set the output for rows processed
				SELECT @oRecsProcessed = @oRecsProcessed + @RowsProcThisCall, @oFailedRecs = @oFailedRecs + @lRecFailed
	 	
	 	
			END
	 	
			IF CURSOR_STATUS ('global', 'curCustSTaxExmpt') = @STATUS_CURSOR_CLOSED -- Time to Deallocate the Contact Cursor
			BEGIN
				DEALLOCATE curCustSTaxExmpt
			END --

			-- Determine if any remaining rows exist for insertion.
			--        If not, then consider this successful.
			SELECT @SourceRowCount = COUNT(*)
				FROM #StgCustSTaxExmpt WITH (NOLOCK)
				WHERE ProcessStatus = @NOT_PROCESSED	
			
		END -- if @oContinue

		-- Exit if there are no rows to migrate.  Consider this success and tell the caller
		-- not to continue calling this SP.
		--IF @SourceRowCount = 0
		--	BEGIN
		--		SELECT @_oRetVal = @RET_SUCCESS, @oContinue = 0
		--	END

		IF @iUseStageTable = 1
		BEGIN
			IF (SELECT COUNT(*) FROM StgCustSTaxExmpt WITH (NOLOCK) WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED) > 0
			BEGIN
				SELECT @oContinue = 1
				SELECT @_oRetVal = @RET_SUCCESS
				RETURN	
			END
		END
	END
	
	------------------------------------------
	-- Start processing StgCustDocTrnsmit
	------------------------------------------
	SELECT @RowsProcThisCall = 0, @lRecFailed = 0
-- **********
	SELECT @StartDate = GetDate()
	-- added AND @SourceRowCount to continue until no more rows need processing
	IF @iUseStageTable = 1
	BEGIN
		SELECT @SourceRowCount = COUNT(*) FROM StgCustDocTrnsmit WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
	END
	ELSE
	BEGIN
		SELECT @SourceRowCount = COUNT(*) FROM #StgCustDocTrnsmit WHERE SessionKey = @iSessionKey AND ProcessStatus = @NOT_PROCESSED
	END
		
	
		
	WHILE  DateDiff(s,@StartDate, GetDate()) < @MIGRATE_SECS AND @SourceRowCount > 0
	BEGIN 		

		IF @iUseStageTable = 1
		BEGIN
			UPDATE 	StgCustDocTrnsmit
			SET	Action = @REC_AUTO
			WHERE 	Action IS NULL
			AND SessionKey = @iSessionKey
			OPTION (KEEP PLAN)

			--UPDATE Action flag if it is specified as Automatic
			--Set Action to 'Update' if record already exists in database
			--Otherwise, set the Action to 'Insert'.
			UPDATE 	StgCustDocTrnsmit
			SET	Action = CASE WHEN c.CustKey IS NOT NULL THEN @REC_UPDATE
						ELSE @REC_INSERT END
			FROM	StgCustDocTrnsmit
			LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
			ON	StgCustDocTrnsmit.CustID = c.CustID
			AND	c.CompanyID = @iCompanyID
			WHERE 	StgCustDocTrnsmit.Action = @REC_AUTO
			OPTION(KEEP PLAN)
		END
		ELSE
		BEGIN
			UPDATE 	#StgCustDocTrnsmit
			SET	Action = @REC_AUTO
			WHERE 	Action IS NULL
			AND SessionKey = @iSessionKey
			OPTION (KEEP PLAN)

			--UPDATE Action flag if it is specified as Automatic
			--Set Action to 'Update' if record already exists in database
			--Otherwise, set the Action to 'Insert'.
			UPDATE 	#StgCustDocTrnsmit
			SET	Action = CASE WHEN c.CustKey IS NOT NULL THEN @REC_UPDATE
						ELSE @REC_INSERT END
			FROM	#StgCustDocTrnsmit
			LEFT OUTER JOIN tarCustomer c WITH (NOLOCK)
			ON	#StgCustDocTrnsmit.CustID = c.CustID
			AND	c.CompanyID = @iCompanyID
			WHERE 	#StgCustDocTrnsmit.Action = @REC_AUTO
			OPTION(KEEP PLAN)
		END

		IF @iUseStageTable = 1
		BEGIN
			-- Populate #StgCustDocTrnsmit if using staging tables--
			TRUNCATE TABLE #StgCustDocTrnsmit

			SET IDENTITY_INSERT #StgCustDocTrnsmit ON

			INSERT #StgCustDocTrnsmit
			(RowKey,CustID,EMail,EMailFormat,Fax,HardCopy,TranType,ProcessStatus,SessionKey,Action)
			SELECT CDT.RowKey,CDT.CustID,CDT.EMail,CDT.EMailFormat,CDT.Fax,CDT.HardCopy
			,CDT.TranType,CDT.ProcessStatus,CDT.SessionKey, Action
			FROM StgCustDocTrnsmit CDT WITH (NOLOCK)
			WHERE CDT.SessionKey = @iSessionKey
			AND CDT.Action = @REC_INSERT
			AND CDT.ProcessStatus = @NOT_PROCESSED
			OPTION (KEEP PLAN)

			SET ROWCOUNT 0
			
			SET IDENTITY_INSERT #StgCustDocTrnsmit OFF
		END

		--Build insert statement for the updated record
		TRUNCATE TABLE #CustDocTranUpdColumnList

		INSERT INTO #CustDocTranUpdColumnList
					(SessionKey, ColumnName)
		SELECT		@iSessionKey, tc.InternalColumnName
		FROM	tdiDataTrnsfrColumnMap tc WITH (NOLOCK)
			JOIN	(SELECT DISTINCT DataTrnsfrjobDefStepKey
							FROM tdiDataTrnsfrJobDefStep tjs WITH (NOLOCK)
								JOIN 	tdiDataTrnsfrJobExecute tj WITH (NOLOCK)	ON	tjs.DataTrnsfrJobDefKey = tj.DataTrnsfrJobDefKey
							WHERE tj.SessionKey = @iSessionkey AND tjs.InternalTableName = 'StgCustDocTrnsmit') derv
						ON	tc.DataTrnsfrjobDefStepKey = derv.DataTrnsfrjobDefStepKey
		WHERE	tc.AllowUpdate = 1

		SELECT	@UpdColList = ''

		SELECT	@UpdColList = @UpdColList + List.ColumnName,
				@UpdColList = @UpdColList + ', '
		FROM 	#CustDocTranUpdColumnList List
		
		-- If there are updatable column specified
		IF LEN(RTRIM(LTRIM(@UpdColList))) > 0
		BEGIN
			SELECT @UpdColList = SUBSTRING(@UpdColList, 1, LEN(@UpdColList) - 1)
			SELECT @UpdColList = 'RowKey, SessionKey, CustID, TranType, ProcessStatus, Action, ' + @UpdColList

			SELECT @SQLStmt = 'SET IDENTITY_INSERT #StgCustDocTrnsmit ON '
			SELECT @SQLStmt = @SQLStmt + 'INSERT INTO #StgCustDocTrnsmit (' + RTRIM(LTRIM(@UpdColList)) + ') SELECT ' + RTRIM(LTRIM(@UpdColList)) + ' FROM StgCustDocTrnsmit WHERE SessionKey = ' + CONVERT(VARCHAR(10), @iSessionKey) + ' AND ProcessStatus = ' + CONVERT(VARCHAR(1), @NOT_PROCESSED) + ' AND Action = 2 OPTION(KEEP PLAN)'
			SELECT @SQLStmt = @SQLStmt + ' SET IDENTITY_INSERT #StgCustDocTrnsmit OFF'
			EXECUTE (@SQLStmt)	
		END

		SET ROWCOUNT 0
						
		IF LEN(RTRIM(LTRIM(@UpdColList))) = 0
		BEGIN
		-- If there is no updatable column specified but there are records with the 'Update' flag, mark the records
		-- to "Fail"
			SELECT	@UpdRecCount = 0

			-- Check for count of unverified records with the 'Update' flag
			SELECT  @UpdRecCount = COUNT(*) FROM #StgCustDocTrnsmit
			WHERE	SessionKey = @iSessionKey
			AND		ProcessStatus = @NOT_PROCESSED
			AND		Action = @REC_UPDATE
		
			-- Record found, run validation
			IF @UpdRecCount > 0
			BEGIN
				SELECT @EntityColName = 'COALESCE(CustID, '''')' + '+''|''+' + 'COALESCE(TranType, '''')'

				EXEC spDMValidateUpdColforUpdRec @iSessionKey, 0/*@iUseStageTable*/,'StgCustDocTrnsmit', @EntityColName,
						@LanguageID, 1 /*LogError*/, @iRptOption, @iCompanyID, @ErrorCount OUTPUT, @RetVal OUTPUT

				-- Update record count
				SELECT	@oFailedRecs = @oFailedRecs + @ErrorCount
				SELECT	@oRecsProcessed = @oRecsProcessed + @ErrorCount

				-- Cleanup #tdmMigrationLogEntryWrk
				TRUNCATE TABLE #tdmMigrationLogEntryWrk

				IF @iUseStageTable = 1
				BEGIN
					-- Sync Validation table to StgCustAddr
					UPDATE	StgCustDocTrnsmit
					SET		ProcessStatus = a.ProcessStatus
					FROM	StgCustDocTrnsmit
					JOIN	#StgCustDocTrnsmit a
					ON		StgCustDocTrnsmit.RowKey = a.RowKey
					WHERE	a.ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)	

					-- Sync Validation table to StgCustAddr
					DELETE	#StgCustDocTrnsmit
					WHERE	ProcessStatus = @GENRL_PROC_ERR
					OPTION(KEEP PLAN)
				END

				IF @RetVal <> @RET_SUCCESS
				BEGIN
					SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
					RETURN
				END
			END
		END

		SET IDENTITY_INSERT #StgCustDocTrnsmit OFF
		SET ROWCOUNT 0

		-- Exit if there are no rows to migrate.  Consider this success and tell the caller
		-- not to continue calling this SP.
		IF @SourceRowCount = 0
			BEGIN
				SELECT @_oRetVal = @RET_SUCCESS, @oContinue = 0
			END

		---------------------------------------		
		--Populate child validation tables
		---------------------------------------

		-- populate the StgCustDocTrnsmit temp table with the migrated keys
		-- to keep track of which rows are being migrated vs. generated
		TRUNCATE TABLE #tarCustDocTrnsmitWrk
		INSERT #tarCustDocTrnsmitWrk (RowKey, ProcessStatus, Action)
		SELECT RowKey,ProcessStatus, Action FROM #StgCustDocTrnsmit

		UPDATE	#tarCustDocTrnsmitWrk
		SET 	CustKey = c.CustKey
		FROM	#tarCustDocTrnsmitWrk
		JOIN	#StgCustDocTrnsmit sdt
		ON	#tarCustDocTrnsmitWrk.RowKey = sdt.RowKey
		JOIN	tarCustomer c WITH (NOLOCK)
		ON	sdt.CustID = c.CustID
		AND	c.CompanyID = @iCompanyID
		OPTION(KEEP PLAN)

		IF @@ERROR <> 0
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			RETURN
		END

		IF EXISTS(SELECT * FROM #tarCustDocTrnsmitWrk WITH (NOLOCK) WHERE
			CustKey IS NULL)
		BEGIN

			UPDATE 	#tarCustDocTrnsmitWrk
			SET 	ProcessStatus = @GENRL_PROC_ERR	
			WHERE	CustKey IS NULL
			OPTION(KEEP PLAN)

			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
				RETURN
			END

			EXEC spciGetMenuTitle @TASKID_CUSTOMER, @TaskString OUTPUT

			EXEC ciBuildString @RPT_STRINGNO_SEEFORVALID, @LanguageID, @Message OUTPUT, @RetVal OUTPUT, 'CustID', @TaskString

			IF @@ERROR <> 0 OR @RetVal <> @RET_SUCCESS
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR
				RETURN
			END

			INSERT #tdmMigrationLogEntryWrk
			(ColumnID,ColumnValue,Comment,Duplicate,
			EntityID,SessionKey,Status)
			SELECT 'CustID', COALESCE(CustID,''), @Message, 0,
			COALESCE(LTRIM(RTRIM(CustID)),'') + '|' + COALESCE(LTRIM(RTRIM(TranType)),''), @iSessionKey,@MIG_STAT_FAILURE
			FROM	#StgCustDocTrnsmit sdt WITH (NOLOCK)
			JOIN	#tarCustDocTrnsmitWrk cdt WITH (NOLOCK)
			ON	sdt.RowKey = cdt.RowKey
			WHERE cdt.CustKey IS NULL
			OPTION(KEEP PLAN)

			IF @@ERROR <> 0
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR
				RETURN
			END
	
		
			EXEC spDMCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

			IF @@ERROR <> 0 OR @RetVal <> @RET_SUCCESS
			BEGIN
				SELECT @_oRetVal = @RET_UNKNOWN_ERR
				RETURN
			END

			TRUNCATE TABLE #tdmMigrationLogEntryWrk
			SELECT @StringNo = NULL
		END

		-- Call to setup and Validate DocTrnsmit table
		EXEC spARCustDocTrnsmitVal @iSessionKey, @iCompanyID, @LanguageID, @RetVal OUTPUT

		IF @RetVal <> @RET_SUCCESS OR @@ERROR <> 0
		BEGIN
			SELECT @_oRetVal = @RET_UNKNOWN_ERR, @oContinue = 0
			RETURN
		END

		SELECT 	@lRecFailed = COUNT(*)
	   	FROM	#tarCustDocTrnsmitWrk	
	   	WHERE 	ProcessStatus = @GENRL_PROC_ERR

		SELECT	@RowsProcThisCall = @RowsProcThisCall + COALESCE(@lRecFailed,0)

		DELETE 	#StgCustDocTrnsmit
		WHERE	ProcessStatus = @GENRL_PROC_ERR

		DELETE	#tarCustDocTrnsmitWrk
		WHERE	ProcessStatus = @GENRL_PROC_ERR

		IF EXISTS(SELECT * FROM #StgCustDocTrnsmit WHERE SessionKey = @iSessionKey)
		BEGIN

			BEGIN TRAN

			UPDATE c
			SET 	c.EMail = CASE WHEN EXISTS( SELECT 1 FROM #CustDocTranUpdColumnList WHERE ColumnName = 'EMail')
								 THEN CONVERT(SMALLINT, cdt.EMail)
								ELSE c.EMail END,
				c.EMailFormat = CASE WHEN EXISTS( SELECT 1 FROM #CustDocTranUpdColumnList WHERE ColumnName = 'EMailFormat')
								 THEN CONVERT(SMALLINT, cdt.EMailFormat)
								ELSE c.EMailFormat END,
				c.Fax = CASE WHEN EXISTS( SELECT 1 FROM #CustDocTranUpdColumnList WHERE ColumnName = 'Fax')
								 THEN CONVERT(SMALLINT, cdt.Fax)
								ELSE c.Fax END,
				c.HardCopy = CASE WHEN EXISTS( SELECT 1 FROM #CustDocTranUpdColumnList WHERE ColumnName = 'HardCopy')
								 THEN CONVERT(SMALLINT, cdt.HardCopy)
								ELSE c.HardCopy END
			FROM	tarCustDocTrnsmit c
			JOIN	#tarCustDocTrnsmitWrk ctw
			ON	c.CustKey = ctw.CustKey
			JOIN	#StgCustDocTrnsmit cdt
			ON	ctw.RowKey = cdt.RowKey
			AND	c.TranType = CONVERT(INT,cdt.TranType)
			OPTION(KEEP PLAN)

			-- set flags on failure
			IF @@ERROR <> 0
				SELECT @RecordHadErrors = 1, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL

			IF @RecordHadErrors = 1-- an error occured
			BEGIN
				ROLLBACK TRANSACTION
				SELECT @ProcessStatus = @MIG_STAT_FAILURE, @StringNo = @RPT_STRINGNO_UNSUCCESSFUL					
			END
			ELSE -- an error did not occur
			BEGIN
				COMMIT TRANSACTION -- We commit the transaction here
				SELECT @ProcessStatus = @MIG_STAT_SUCCESSFUL, @StringNo = NULL
			END
	
			IF 	(@iRptOption = 	@RPT_PRINT_ALL) OR
				(@iRptOption = 	@RPT_PRINT_UNSUCCESSFUL AND @ProcessStatus = @MIG_STAT_FAILURE) OR
				(@iRptOption =  @RPT_PRINT_SUCCESSFUL AND @ProcessStatus = @MIG_STAT_SUCCESSFUL)

			BEGIN						
				
				INSERT	#tdmMigrationLogEntryWrk
				(ColumnID,ColumnValue,Duplicate,Comment,
				EntityID,Status,SessionKey)
				SELECT DISTINCT '','',0,  CASE WHEN a.Action = 1 THEN 'Record has been inserted.'
								WHEN a.Action = 2 THEN 'Record has been updated.'
								WHEN a.Action = 3 THEN 'Record has been deleted.' END ,
				COALESCE(RTRIM(LTRIM(a.CustID)),'') + '|' + COALESCE(RTRIM(LTRIM(a.TranType)), ''),@ProcessStatus, @iSessionKey
				FROM	#StgCustDocTrnsmit a
				INNER JOIN #tarCustDocTrnsmitWrk b
				ON a.RowKey = b.RowKey
				WHERE a.ProcessStatus <> @GENRL_PROC_ERR
				OPTION(KEEP PLAN)				
								
				EXEC spdmCreateMigrationLogEntries @LanguageID, @iSessionKey, @RetVal OUTPUT

				TRUNCATE TABLE #tdmMigrationLogEntryWrk
	
			END -- Reporting was requested

			-- update failed count
			IF @ProcessStatus = @MIG_STAT_FAILURE
				SELECT @lRecFailed = COUNT(*) FROM #StgCustDocTrnsmit	
				
			-- update rows processed
			SELECT @RowsProcThisCall = @RowsProcThisCall + (SELECT COUNT(*) FROM #StgCustDocTrnsmit)
					OPTION(KEEP PLAN)
	
			IF @iUseStageTable = 1
			BEGIN -- using Staging tables
			
				IF @ProcessStatus = @MIG_STAT_FAILURE
				BEGIN
				
				 	-- update the process status in the real stage table
					UPDATE 	StgCustDocTrnsmit
					SET 	ProcessStatus = @GENRL_PROC_ERR-- did not process
					FROM 	StgCustDocTrnsmit WITH (NOLOCK)
					JOIN	#tarCustDocTrnsmitWrk wrk WITH (NOLOCK)
					ON 	StgCustDocTrnsmit.RowKey = wrk.RowKey
				
			   	END -- using staging table
				ELSE-- @MIG_STAT_SUCCESSFUL
				
				BEGIN

					DELETE 	StgCustDocTrnsmit
					FROM 	StgCustDocTrnsmit
					JOIN	#tarCustDocTrnsmitWrk wrk WITH (NOLOCK)
					ON 	StgCustDocTrnsmit.RowKey = wrk.RowKey
	
				END -- Sucessful

			END-- IF using Staging tables
						
			-------------------------------------------------------------------
			-- Process the temp tables
			-------------------------------------------------------------------

			IF @ProcessStatus = @MIG_STAT_FAILURE
			BEGIN
				-- update the temp table rrow if failed
				UPDATE 	#StgCustDocTrnsmit
				SET 	ProcessStatus = @GENRL_PROC_ERR
				FROM 	#StgCustDocTrnsmit
				JOIN 	#tarCustDocTrnsmitWrk wrk WITH (NOLOCK)
				ON 	#StgCustDocTrnsmit.RowKey = wrk.RowKey
				OPTION(KEEP PLAN)

			END
			ELSE
			BEGIN
				-- delete the temp table row if successful
				-- DELETE 	StgCustDocTrnsmit
				-- FROM 	StgCustDocTrnsmit
				-- JOIN	#StgCustDocTrnsmit wrk WITH (NOLOCK)
				-- ON 	StgCustDocTrnsmit.RowKey = wrk.RowKey
			-- OPTION(KEEP PLAN)
				DELETE 	#StgCustDocTrnsmit WHERE ProcessStatus <> @GENRL_PROC_ERR
			END
		END

		-- set the output for rows processed
		SELECT 	@oRecsProcessed = @oRecsProcessed + @RowsProcThisCall,
			@oFailedRecs = @oFailedRecs + @lRecFailed

		-- Determine if any remaining rows exist for insertion.  If not, then consider this successful.
			SELECT @SourceRowCount = COUNT(*) FROM #StgCustDocTrnsmit WHERE ProcessStatus = @NOT_PROCESSED
			
	END -- IF @oContinue = 0

	SELECT @oContinue = 0

	SELECT @_oRetVal = @RET_SUCCESS

	RETURN

CloseAndDeallocateCursors:

	--We are not going to continue running this sp.
	SELECT @oContinue = 0

	--Close and Deallocate the CustAddr cursor, if it exists.
	IF CURSOR_STATUS ('global', 'curCustAddr') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
	BEGIN
		CLOSE curCustAddr
	END

	IF CURSOR_STATUS ('global', 'curCustAddr') = @STATUS_CURSOR_CLOSED -- Time to Deallocate the Cursor
	BEGIN
		DEALLOCATE curCustAddr
	END

	--Close and Deallocate the Contact cursor, if it exists.
	IF CURSOR_STATUS ('global', 'curContact') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
	BEGIN
		CLOSE curContact
	END

	IF CURSOR_STATUS ('global', 'curContact') = @STATUS_CURSOR_CLOSED -- Time to Deallocate the Cursor
	BEGIN
		DEALLOCATE curContact
	END


	--Close and Deallocate the Contact cursor, if it exists.
	IF CURSOR_STATUS ('global', 'curCustSTaxExmpt') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
	BEGIN
		CLOSE curCustSTaxExmpt
	END

	IF CURSOR_STATUS ('global', 'curCustSTaxExmpt') = @STATUS_CURSOR_CLOSED -- Time to Deallocate the Cursor
	BEGIN
		DEALLOCATE curCustSTaxExmpt
	END


	--Close and Deallocate the Contact cursor, if it exists.
	IF CURSOR_STATUS ('global', 'curCustDocTrnsmit') IN (@STATUS_CURSOR_OPEN_BUT_EMPTY, @STATUS_CURSOR_OPEN_WITH_DATA)
	BEGIN
		CLOSE curCustDocTrnsmit
	END

	IF CURSOR_STATUS ('global', 'curCustDocTrnsmit') = @STATUS_CURSOR_CLOSED -- Time to Deallocate the Cursor
	BEGIN
		DEALLOCATE curCustDocTrnsmit
	END

	SET NOCOUNT OFF

	SET NOCOUNT OFF

END -- MAIN CODE BLOCK

--GO

--GRANT EXECUTE ON spARapiCustomerIns TO Public

--GO

GO



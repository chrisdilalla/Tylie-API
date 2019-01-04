USE [MAS500_Test]
GO

/****** Object:  StoredProcedure [dbo].[spARCustomerImport_Tylie]    Script Date: 04.01.19 14:18:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



ALTER procedure [dbo].[spARCustomerImport_Tylie](
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
	@iSessionKey			INTEGER=0,			
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



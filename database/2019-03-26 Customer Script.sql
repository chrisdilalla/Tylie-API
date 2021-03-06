USE [MAS500_Test]
GO
/****** Object:  StoredProcedure [dbo].[spARCustomerImport_Tylie]    Script Date: 3/26/2019 3:29:17 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER procedure [dbo].[spARCustomerImport_Tylie](
	@_oRetVal			INTEGER	OUTPUT	,@message varchar(8000) output)
AS
/* declare @_oRetVal			INT
exec spARCustomerImport_Tylie
	@_oRetVal		OUTPUT
	*/	
Begin

declare @custkey int,@cntctkey int,@addrkey int,@companyid varchar(3),@counter int,@countmax int,@custid varchar(30)

set @companyid='TJA'

set @counter =(select min(rowkey) from stgCustomer_Tylie where isnull(importStatus,0)=0)
set @countmax= (select max(rowkey) from stgCustomer_Tylie where isnull(importStatus,0)=0)

---Outer Loop

while @counter<=@countmax
begin
 print 'loop entry'
 set @custkey=null

set @custid=''
set @custid=(select custid from stgCustomer_Tylie where rowkey=@counter)

set @custkey=(select custkey from tarCustomer where custid=@custid and CompanyID=@companyid)

print 'custkey'
print @custkey

if isnull(@custkey,0) <>0
begin
	goto skip_Insert
end

if isnull(@custkey,0) =0
begin

print 'new keys'
exec spGetNextSurrogateKey 'tcicontact',@cntctkey output
exec spGetNextSurrogateKey 'tciaddress',@addrkey output
exec spGetNextSurrogateKey 'tarcustomer',@custkey output

insert into tcicontact (CntctKey
,CCCreditMemo
,CCCustStmnt
,CCDebitMemo
,CCFinanceCharge
,CCInvoice
,CCPurchaseOrder
,CCRMA
,CCSalesOrder
,CntctOwnerKey
,CreateDate
,CreateType
,CreateUserID
,CRMContactID
,EMailAddr
,EMailFormat
,EntityType
,ExtUser
,Fax
,FaxExt
,ImportLogKey
,Name
,Phone
,PhoneExt
,Title
,UpdateCounter
,UpdateDate
,UpdateUserID)
select @CntctKey
,0
,0
,0
,0
,0
,0
,0
,0
,@custkey
,getdate()
,2
,'admin'
,null
,ContactEmail
,1
,501
,'0'
,ContactFax
,''
,null
,contactName
,contactPhone
,null
,contactTitle
,0
,getdate()
,'admin'
from stgCustomer_Tylie
where rowkey=@counter

insert into tciaddress (AddrKey
,AddrLine1
,AddrLine2
,AddrLine3
,AddrLine4
,AddrLine5
,AddrName
,City
,CountryID
,CRMAddrID
,Fax
,FaxExt
,Latitude
,Longitude
,Phone
,PhoneExt
,PostalCode
,Residential
,StateID
,TransactionOverride
,UpdateCounter)
select @AddrKey
,AddrLine1
,AddrLine2
,null
,null
,null
,BrandID
,City
,CountryID
,null
,contactFax
,null
,null
,null
,contactPhone
,null
,PostalCode
,0
,StateID
,0
,0
from stgCustomer_Tylie
where rowkey=@counter


insert into tarcustomer (CustKey
,ABANo
,AllowCustRefund
,AllowWriteOff
,BillingType
,BillToNationalAcctParent
,CompanyID
,ConsolidatedStatement
,CreateDate
,CreateType
,CreateUserID
,CreditLimit
,CreditLimitAgeCat
,CreditLimitUsed
,CRMCustID
,CurrExchSchdKey
,CustClassKey
,CustID
,CustName
,CustRefNo
,DateEstab
,DfltBillToAddrKey
,DfltItemKey
,DfltMaxUpCharge
,DfltMaxUpChargeType
,DfltSalesAcctKey
,DfltSalesReturnAcctKey
,DfltShipToAddrKey
,FinChgFlatAmt
,FinChgPct
,Hold
,ImportLogKey
,NationalAcctLevelKey
,PmtByNationalAcctParent
,PrimaryAddrKey
,PrimaryCntctKey
,PrintDunnMsg
,ReqCreditLimit
,ReqPO
,RetntPct
,SalesSourceKey
,ShipPriority
,Status
,StdIndusCodeID
,StmtCycleKey
,StmtFormKey
,TradeDiscPct
,UpdateCounter
,UpdateDate
,UpdateUserID
,UserFld1
,UserFld2
,UserFld3
,UserFld4
,VendKey)
select @CustKey
,''
,0
,1
,1
,0
,@CompanyID
,0
,getdate()
,2
,'admin'
,0
,0
,0
,''
,null
,CustClassKey
,CustID
,a.CustName
,''
,getdate()
,@AddrKey
,null
,0
,0
,DfltSalesAcctKey
,b.SalesReturnAcctKey
,@AddrKey
,FinChgFlatAmt
,FinChgPct
,0
,null
,null
,0
,@AddrKey
,@CntctKey
,PrintDunnMsg
,0
,0
,0
,null
,3
,1
,null
,null
,149
,0
,0
,getdate()
,'admin'
,null
,null
,null
,null
,null
from stgCustomer_Tylie a
inner join tarcustclass b on a.CustClassID=b.CustClassID
where rowkey=@counter and b.companyid='TJA'

insert into tarcustaddr (AddrKey
,AllowInvtSubst
,BackOrdPrice
,BOLReqd
,CarrierAcctNo
,CarrierBillMeth
,CloseSOLineOnFirstShip
,CloseSOOnFirstShip
,CommPlanKey
,CreateDate
,CreateType
,CreateUserID
,CreditCardKey
,CurrExchSchdKey
,CurrID
,CustAddrID
,CustKey
,CustPriceGroupKey
,DfltCntctKey
,FOBKey
,FreightMethod
,ImportLogKey
,InvcFormKey
,InvcMsg
,InvoiceReqd
,LanguageID
,PackListContentsReqd
,PackListFormKey
,PackListReqd
,PmtTermsKey
,PriceAdj
,PriceBase
,PrintOrderAck
,RequireSOAck
,SalesTerritoryKey
,ShipComplete
,ShipDays
,ShipLabelFormKey
,ShipLabelsReqd
,ShipMethKey
,ShipZoneKey
,SOAckFormKey
,SOAckMeth
,SperKey
,STaxSchdKey
,UpdateDate
,UpdateUserID
,WhseKey
,UsePromoPrice)
select @AddrKey
,0
,0
,0
,null
,6
,0
,0
,null
,getdate()
,1
,'admin'
,null
,null
,'USD'
,BrandID
,@CustKey
,null
,@CntctKey
,null
,2
,null
,150
,null
,0
,1033
,0
,null
,0
,45
,0
,0
,0
,0
,null
,0
,0
,null
,0
,482
,null
,null
,0
,null
,null
,getdate()
,'admin'
,23
,0
from stgCustomer_Tylie
where rowkey=@counter

update stgCustomer_Tylie
set importStatus=1  ---inserted
where rowkey=@counter

goto skip_update
 
end   ---end of inserts

skip_insert:

print 'skipped insert'

update tarcustomer
set custclasskey=b.CustClassKey
from stgCustomer_Tylie a
inner join tarCustClass b on a.CustClassID=b.CustClassID
inner join tarcustomer  on a.custid=tarcustomer.custid
where rowkey=@counter and tarcustomer.CompanyID=@companyid

print '392'

update tciaddress
set addrline1=a.AddrLine1
,addrline2=a.AddrLine2
,city=a.City
,StateID=a.StateID
,CountryID=a.CountryID
,PostalCode=a.PostalCode
from stgCustomer_Tylie a
inner join tarcustomer c on a.custid=c.custid
inner join tciaddress on c.PrimaryAddrKey=tciAddress.AddrKey
where rowkey=@counter and c.CompanyID=@companyid

print '405'

update tcicontact
set Name=a.ContactName
,Title=a.ContactTitle
,fax=a.ContactFax
,phone=a.ContactPhone
,EMailAddr=a.ContactEmail
from stgCustomer_Tylie a
inner join tarcustomer c on a.custid=c.custid
inner join tciContact on c.PrimaryCntctKey=tciContact.CntctKey
where rowkey=@counter and c.CompanyID=@companyid

print '392'
print @counter

update stgCustomer_Tylie
set importStatus=2  ---updated
where rowkey=@counter

skip_update:

set @counter =(select min(rowkey) from stgCustomer_Tylie where isnull(importStatus,0)=0 and rowkey>@counter)

end   ---end of outer loop

End


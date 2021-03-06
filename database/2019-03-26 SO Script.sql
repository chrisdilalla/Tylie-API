USE [MAS500_Test]
GO
/****** Object:  StoredProcedure [dbo].[spSOapiSalesOrdIns_Tylie]    Script Date: 3/26/2019 3:27:31 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


ALTER procedure [dbo].[spSOapiSalesOrdIns_Tylie]
	(	@_oRetVal			INT		OUTPUT,@message varchar(8000) output
	)
AS
/*
declare @_oRetVal			INT
exec spSOapiSalesOrdIns_Tylie
		@_oRetVal		OUTPUT
*/

declare @sokey int,@solinekey int, @solinedistkey int, @companyid varchar(3),
@pokey int,@polinekey int, @polinedistkey int, @socount varchar(30), @somax varchar(30), @pocount int, 
@pomax int,@SONum varchar(10), @POnum varchar(10),@trandate datetime,@whsekey int,@tranamt decimal(15,3),
@unitmeaskey int,@POTranAmt decimal(15,3),@solinecount int,@solinemax int,@solineno int,@polinecount int,
@polinemax int,@polineno int, @dfltpmttermskey int, @invttrandistkey int

set @companyid='TJA'
set @whsekey=(select whsekey from timwarehouse where companyid=@companyid and whseid='001')

set @unitmeaskey=(select unitmeaskey from tciUnitMeasure where CompanyID=@companyid and UnitMeasID='each')
set @dfltpmttermskey = (Select top 1 pmttermskey from tciPaymentTerms where CompanyID=@companyid and PmtTermsID like '%Due On Receipt%')
---Load sales orders

Set @socount=(select min(WorkOrderNo) from StgSOLine_Tylie where isnull(status,0)=0)
Set @somax=(select max(WorkOrderNo) from StgSOLine_Tylie where isnull(status,0)=0)

while @socount<=@somax
begin

set @tranamt=0  ----gets updated later
set @trandate=(select max(trandate) from StgSOLine_Tylie where WorkOrderNo=@socount)
set @sonum=(select max(WorkOrderNo) from StgSOLine_Tylie where WorkOrderNo=@socount)

set @sonum=right('0000000000'+@sonum,10)

exec spGetNextSurrogateKey
	'tsoSalesOrdLog',@soKey OUTPUT

	Print '45'

insert into tsoSalesOrdLog (SOKey
,CompanyID
,TranAmtHC
,TranDate
,TranNo
,TranNoRel
,TranStatus
,TranType)
select @SOKey
,@CompanyID
,@TranAmt
,@TranDate
,@SONum
,@SONum
,2
,801

Print '64'

insert into tsoSalesOrder (SOKey
,AckDate
,BillToAddrKey
,BillToCustAddrKey
,BlnktRelNo
,BlnktSOKey
,CCCustCode
,ChngOrdDate
,ChngOrdNo
,ChngReason
,ChngUserID
,CloseDate
,CntctKey
,CompanyID
,ConfirmNo
,CreateDate
,CreateType
,CreateUserID
,CreditApprovedAmt
,CreditAuthUserID
,CrHold
,CRMOpportunityID
,CurrExchRate
,CurrExchSchdKey
,CurrID
,CustClassKey
,CustKey
,CustPONo
,CustQuoteKey
,DfltAcctRefKey
,DfltCommPlanKey
,DfltCreatePO
,DfltDeliveryMeth
,DfltFOBKey
,DfltPromDate
,DfltPurchVAddrKey
,DfltRequestDate
,DfltShipDate
,DfltShipMethKey
,DfltShipPriority
,DfltShipToAddrKey
,DfltShipToCAddrKey
,DfltShipZoneKey
,DfltVendKey
,DfltWhseKey
,DutyAmount
,Expiration
,FixedCurrExchRate
,FreightAmt
,FreightMethod
,Hold
,HoldReason
,ImportLogKey
,NationalTaxAmount
,NextLineNo
,OpenAmt
,OpenAmtHC
,PmtTermsKey
,PrimarySperKey
,QuoteFormKey
,RecurSOKey
,RequireSOAck
,SalesAmt
,SalesAmtHC
,SalesSourceKey
,SOAckFormKey
,Status
,STaxAmt
,STaxCalc
,STaxTranKey
,TradeDiscAmt
,TranAmt
,TranAmtHC
,TranCmnt
,TranDate
,TranID
,TranNo
,TranNoChngOrd
,TranNoRel
,TranNoRelChngOrd
,TranType
,UpdateCounter
,UpdateDate
,UpdateUserID
,UserFld1
,UserFld2
,UserFld3
,UserFld4
,VATInvoiceNumber
,VATNumber
,VATTaxAmount
,VATTaxRate)
select @SOKey
,null
,max(dfltBillToAddrKey)
,max(dfltBillToAddrKey)
,0
,null
,null
,null
,0
,null
,null
,null
,max(b.PrimaryCntctKey)
,@CompanyID
,null
,getdate()
,1
,'admin'
,0
,null
,0
,null
,1
,null
,'USD'
,max(CustClassKey)
,max(CustKey)
,max(a.CustPONo)
,null
,null
,null
,0
,1
,null
,@trandate
,null
,@trandate
,@trandate
,null
,3
,max(DfltShipToAddrKey)
,max(DfltShipToAddrKey)
,null
,null
,@WhseKey
,0
,null
,0
,0
,2
,0
,null
,null
,null
,2
,@tranamt
,@tranamt
,@dfltpmttermskey
,16
,null
,null
,0
,@tranamt
,@tranamt
,null
,46
,1
,0
,0
,null
,isnull(sum(a.Discount),0)
,@TranAmt
,@TranAmt
,null
,@TranDate
,'SO-' + @SONum
,@SONum
,@SONum
,@SONum
,@SONum
,801
,0
,getdate()
,'admin'
,null
,null
,null
,null
,null
,null
,null
,null
from StgSOLine_Tylie a
inner join tarcustomer b on a.CustID=b.custid
where WorkOrderNo=@socount and b.CompanyID=@companyid
group by WorkOrderNo 

insert into tsosalesorder_ext (SOKey
,Brand
,JobNumber
,OrderedBy
,BilledTo)
select @SOKey
,null  ---max(Brand)
,max(CustJobNo)
,null  ---max(OrderedBy)
,null  ---max(BilledTo)
from StgSOLine_Tylie
where WorkOrderNo=@socount
group by WorkOrderNo


Set @solinecount=(select min(rowkey) from StgSOLine_Tylie where WorkOrderNo=@socount)
Set @solinemax=(select max(rowkey) from StgSOLine_Tylie where WorkOrderNo=@socount)
set @solineno=1

while @solinecount<=@solinemax
begin

exec spGetNextSurrogateKey
	'tsosoline',@solineKey OUTPUT

exec spGetNextSurrogateKey
	'tsosolinedist',@solinedistKey OUTPUT

	Print '283'
	print @solinecount
	print @solinemax

insert into tsosoline(SOLineKey
,AlternateTaxID
,CloseDate
,CmntOnly
,CommClassKey
,CommPlanKey
,CustomBTOKit
,CustQuoteLineKey
,Description
,EstimateKey
,ExtCmnt
,InclOnPackList
,InclOnPickList
,ItemAliasKey
,ItemKey
,OrigItemKey
,POLineKey
,ReqCert
,SalesPromotionID
,SalesPromotionKey
,SOKey
,SOLineNo
,Status
,STaxClassKey
,SystemPriceDetermination
,TaxTypeApplied
,UnitMeasKey
,UnitPrice
,UnitPriceFromSalesPromo
,UnitPriceFromSchedule
,UnitPriceOvrd
,UpdateCounter
,UserFld1
,UserFld2)
select @SOLineKey
,null
,null
,0
,null
,null
,0
,null
,c.ShortDesc
,null
,null
,1
,1
,null
,b.ItemKey
,b.ItemKey
,null
,0
,null
,null
,@SOKey
,@solineno
,1
,6
,0
,null
,@UnitMeasKey
,50 ---Unit price
,0
,0
,1
,0
,null
,null
from StgSOLine_Tylie a
left join timitem b on a.ItemID=b.ItemID
left join timItemDescription c on b.ItemKey=c.ItemKey
where rowkey=@solinecount and CompanyID=@companyid

insert into tsoSOWorkOrdLine_SWK(SOLineKey
,SOKey
,WorkOrder
,NoOfSpots
,Description
,Destinations)
select @SOLineKey
,@SOKey
,a.WorkOrderNo
,a.Spots
,null
,a.QtyOrd
from StgSOLine_Tylie a
where rowkey=@solinecount

Print '372'

insert into tsoSOLineDist (SOLineDistKey
,AcctRefKey
,AmtInvcd
,BlnktSOLineDistKey
,CreatePO
,DeliveryMeth
,ExtAmt
,FOBKey
,FreightAmt
,GLAcctKey
,Hold
,HoldReason
,OrigOrdered
,OrigPromiseDate
,OrigWhseKey
,PromiseDate
,PurchVendAddrKey
,QtyInvcd
,QtyOnBO
,QtyOpenToShip
,QtyOrd
,QtyRtrnCredit
,QtyRtrnReplacement
,QtyShip
,RequestDate
,ShipDate
,ShipMethKey
,ShipPriority
,ShipToAddrKey
,ShipToCustAddrKey
,ShipToCustAddrUpdateCntr
,ShipZoneKey
,SOLineKey
,Status
,STaxTranKey
,TradeDiscAmt
,TradeDiscPct
,UpdateCounter
,VendKey
,WhseKey)
select @SOLineDistKey
,null
,0
,null
,0
,1
,50   ----Ext amt
,null
,0
,b.DfltSalesAcctKey
,0
,null
,a.QtyOrd*a.Spots
,@trandate
,@whsekey
,@trandate
,null
,0
,0
,a.QtyOrd*a.Spots
,a.QtyOrd*a.Spots
,0
,0
,0
,@trandate
,@trandate
,null
,3
,b.DfltShipToAddrKey
,b.DfltShipToAddrKey
,0
,null
,@SOLineKey
,1
,null
,isnull(a.Discount,0)
,0
,0
,null
,@WhseKey
from StgSOLine_Tylie a
inner join tarcustomer b on a.CustID=b.custid
where rowkey=@solinecount and b.CompanyID=@companyid




set @solinecount=(select min(rowkey) from StgSOLine_Tylie where isnull(status,0)=0 and rowkey>@solinecount)

set @solineno=@solineno+1

end   ----end of SO Line Loop

Print '464'
update tsoSalesOrder
set NextLineNo=@solineno+1
where sokey=@sokey

----update tranamount in sales order tables before going to next order

update StgSOLine_Tylie
set status=1
where WorkOrderNo=@socount

---add taxes
print 'calc taxes'
print @sokey

exec SPCalcTax_Tylie @sokey

print '484'

---Process orders
exec SPSOPickandShip_Tylie @sokey, @whsekey


Set @socount=(select min(WorkOrderNo) from StgSOLine_Tylie where isnull(status,0)=0 and WorkOrderNo>@socount)

end    ----end of SO loop

print '495'
---Create pending invoices
exec SPARCreateInvoices_Tylie

print '499'

---Load Purchase orders

Set @pocount=(select min(VendPO) from StgPOLine_Tylie where isnull(status,0)=0)
Set @pomax=(select max(VendPO) from StgPOLine_Tylie where isnull(status,0)=0)

while @pocount<=@pomax
begin

exec spGetNextSurrogateKey
	'tpoPurchOrderLog',@poKey OUTPUT


Print '487'
print @pocount
set @POTranAmt=0

set @PONum=right('00000000'+@pocount,10)

insert into tpoPurchOrderLog (POKey
,CompanyID
,TranAmtHC
,TranDate
,TranNo
,TranNoRel
,TranStatus
,TranType)
select @POKey
,@CompanyID
,@POTranAmt
,max(TranDate)
,@POnum
,@POnum
,2
,1101
from StgPOLine_Tylie a
where VendPO=@pocount 
group by a.vendpo

Print '506'

insert into tpopurchorder (POKey
,AmtInvcd
,ApprovalDate
,ApprovalStatus
,BlnktPOKey
,BlnktRelNo
,BuyerKey
,ChngOrdDate
,ChngOrdNo
,ChngReason
,ChngUserID
,CloseDate
,ClosedForInvc
,ClosedForRcvg
,CntctKey
,CompanyID
,CreateDate
,CreateType
,CreateUserID
,CreditHold
,CurrExchRate
,CurrExchSchdKey
,CurrID
,DfltAcctRefKey
,DfltDropShip
,DfltExclLastCost
,DfltExclLeadTime
,DfltExclLTReasKey
,DfltExpedite
,DfltExpediteRsnKey
,DfltPurchDeptKey
,DfltRequestDate
,DfltShipMethKey
,DfltShipToAddrKey
,DfltShipToCAddrKey
,DfltShipToCustKey
,DfltShipToWhseKey
,DfltShipZoneKey
,DfltTargetCompID
,FixedCurrExchRate
,FOBKey
,FreightAllocMeth
,FreightAmt
,Hold
,HoldReason
,ImportLogKey
,IssueDate
,MatchToleranceKey
,NextLineNo
,OpenAmt
,OpenAmtHC
,OriginationDate
,PmtTermsKey
,POFormKey
,PurchAddrKey
,PurchAmt
,PurchAmtHC
,PurchVendAddrKey
,RecurPOKey
,RemitToAddrKey
,RemitToVendAddrKey
,RequirePOIssue
,Status
,STaxAmt
,STaxTranKey
,TranAmt
,TranAmtHC
,TranCmnt
,TranDate
,TranID
,TranNo
,TranNoChngOrd
,TranNoRel
,TranNoRelChngOrd
,TranType
,UpdateCounter
,UpdateDate
,UpdateUserID
,UserFld1
,UserFld2
,UserFld3
,UserFld4
,V1099Box
,V1099BoxText
,V1099Form
,VendClassKey
,VendKey
,VendQuoteKey)
select @POKey
,0
,null
,0
,null
,0
,null
,null
,0
,null
,null
,null
,0
,0
,max(b.PrimaryCntctKey)
,@CompanyID
,getdate()
,1
,'admin'
,0
,1
,null
,'USD'
,null
,0
,0
,0
,null
,0
,null
,null
,max(trandate)
,null
,max(b.DfltPurchAddrKey)
,null
,null
,@whsekey
,null
,@companyid
,0
,null
,0
,0
,0
,null
,null
,null
,null
,2
,@POTranAmt
,@POTranAmt
,max(trandate)
,max(PmtTermsKey)
,max(POFormKey)
,max(b.DfltPurchAddrKey)
,@POTranAmt
,@POTranAmt
,max(b.DfltPurchAddrKey)
,null
,max(b.DfltRemitToAddrKey)
,max(b.DfltRemitToAddrKey)
,max(RequirePOIssue)
,1
,0
,null
,@poTranAmt
,@poTranAmt
,null
,max(TranDate)
,'PO-' + @POnum
,@POnum
,@POnum
,@POnum
,@POnum
,1101
,0
,getdate()
,'admin'
,null
,null
,null
,null
,max(V1099Box)
,null
,max(V1099Form)
,max(VendClassKey)
,max(VendKey)
,null
from StgPOLine_Tylie a
inner join tapVendor b on a.VendID=b.VendID
where VendPO=@pocount and b.CompanyID=@companyid
group by a.vendpo

set @polinemax=(select max(rowkey) from StgPOLine_Tylie where VendPO=@pocount)
set @polinecount=(select min(rowkey) from StgPOLine_Tylie where VendPO=@pocount)
set @polineno=1

while @polinecount<=@polinemax
begin

Print '695'

exec spGetNextSurrogateKey
	'tpopoline',@polineKey OUTPUT

exec spGetNextSurrogateKey
	'tpopolinedist',@polinedistKey OUTPUT

insert into tpopoline (POLineKey
,CloseDate
,ClosedForInvc
,ClosedForRcvg
,CmntOnly
,Description
,ExclLastCost
,ExtAmt
,ExtCmnt
,ItemAliasKey
,ItemKey
,MatchToleranceKey
,POKey
,POLineNo
,RcptReq
,Status
,STaxClassKey
,TargetCompanyID
,UnitCost
,UnitCostExact
,UnitMeasKey
,UpdateCounter
,UserFld1
,UserFld2
,VendQuoteLineKey)
select @POLineKey
,null
,0
,0
,0
,Description
,0
,50   ----extamt
,null
,null
,b.ItemKey
,MatchToleranceKey
,@POKey
,@polineno   ---POLineNo
,RcptReq
,1
,null
,@companyID
,20   -----UnitCost
,20    ----UnitCostExact
,@UnitMeasKey
,0
,a.Workorderno
,null
,null
from StgPOLine_Tylie a
left join timitem b on a.ItemID=b.ItemID
left join timItemDescription c on b.ItemKey=c.ItemKey
where a.rowkey=@polinecount and CompanyID=@companyid


insert into tpopolinedist (POLineDistKey
,AcctRefKey
,AmtInvcd
,BlnktPOLineDistKey
,ClosedForInvc
,ClosedForRcvg
,DropShip
,ExclLeadTime
,ExclLTReasCodeKey
,Expedite
,ExpediteReasonKey
,ExtAmt
,FASAssetNumber
,FASAssetTemplate
,FOBKey
,FreightAmt
,GLAcctKey
,OrigOrdered
,OrigPromiseDate
,POLineKey
,PromiseDate
,PurchDeptKey
,QtyInvcd
,QtyOnBO
,QtyOpenToRcv
,QtyOrd
,QtyRcvd
,QtyRtrnCredit
,QtyRtrnReplacement
,RequestDate
,ShipMethKey
,ShipToAddrKey
,ShipToCustAddrKey
,ShipToCustKey
,ShipToWhseKey
,ShipZoneKey
,Status
,STaxTranKey
,UpdateCounter)
select @POLineDistKey
,null
,0
,null
,0
,0
,0
,0
,null
,0
,null
,20    ----ExtAmt
,null
,''
,null
,0
,PurchAcctKey
,10   ----OrigOrdered
,trandate  ----OrigPromiseDate
,@POLineKey
,trandate
,null
,0
,0
,10 ----QtyOpenToRcv
,10   ----QtyOrd
,0
,0
,0
,trandate
,null
,null
,null
,null
,@WhseKey
,null
,1
,null
,0
from StgPOLine_Tylie a
left join timitem b on a.ItemID=b.ItemID
left join timItemDescription c on b.ItemKey=c.ItemKey
left join timInventory d on b.itemkey=d.ItemKey
where a.rowkey=@polinecount and CompanyID=@companyid and d.WhseKey=@whsekey

Print '837'

set @polinecount=(select min(rowkey) from StgPOLine_Tylie where isnull(status,0)=0 and rowkey>@polinecount)

set @polineno=@polineno+1

end   ----end of PO Line Loop

----update tranamount in PO tables before going to next order

set @POTranAmt =(select sum(extamt) from tpopoline where POKey=@pokey)

update tpoPurchOrderLog
set tranamthc=@POTranAmt
where pokey=@pokey

update tpoPurchOrder
set tranamthc=@POTranAmt,TranAmt=@POTranAmt
where pokey=@pokey

update StgPOLine_Tylie
set status=1
where VendPO=@pocount

update tpoPurchOrder
set NextLineNo=@polineno+1
where pokey=@pokey

Set @pocount=(select min(VendPO) from StgPOLine_Tylie where isnull(status,0)=0 and VendPO>@pocount)

end    ----end of PO loop
Print '853'





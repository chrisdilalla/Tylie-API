using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using TylieSageApi.Common.Exceptions;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface ISalesOrderRepository
    {
        void AddSalesOrders(IList<SalesOrder> data);
        int MigrateSalesOrdersToRealTables(string guid);
    }

    public class SalesOrderRepository : BaseRepository, ISalesOrderRepository
    {
        public void AddSalesOrders(IList<SalesOrder> data)
        {
            string sql = @"insert into stgSOLine_Tylie (
                [RowKey],
	            [AcctRefCode],
	            [Action],
	            [AmtInvcd],
	            [CloseDate],
	            [CmntOnly],
	            [CommClassID],
	            [CommPlanID],
	            [DeliveryMeth],
	            [Description],
	            [ExtAmt],
	            [ExtCmnt],
	            [FOBID],
	            [FreightAmt],
	            [GLAcctNo],
	            [Hold],
	            [HoldReason],
	            [ItemAliasID],
	            [ItemID],
	            [KitComponent],
	            [MAS90LineIndex],
	            [OrigOrdered],
	            [OrigPromiseDate],
	            [PONumber],
	            [PromiseDate],
	            [QtyInvcd],
	            [QtyOnBO],
	            [QtyOrd],
	            [QtyRtrnCredit],
	            [QtyRtrnReplacement],
	            [QtyShip],
	            [ReqCert],
	            [RequestDate],
	            [ShipDate],
	            [ShipMethID],
	            [ShipPriority],
	            [ShipToAddrLine1],
	            [ShipToAddrLine2],
	            [ShipToAddrLine3],
	            [ShipToAddrLine4],
	            [ShipToAddrLine5],
	            [ShipToAddrName],
	            [ShipToCity],
	            [ShipToCountryID],
	            [ShipToPostalCode],
	            [ShipToStateID],
	            [SOLineNo],
	            [Status],
	            [STaxClassID],
	            [TradeDiscAmt],
	            [TradeDiscPct],
	            [TranNo],
	            [UnitMeasID],
	            [UnitPrice],
	            [UserFld1],
	            [UserFld2],
	            [VendorID],
	            [WarehouseID],
	            [WillCall],
	            [ProcessStatus],
	            [SessionKey])
             values (
                @RowKey,
	            @AcctRefCode,
	            @Action,
	            @AmtInvcd,
	            @CloseDate,
	            @CmntOnly,
	            @CommClassID,
	            @CommPlanID,
	            @DeliveryMeth,
	            @Description,
	            @ExtAmt,
	            @ExtCmnt,
	            @FOBID,
	            @FreightAmt,
	            @GLAcctNo,
	            @Hold,
	            @HoldReason,
	            @ItemAliasID,
	            @ItemID,
	            @KitComponent,
	            @MAS90LineIndex,
	            @OrigOrdered,
	            @OrigPromiseDate,
	            @PONumber,
	            @PromiseDate,
	            @QtyInvcd,
	            @QtyOnBO,
	            @QtyOrd,
	            @QtyRtrnCredit,
	            @QtyRtrnReplacement,
	            @QtyShip,
	            @ReqCert,
	            @RequestDate,
	            @ShipDate,
	            @ShipMethID,
	            @ShipPriority,
	            @ShipToAddrLine1,
	            @ShipToAddrLine2,
	            @ShipToAddrLine3,
	            @ShipToAddrLine4,
	            @ShipToAddrLine5,
	            @ShipToAddrName,
	            @ShipToCity,
	            @ShipToCountryID,
	            @ShipToPostalCode,
	            @ShipToStateID,
	            @SOLineNo,
	            @Status,
	            @STaxClassID,
	            @TradeDiscAmt,
	            @TradeDiscPct,
	            @TranNo,
	            @UnitMeasID,
	            @UnitPrice,
	            @UserFld1,
	            @UserFld2,
	            @VendorID,
	            @WarehouseID,
	            @WillCall,
	            @ProcessStatus,
	            @SessionKey
                )";
            for (int itemNum = 0; itemNum < data.Count; itemNum++)
            {
                Execute(sql, data[itemNum]);
            }
        }

        public int MigrateSalesOrdersToRealTables(string guid)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            string retValName = "_oRetVal";
            string storedProcName = "spSOapiSalesOrdIns_Tylie";
            dynamicParameters.Add(retValName, dbType: DbType.Int32, direction: ParameterDirection.Output);
            Query<int>(storedProcName, dynamicParameters, null, true, null, CommandType.StoredProcedure).SingleOrDefault();
            int result = dynamicParameters.Get<int>(retValName);
            if (result != 1)
                throw new StoredProcedureException(storedProcName, result);
            return result;
        }
    }
}

using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;
using TylieSageApi.Common.Exceptions;
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
                [CustID],
                [ShiptoCustID],
                [Trandate],
                [URL],
                [CustPONo],
                [CustJobNo],
                [PlatformID],
                [ItemID],
                [QtyOrd],
                [WorkOrderNo],
                [Spots],
                [Comment],
                [Status])
             values (
                @CustID,
                @ShiptoCustID,
                @Trandate,
                @URL,
                @CustPONo,
                @CustJobNo,
                @PlatformID,
                @ItemID,
                @QtyOrd,
                @WorkOrderNo,
                @Spots,
                @Comment,
                @Status
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

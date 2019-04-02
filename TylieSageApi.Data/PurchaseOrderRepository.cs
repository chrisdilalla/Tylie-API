using System.Collections.Generic;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface IPurchaseOrderRepository
    {
        void AddPurchaseOrders(IList<PurchaseOrder> data);
    }

    public class PurchaseOrderRepository : BaseRepository, IPurchaseOrderRepository
    {
        public void AddPurchaseOrders(IList<PurchaseOrder> data)
        {
            string sql = @"insert into StgPOLine_Tylie (
                [VendID],
                [VendPO],
                [TranDate],
                [ItemID],
                [Description],
                [QtyOrd],
                [Comment],
                [Workorderno],
                [Status],
                [Spots],
                [Destinations])
             values (
                @VendID,
                @VendPO,
                @TranDate,
                @ItemID,
                @Description,
                @QtyOrd,
                @Comment,
                @Workorderno,
                @Status,
                @Spots,
                @Destinations
                )";
            for (int itemNum = 0; itemNum < data.Count; itemNum++)
            {
                Execute(sql, data[itemNum]);
            }
        }
    }
}

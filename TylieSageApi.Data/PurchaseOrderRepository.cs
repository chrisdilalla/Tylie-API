﻿using System.Collections.Generic;
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
                [Destination])
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
                @Destination
                )";
            for (int itemNum = 0; itemNum < data.Count; itemNum++)
            {
                Execute(sql, data[itemNum]);
            }
        }
    }
}

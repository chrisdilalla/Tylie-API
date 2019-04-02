using System;

namespace TylieSageApi.Data.Entities.Entities
{
    public class PurchaseOrder
    {
        public string VendID { get; set; }
        public string VendPO { get; set; }
        public DateTime? TranDate { get; set; }
        public string ItemID { get; set; }
        public string Description { get; set; }
        public decimal? QtyOrd { get; set; }
        public string Comment { get; set; }
        public string Workorderno { get; set; }
        public int? Status { get; set; }
        public int? Spots { get; set; }
        public int? Destinations { get; set; }
    }
}

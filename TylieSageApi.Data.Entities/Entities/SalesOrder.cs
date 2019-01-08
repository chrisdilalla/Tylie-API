namespace TylieSageApi.Data.Entities.Entities
{
    public class SalesOrder
    {
        public string RowKey { get; set; }
        public string CustID { get; set; }
        public string ShiptoCustID { get; set; }
        public string Trandate { get; set; }
        public string URL { get; set; }
        public string CustPONo { get; set; }
        public string CustJobNo { get; set; }
        public string PlatformID { get; set; }
        public string ItemID { get; set; }
        public decimal? QtyOrd { get; set; }
        public string WorkOrderNo { get; set; }
        public decimal? Spots { get; set; }
        public string Comment { get; set; }
        public int? Status { get; set; }
    }
}

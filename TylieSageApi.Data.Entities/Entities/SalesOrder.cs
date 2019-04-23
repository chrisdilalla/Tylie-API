namespace TylieSageApi.Data.Entities.Entities
{
    public class SalesOrder
    {
        public string RowKey { get; set; }
        public string SoNumber { get; set; }
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
        public string CostCenter { get; set; }
        public string AdditionalInfo { get; set; }
        public bool? ProBono { get; set; }
        public int? Destination { get; set; }
        public int? Length { get; set; }
        public decimal? AddlOrDiscount { get; set; }
    }
}

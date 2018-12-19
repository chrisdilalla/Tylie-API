namespace TylieSageApi.Data.Entities.Entities
{
    public class Customer
    {
        public string CompanyID { get; set; }
        public string Key { get; set; }
        public string CustID { get; set; }
        public string CustClassID { get; set; }
        public string CustClassName { get; set; }
        public string AddrLine1 { get; set; }
        public string AddrLine2 { get; set; }
        public string City { get; set; }
        public string StateID { get; set; }
        public string CountryID { get; set; }
        public string PostalCode { get; set; }
        public string ContactName { get; set; }
        public string ContactTitle { get; set; }
        public string ContactFax { get; set; }
        public string ContactPhone { get; set; }
        public string ContactEmail { get; set; }
        public bool PrintAck { get; set; }
        public bool RequireAck { get; set; }

        public string Status { get; set; }
        public int BrandKey { get; set; }
        public string BrandID { get; set; }
        public string Brand { get; set; }
        public string BrandStatus { get; set; }
        public int ImportStatus { get; set; }
    }
}

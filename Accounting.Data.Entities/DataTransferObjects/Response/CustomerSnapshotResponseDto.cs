using System.Collections.Generic;
using Accounting.Data.DataTransferObjects.Response.Abstract;

namespace Accounting.Data.DataTransferObjects.Response
{
    public class CustomerSnapshotResponseDto : BaseResponseDto
    {
        public IList<CustomerSnapshotItem> Data { get; set; }

        public class CustomerSnapshotItem
        {
            public string CompanyID { get; set; }
            public string Key { get; set; }
            public string CustomerID { get; set; }
            public string CustClassID { get; set; }
            public string CustClassName { get; set; }
            public string AddrLine1 { get; set; }
            public string AddrLine2 { get; set; }
            public string City { get; set; }
            public string StateID { get; set; }
            public string CountryID { get; set; }
            public string PostalCode { get; set; }
            public string CntctName { get; set; }
            public string CntctTitle { get; set; }
            public string CntctFax { get; set; }
            public string CntctPhone { get; set; }
            public string CntctEmail { get; set; }
            public bool PrintAck { get; set; }
            public bool RequireAck { get; set; }
            public IList<CustomerSnapshotItemBrand> Brands { get; set; }

            public class CustomerSnapshotItemBrand
            {
                public string Brand { get; set; }
            }
        }
    }
}

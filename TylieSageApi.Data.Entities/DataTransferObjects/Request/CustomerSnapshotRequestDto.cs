using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using TylieSageApi.Data.Entities.DataTransferObjects.Abstract;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Request
{
    public class CustomerSnapshotRequestDto : BaseDto
    {
        public IList<CustomerSnapshotItem> Data { get; set; }

        public class CustomerSnapshotItem
        {
            [MaxLength(3)]
            [Required]
            public string CompanyID { get; set; }

            [MaxLength(10)]
            public string Key { get; set; }
            [Required]
            [MaxLength(12)]
            public string CustomerID { get; set; }
            [Required]
            [MaxLength(12)]
            public string CustClassID { get; set; }
            [Required]
            [MaxLength(40)]
            public string CustClassName { get; set; }
            [Required]
            [MaxLength(40)]
            public string AddrLine1 { get; set; }
            [MaxLength(40)]
            public string AddrLine2 { get; set; }
            [Required]
            [MaxLength(20)]
            public string City { get; set; }
            [Required]
            [MaxLength(3)]
            public string StateID { get; set; }
            [Required]
            [MaxLength(3)]
            public string CountryID { get; set; }
            [Required]
            [MaxLength(9)]
            public string PostalCode { get; set; }
            [Required]
            [MaxLength(40)]
            public string CntctName { get; set; }
            [MaxLength(40)]
            public string CntctTitle { get; set; }
            [MaxLength(21)]
            public string CntctFax { get; set; }
            [MaxLength(21)]
            public string CntctPhone { get; set; }
            [MaxLength(256)]
            public string CntctEmail { get; set; }
            public bool PrintAck { get; set; }
            public bool RequireAck { get; set; }
            public IList<CustomerSnapshotItemBrand> Brands { get; set; }

            public class CustomerSnapshotItemBrand
            {
                [MaxLength(15)]
                public string Brand { get; set; }
            }
        }
    }
}

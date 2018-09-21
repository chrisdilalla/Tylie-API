using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Accounting.Data.DataTransferObjects.Abstract;

namespace Accounting.Data.DataTransferObjects.Request
{
    public class SalesOrderRequestDto : BaseDto
    {
        [Required]
        [MaxLength(12)]
        public string CustID { get; set; }

        [Required]
        [MaxLength(15)]
        public string ShipToCustID { get; set; }

        [Required]
        [MaxLength(10)]
        public string TranDate { get; set; }
        [MaxLength(15)]
        public string CustPONo { get; set; }
        [MaxLength(15)]
        public string CustJobNo { get; set; }
        [MaxLength(2083)]
        public string CallbackUrl { get; set; }
        public IList<SalesOrderItem> Lines { get; set; }

        public class SalesOrderItem
        {
            [MaxLength(50)]
            public string PlatformID { get; set; }
            [Required]
            [MaxLength(30)]
            public string ItemID { get; set; }
            [Required]
            public decimal QtyOrd { get; set; }
            [Required]
            [MaxLength(15)]
            public string WorkOrderNo { get; set; }
            public decimal Spots { get; set; }
            [MaxLength(255)]
            public string Comment { get; set; }
        }
    }
}

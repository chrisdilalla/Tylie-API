using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using TylieSageApi.Data.Entities.DataTransferObjects.Abstract;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Request
{
    public class PurchaseOrderRequestDto : BaseDto
    {
        [Required]
        [MaxLength(12)]
        public string VendID { get; set; }
        [Required]
        [MaxLength(15)]
        public string TranNo { get; set; }
        [Required]
        [MaxLength(10)]
        public string TranDate { get; set; }
        [MaxLength(2083)]
        public string CallbackUrl { get; set; }
        public IList<PurchaseOrderItem> Lines { get; set; }

        public class PurchaseOrderItem
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
            [MaxLength(40)]
            public string Description { get; set; }
            [MaxLength(255)]
            public string Comment { get; set; }
        }
    }
}

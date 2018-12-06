using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using TylieSageApi.Data.Entities.DataTransferObjects.Request.SalesOrder;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Request
{
    public class PurchaseOrderItemInSalesOrder
    {
        [Required]
        [MaxLength(12)]
        public string VendID { get; set; }
        [Required]
        [MaxLength(15)]
        public string VendPO { get; set; }
        [Required]
        [MaxLength(10)]
        public string TranDate { get; set; }
        public IList<PurchaseOrderItem> Lines { get; set; }        
    }
}

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Request.SalesOrder
{
    public class PurchaseOrderItem
    {
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
        public int? Spots { get; set; }
        public int? Destinations { get; set; }
    }
}

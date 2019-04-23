using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Request.SalesOrder
{
    public class SalesOrderItem
    {
        [MaxLength(50)]
        public string PlatformID { get; set; }
        [Required]
        [MaxLength(30)]
        public string ItemID { get; set; }
        [MaxLength(15)]
        public string WorkOrderNo { get; set; }
        [Required]
        public decimal Spots { get; set; }
        public int Destination { get; set; }
        [Required]
        public decimal QtyOrd { get; set; }
        public int? Length { get; set; }
        public decimal? AddlOrDiscount { get; set; }
        [MaxLength(255)]
        public string Comment { get; set; }
    }
}

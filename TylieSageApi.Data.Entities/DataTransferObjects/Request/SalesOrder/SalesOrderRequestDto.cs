﻿using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using TylieSageApi.Data.Entities.DataTransferObjects.Abstract;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Request.SalesOrder
{
    public class SalesOrderRequestDto : BaseDto
    {
        [Required]
        [MaxLength(10)]
        public string SalesOrder { get; set; }

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

        public string Brand { get; set; }
        public string Product { get; set; }
        public string OrderedBy { get; set; }
        public string BilledTo { get; set; }
        public string CostCenter { get; set; }
        public string AdditionalInfo { get; set; }
        public bool? ProBono { get; set; }

        public IList<SalesOrderItem> Lines { get; set; }
        public IList<PurchaseOrderItemInSalesOrder> PurchaseOrders { get; set; }
    }
}

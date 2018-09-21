using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Accounting.Data.DataTransferObjects.Response.Abstract;

namespace Accounting.Data.DataTransferObjects.CallbackRequest
{
    public class PurchaseOrderCallbackRequestDto : BaseResponseDto
    {
        public string CompanyID { get; set; }
        public string PurchaseOrderNo { get; set; }
    }
}

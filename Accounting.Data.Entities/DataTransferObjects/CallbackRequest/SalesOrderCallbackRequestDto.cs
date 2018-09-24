using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Accounting.Data.DataTransferObjects.Response.Abstract;

namespace Accounting.Data.DataTransferObjects.CallbackRequest
{
    public class SalesOrderCallbackRequestDto : BaseResponseDto
    {
        public string CompanyID { get; set; }
        public string SalesOrderNo { get; set; }
        public string InvoiceNo { get; set; }
    }
}

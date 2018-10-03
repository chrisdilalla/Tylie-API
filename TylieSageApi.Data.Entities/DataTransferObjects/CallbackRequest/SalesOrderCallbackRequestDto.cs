using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;

namespace TylieSageApi.Data.Entities.DataTransferObjects.CallbackRequest
{
    public class SalesOrderCallbackRequestDto : BaseResponseDto
    {
        public string CompanyID { get; set; }
        public string SalesOrderNo { get; set; }
        public string InvoiceNo { get; set; }
    }
}

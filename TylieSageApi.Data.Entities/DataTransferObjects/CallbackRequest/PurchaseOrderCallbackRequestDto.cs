using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;

namespace TylieSageApi.Data.Entities.DataTransferObjects.CallbackRequest
{
    public class PurchaseOrderCallbackRequestDto : BaseResponseDto
    {
        public string CompanyID { get; set; }
        public string PurchaseOrderNo { get; set; }
    }
}

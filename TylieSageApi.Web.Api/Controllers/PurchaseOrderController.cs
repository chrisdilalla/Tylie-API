using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using System.Web.Http;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.DomainLogic;
using TylieSageApi.Web.Api.Controllers.Abstract;
using TylieSageApi.Web.Api.Infrastructure;

namespace TylieSageApi.Web.Api.Controllers
{
    public class PurchaseOrderController : BaseController
    {
        private IPurchaseOrderDomainLogic _purchaseOrderDomainLogic;

        public PurchaseOrderController()
        {
            _purchaseOrderDomainLogic = new PurchaseOrderDomainLogic();
        }


        [Route("purchaseorders/{companyID}")]
        [ValidateActionParameters]
        public async Task<IHttpActionResult> Post([MinLength(1)][MaxLength(3)]string companyID,
            [Required][FromBody]PurchaseOrderRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            PurchaseOrderResponseDto res = await _purchaseOrderDomainLogic.AddPurchaseOrder(companyID, inputDto);
            return Ok(res);
        }
    }
}

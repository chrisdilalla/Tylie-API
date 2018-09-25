using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using System.Web.Http;
using Accounting.Controllers.Abstract;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;
using Accounting.Infrastructure;

namespace Accounting.Controllers
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

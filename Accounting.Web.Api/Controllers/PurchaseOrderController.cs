using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using Accounting.Controllers.Abstract;
using Accounting.Data.DataTransferObjects.Request;
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
        public IHttpActionResult Post([MinLength(1)][MaxLength(3)]string companyID,
            [Required][FromBody]PurchaseOrderRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            _purchaseOrderDomainLogic.AddPurchaseOrder(companyID, inputDto);
            return Ok();
        }
    }
}

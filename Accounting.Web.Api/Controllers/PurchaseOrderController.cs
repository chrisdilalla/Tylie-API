using System.Web.Http;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;

namespace Accounting.Controllers
{
    public class PurchaseOrderController : ApiController
    {
        private IPurchaseOrderDomainLogic _purchaseOrderDomainLogic;

        public PurchaseOrderController()
        {
            _purchaseOrderDomainLogic = new PurchaseOrderDomainLogic();
        }


        [Route("purchaseorders/{companyID}")]
        public IHttpActionResult Post(string companyID, [FromBody]PurchaseOrderRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            try
            {
                _purchaseOrderDomainLogic.AddPurchaseOrder(companyID, inputDto);
                return Ok();
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

using System.Web.Http;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;

namespace Accounting.Controllers
{
    [Authorize]
    public class SalesOrderController : ApiController
    {
        private ISalesOrderDomainLogic _salesOrderDomainLogic;

        public SalesOrderController()
        {
            _salesOrderDomainLogic = new SalesOrderDomainLogic();
        }


        [Route("salesorders/{companyID}")]
        public IHttpActionResult Post(string companyID, [FromBody]SalesOrderRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            try
            {
                _salesOrderDomainLogic.AddSalesOrder(companyID, inputDto);
                return Ok();
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

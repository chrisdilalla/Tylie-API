using System.Web.Http;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;

namespace Accounting.Controllers
{
    public class CustomerSnapshotController : ApiController
    {
        private ICustomerSnapshotDomainLogic _customerSnapshotDomainLogic;

        public CustomerSnapshotController()
        {
            _customerSnapshotDomainLogic = new CustomerSnapshotDomainLogic();
        }


        [Route("customers/{companyID}")]
        public IHttpActionResult Post(string companyID, [FromBody]CustomerSnapshotRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            try
            {
                _customerSnapshotDomainLogic.AddCustomer(companyID, inputDto);
                return Ok();
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

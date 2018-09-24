using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using Accounting.Controllers.Abstract;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;
using Accounting.Infrastructure;

namespace Accounting.Controllers
{
    public class CustomerSnapshotController : BaseController
    {
        private ICustomerSnapshotDomainLogic _customerSnapshotDomainLogic;

        public CustomerSnapshotController()
        {
            _customerSnapshotDomainLogic = new CustomerSnapshotDomainLogic();
        }


        [Route("customers/{companyID}")]
        [ValidateActionParameters]
        public IHttpActionResult Post([MinLength(1)][MaxLength(3)]string companyID,
            [Required][FromBody]CustomerSnapshotRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            _customerSnapshotDomainLogic.AddCustomer(companyID, inputDto);
            return Ok();
        }
    }
}

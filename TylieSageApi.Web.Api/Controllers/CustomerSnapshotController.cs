using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.DomainLogic;
using TylieSageApi.Web.Api.Controllers.Abstract;
using TylieSageApi.Web.Api.Infrastructure;

namespace TylieSageApi.Web.Api.Controllers
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

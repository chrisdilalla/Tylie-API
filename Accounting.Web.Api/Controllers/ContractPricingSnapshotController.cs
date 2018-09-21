using System;
using System.Web.Http;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;

namespace Accounting.Controllers
{
    [Authorize]
    public class ContractPricingSnapshotController : ApiController
    {
        private IContractPricingSnapshotDomainLogic _contractPricingSnapshotDomainLogic;

        public ContractPricingSnapshotController()
        {
            _contractPricingSnapshotDomainLogic = new ContractPricingSnapshotDomainLogic();
        }

        [Route("contractpricing/{companyID}/{lastUpdatedDate}")]
        public IHttpActionResult Get([FromUri]SnapshotForCompanyAndDateRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            try
            {
                ContractPricingSnapshotResponseDto responseDto = _contractPricingSnapshotDomainLogic.GetContractPricingSnapshot(inputDto);
                return Ok(responseDto);
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

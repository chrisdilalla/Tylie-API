using System;
using System.ComponentModel.DataAnnotations;
using System.Linq.Expressions;
using System.Web.Http;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;
using Accounting.Infrastructure;
using Accounting.Utils;

namespace Accounting.Controllers
{
    public class ContractPricingSnapshotController : ApiController
    {
        private IContractPricingSnapshotDomainLogic _contractPricingSnapshotDomainLogic;

        public ContractPricingSnapshotController()
        {
            _contractPricingSnapshotDomainLogic = new ContractPricingSnapshotDomainLogic();
        }

        [Route("contractpricing/" + Constants.RouteWithCompanyAndDate)]
        [ValidateActionParameters]
        public IHttpActionResult Get([MinLength(1)][MaxLength(3)]string companyID,
            DateTime lastUpdatedDate)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            try
            {
                ContractPricingSnapshotResponseDto responseDto = _contractPricingSnapshotDomainLogic.GetContractPricingSnapshot(companyID, lastUpdatedDate);
                return Ok(responseDto);
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

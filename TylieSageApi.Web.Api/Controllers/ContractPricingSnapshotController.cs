using System;
using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.DomainLogic;
using TylieSageApi.Web.Api.Controllers.Abstract;
using TylieSageApi.Web.Api.Infrastructure;
using TylieSageApi.Web.Api.Utils;

namespace TylieSageApi.Web.Api.Controllers
{
    public class ContractPricingSnapshotController : BaseController
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
                ThrowModelStateException(ModelState);

            ContractPricingSnapshotResponseDto responseDto = _contractPricingSnapshotDomainLogic.GetContractPricingSnapshot(companyID, lastUpdatedDate);
            return Ok(responseDto);
        }
    }
}

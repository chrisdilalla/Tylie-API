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
    public class VendorsSnapshotController : BaseController
    {
        private IVendorsSnapshotDomainLogic _vendorsSnapshotDomainLogic;

        public VendorsSnapshotController()
        {
            _vendorsSnapshotDomainLogic = new VendorsSnapshotDomainLogic();
        }

        [Route("vendors/" + Constants.RouteWithCompanyAndDate)]
        [ValidateActionParameters]
        public IHttpActionResult Get([MinLength(1)][MaxLength(3)]string companyID,
                                    DateTime lastUpdatedDate)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            VendorsSnapshotResponseDto responseDto = _vendorsSnapshotDomainLogic.GetVendorsSnapshot(companyID, lastUpdatedDate);
            return Ok(responseDto);
        }
    }
}

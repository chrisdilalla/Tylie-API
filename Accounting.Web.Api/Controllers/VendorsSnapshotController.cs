using System;
using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;
using Accounting.Infrastructure;
using Accounting.Utils;

namespace Accounting.Controllers
{
    public class VendorsSnapshotController : ApiController
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
                return BadRequest(ModelState);

            try
            {
                VendorsSnapshotResponseDto responseDto = _vendorsSnapshotDomainLogic.GetVendorsSnapshot(companyID, lastUpdatedDate);
                return Ok(responseDto);
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

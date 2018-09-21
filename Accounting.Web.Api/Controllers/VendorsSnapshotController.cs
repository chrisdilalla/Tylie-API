using System;
using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;

namespace Accounting.Controllers
{
    [Authorize]
    public class VendorsSnapshotController : ApiController
    {
        private IVendorsSnapshotDomainLogic _vendorsSnapshotDomainLogic;

        public VendorsSnapshotController()
        {
            _vendorsSnapshotDomainLogic = new VendorsSnapshotDomainLogic();
        }

        [Route("vendors/{companyID}/{lastUpdatedDate}")]
        public IHttpActionResult Get([FromUri]SnapshotForCompanyAndDateRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            try
            {
                VendorsSnapshotResponseDto responseDto = _vendorsSnapshotDomainLogic.GetVendorsSnapshot(inputDto);
                return Ok(responseDto);
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

using System;
using System.Web.Http;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;

namespace Accounting.Controllers
{
    [Authorize]
    public class ItemsSnapshotController : ApiController
    {
        private IItemsSnapshotDomainLogic _itemsSnapshotDomainLogic;

        public ItemsSnapshotController()
        {
            _itemsSnapshotDomainLogic = new ItemsSnapshotDomainLogic();
        }

        [Route("items/{companyID}/{lastUpdatedDate}")]
        public IHttpActionResult Get([FromUri]SnapshotForCompanyAndDateRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState);
            
            try
            {
                ItemsSnapshotResponseDto responseDto = _itemsSnapshotDomainLogic.GetItemsSnapshot(inputDto);
                return Ok(responseDto);
            }
            catch (AccountingException accountingException)
            {
                return BadRequest(accountingException.Message);
            }
        }
    }
}

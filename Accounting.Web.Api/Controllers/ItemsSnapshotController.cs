using System;
using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using Accounting.Controllers.Abstract;
using Accounting.Data.DataTransferObjects.Response;
using Accounting.DomainLogic;
using Accounting.DomainLogic.Exceptions;
using Accounting.Infrastructure;
using Accounting.Utils;

namespace Accounting.Controllers
{
    public class ItemsSnapshotController : BaseController
    {
        private IItemsSnapshotDomainLogic _itemsSnapshotDomainLogic;

        public ItemsSnapshotController()
        {
            _itemsSnapshotDomainLogic = new ItemsSnapshotDomainLogic();
        }

        [Route("items/" + Constants.RouteWithCompanyAndDate)]
        [ValidateActionParameters]
        public IHttpActionResult Get([MinLength(1)][MaxLength(3)]string companyID,
                                    DateTime lastUpdatedDate)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            ItemsSnapshotResponseDto responseDto = _itemsSnapshotDomainLogic.GetItemsSnapshot(companyID, lastUpdatedDate);
            return Ok(responseDto);
        }
    }
}

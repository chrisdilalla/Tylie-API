using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using TylieSageApi.Data.Entities.DataTransferObjects;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic;
using TylieSageApi.Web.Api.Controllers.Abstract;
using TylieSageApi.Web.Api.Infrastructure;
using TylieSageApi.Web.Api.Utils;

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
        public IHttpActionResult Get([MinLength(1)][MaxLength(3)]string companyID)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            IEnumerable<CustomerSnapshotItem> responseDto = _customerSnapshotDomainLogic.GetCustomers(companyID);
            return Ok(responseDto);
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

﻿using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using System.Web.Http.Description;
using TylieSageApi.Data.Entities.DataTransferObjects.Request.SalesOrder;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.DomainLogic;
using TylieSageApi.Web.Api.Controllers.Abstract;
using TylieSageApi.Web.Api.Infrastructure;

namespace TylieSageApi.Web.Api.Controllers
{
    public class SalesOrderController : BaseController
    {
        private ISalesOrderDomainLogic _salesOrderDomainLogic;

        public SalesOrderController()
        {
            _salesOrderDomainLogic = new SalesOrderDomainLogic();
        }

        [ResponseType(typeof(SalesOrderResponseDto))]
        [Route("salesorders/{companyID}")]
        [ValidateActionParameters]
        public IHttpActionResult Post([MinLength(1)][MaxLength(3)]string companyID,
            [Required][FromBody]SalesOrderRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            SalesOrderResponseDto result = _salesOrderDomainLogic.AddSalesOrder(companyID, inputDto);
            if (result.Errors == null || result.Errors.Count == 0)
                return Ok(result);

            return Content(System.Net.HttpStatusCode.BadRequest, result);
        }
    }
}

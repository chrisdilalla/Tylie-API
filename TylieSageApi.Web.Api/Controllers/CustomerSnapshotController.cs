﻿using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using TylieSageApi.Data.Entities.DataTransferObjects;
using System.Web.Http.Description;
using System.Web.Http.Results;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;
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

        //[ResponseType(typeof(IEnumerable<Customer>))]
        //[Route("customers/{companyID}")]
        //[ValidateActionParameters]
        //public IHttpActionResult Get([MinLength(1)][MaxLength(3)]string companyID)
        //{
        //    if (!ModelState.IsValid)
        //        ThrowModelStateException(ModelState);

        //    IEnumerable<CustomerSnapshotItem> responseDto = _customerSnapshotDomainLogic.GetCustomers(companyID);
        //    return Ok(responseDto);
        //}

        [Route("customers/{companyID}")]
        [ValidateActionParameters]
        public IHttpActionResult Post([MinLength(1)][MaxLength(3)]string companyID,
            [Required][FromBody]CustomerSnapshotRequestDto inputDto)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            BaseResponseDto result = _customerSnapshotDomainLogic.AddCustomer(companyID, inputDto);
            IHttpActionResult webApiResult = ResponseMessage(Request.CreateResponse((HttpStatusCode)result.Status, result));
            return webApiResult;
        }
    }
}

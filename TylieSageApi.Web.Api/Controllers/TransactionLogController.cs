using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Web.Http;
using System.Web.Http.Description;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.DomainLogic;
using TylieSageApi.Web.Api.Controllers.Abstract;
using TylieSageApi.Web.Api.Infrastructure;
using TylieSageApi.Web.Api.Utils;

namespace TylieSageApi.Web.Api.Controllers
{
    public class TransactionLogController : BaseController
    {
        private ITransactionLogDomainLogic _transactionLogDomainLogic;

        public TransactionLogController()
        {
            _transactionLogDomainLogic = new TransactionLogDomainLogic();
        }

        [ResponseType(typeof(GetTransactionLogResponseDto))]
        [Route("transactionLog")]
        [ValidateActionParameters]
        public IHttpActionResult Get(Guid transitId)
        {
            if (!ModelState.IsValid)
                ThrowModelStateException(ModelState);

            GetTransactionLogResponseDto responseDto = _transactionLogDomainLogic.GetLogEntries(transitId);
            return Ok(responseDto);
        }
    }
}

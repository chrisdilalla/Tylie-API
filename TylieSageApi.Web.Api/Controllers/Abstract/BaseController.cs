using System;
using System.Collections.Generic;
using System.Web.Http;
using System.Web.Http.ModelBinding;
using TylieSageApi.DomainLogic.Exceptions;

namespace TylieSageApi.Web.Api.Controllers.Abstract
{
    public abstract class BaseController : ApiController
    {
        protected void ThrowModelStateException(ModelStateDictionary modelState)
        {
            var errors = new List<string>();
            foreach (var state in ModelState)
            {
                foreach (var error in state.Value.Errors)
                {
                    errors.Add(error.ErrorMessage);
                    if (string.IsNullOrEmpty(error.ErrorMessage)) errors.Add(error.Exception?.Message);
                }
            }
            throw new AccountingException("Input data is incorrect", String.Join("\r\n", errors));
        }
    }
}

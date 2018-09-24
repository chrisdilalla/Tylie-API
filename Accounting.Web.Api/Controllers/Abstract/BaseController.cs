using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.ModelBinding;
using Accounting.DomainLogic.Exceptions;

namespace Accounting.Controllers.Abstract
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
                }
            }
            throw new AccountingException("Input data is incorrect", String.Join("\r\n", errors));
        }
    }
}

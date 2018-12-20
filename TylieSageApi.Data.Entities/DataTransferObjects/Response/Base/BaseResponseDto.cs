using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using TylieSageApi.Data.Entities.DataTransferObjects.Abstract;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response.Base
{
    public class BaseResponseDto : BaseDto
    {
        public BaseResponseDto()
        {
            Errors = new List<Error>();
            Status = (int) HttpStatusCode.OK;
        }

        public int Status { get; set; }
        public IList<Error> Errors { get; set; }

        public void AddError(string title, string detail)
        {
            Status = (int)HttpStatusCode.BadRequest;
            Errors.Add(new Error(title, detail));
        }

        public void AddErrorsFromException(string title, Exception exception)
        {
            Status = (int)HttpStatusCode.BadRequest;
            bool isFirst = true;
            while (exception != null)
            {
                string exceptionText = exception.Message;
                Errors.Add(new Error(isFirst ? title : null, exceptionText));
                isFirst = false;
                exception = exception.InnerException;
            }
        }

        public class Error
        {
            public Error(string title, string detail)
            {
                Title = title;
                Detail = detail;

            }

            public string Title { get; set; }
            public string Detail { get; set; }
        }
    }
}

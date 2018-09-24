using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Accounting.Data.DataTransferObjects.Abstract;

namespace Accounting.Data.DataTransferObjects.Response.Abstract
{
    public class BaseResponseDto : BaseDto
    {
        public BaseResponseDto()
        {
            Errors = new List<Error>();
            Status = (int)HttpStatusCode.OK;
        }

        public int Status { get; set; }
        public IList<Error> Errors { get; set; }

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

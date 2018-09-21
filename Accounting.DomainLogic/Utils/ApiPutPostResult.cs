using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web;

namespace Accounting.Utils
{
    public class ApiPutPostResult
    {
        public object Object { get; set; }
        public HttpStatusCode StatusCode { get; set; }
        public bool IsSuccessStatusCode { get; set; }

        public ApiPutPostResult()
        {
            Object = null;
            StatusCode = HttpStatusCode.Unused;
            IsSuccessStatusCode = false;
        }

        public ApiPutPostResult(HttpResponseMessage result)
        {
            IsSuccessStatusCode = result.IsSuccessStatusCode;
            StatusCode = result.StatusCode;
        }


    }
}
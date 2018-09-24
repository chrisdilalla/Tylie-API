using System.Collections;
using System.Collections.Generic;
using System.IO;
using Accounting.Data.DataTransferObjects.Abstract;

namespace Accounting.Data.DataTransferObjects.Response.Abstract
{
    public abstract class BaseResponseDtoWithErrorLinks : BaseResponseDto
    {
        public IList<ErrorLink> Links { get; set; }

        public class ErrorLink
        {
            public string Rel { get; set; }
            public string Href { get; set; }
        }
    }
}

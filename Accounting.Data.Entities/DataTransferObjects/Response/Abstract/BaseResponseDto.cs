using System.Collections;
using System.Collections.Generic;
using System.IO;
using Accounting.Data.DataTransferObjects.Abstract;

namespace Accounting.Data.DataTransferObjects.Response.Abstract
{
    public abstract class BaseResponseDto : BaseDto
    {
        public int Status { get; set; }
        public IList<Error> Errors { get; set; }

        public class Error
        {
            public string Title { get; set; }
            public string Detail { get; set; }
        }
    }
}

using System.Collections.Generic;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response.Base
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

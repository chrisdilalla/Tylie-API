using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response
{
    public class CustomerSnapshotResponseDto : BaseResponseDto
    {
        public IList<CustomerSnapshotItem> Data { get; set; }
    }
}

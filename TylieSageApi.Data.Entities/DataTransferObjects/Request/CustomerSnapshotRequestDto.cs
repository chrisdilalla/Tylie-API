using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using TylieSageApi.Data.Entities.DataTransferObjects.Abstract;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Request
{
    public class CustomerSnapshotRequestDto : BaseDto
    {
        public IList<CustomerSnapshotItem> Data { get; set; }

        
    }
}

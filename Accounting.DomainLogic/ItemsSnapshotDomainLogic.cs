using System;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;

namespace Accounting.DomainLogic
{
    public interface IItemsSnapshotDomainLogic
    {
        ItemsSnapshotResponseDto GetItemsSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class ItemsSnapshotDomainLogic : IItemsSnapshotDomainLogic
    {
        public ItemsSnapshotResponseDto GetItemsSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            ItemsSnapshotResponseDto responseDto = new ItemsSnapshotResponseDto();
            return responseDto;
        }
    }
}

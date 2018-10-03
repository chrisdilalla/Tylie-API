using System;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;

namespace TylieSageApi.DomainLogic
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

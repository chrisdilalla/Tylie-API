using System;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;

namespace TylieSageApi.DomainLogic
{
    public interface IItemsSnapshotDomainLogic
    {
        ItemsSnapshotResponseDto GetItemsSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class ItemsSnapshotDomainLogic : IItemsSnapshotDomainLogic
    {
        private IItemRepository _itemRepository;

        public ItemsSnapshotDomainLogic()
        {
            _itemRepository = new ItemRepository();
        }

        public ItemsSnapshotResponseDto GetItemsSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            ItemsSnapshotResponseDto responseDto = _itemRepository.GetByCompanyIdAndLastUpdate(companyID, lastUpdatedDate);
            return responseDto;
        }
    }
}

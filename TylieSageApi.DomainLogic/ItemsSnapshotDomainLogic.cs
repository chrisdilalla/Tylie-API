using System;
using System.Collections.Generic;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
{
    public interface IItemsSnapshotDomainLogic
    {
        ItemsSnapshotResponseDto GetItemsSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class ItemsSnapshotDomainLogic : IItemsSnapshotDomainLogic
    {
        private IItemRepository _itemRepository;
        private SharedDomainLogic _sharedDomainLogic;

        public ItemsSnapshotDomainLogic()
        {
            _itemRepository = new ItemRepository();
            _sharedDomainLogic = new SharedDomainLogic();
        }

        public ItemsSnapshotResponseDto GetItemsSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            Func<ItemsSnapshotResponseDto> function = () => _itemRepository.GetByCompanyIdAndLastUpdate(companyID, lastUpdatedDate);
            ItemsSnapshotResponseDto result = _sharedDomainLogic.GetDataAndLogTransaction(function, "Items snapshot", EventType.ItemsSnapshotDataRetrievalCompleted);
            return result;
        }
    }
}

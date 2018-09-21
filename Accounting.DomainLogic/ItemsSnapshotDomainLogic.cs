using System;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;

namespace Accounting.DomainLogic
{
    public interface IItemsSnapshotDomainLogic
    {
        ItemsSnapshotResponseDto GetItemsSnapshot(SnapshotForCompanyAndDateRequestDto inputDto);
    }

    public class ItemsSnapshotDomainLogic : IItemsSnapshotDomainLogic
    {
        public ItemsSnapshotResponseDto GetItemsSnapshot(SnapshotForCompanyAndDateRequestDto inputDto)
        {
            ItemsSnapshotResponseDto responseDto = new ItemsSnapshotResponseDto();
            return responseDto;
        }
    }
}

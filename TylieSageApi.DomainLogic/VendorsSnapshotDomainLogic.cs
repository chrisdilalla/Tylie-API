using System;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;

namespace TylieSageApi.DomainLogic
{
    public interface IVendorsSnapshotDomainLogic
    {
        VendorsSnapshotResponseDto GetVendorsSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class VendorsSnapshotDomainLogic : IVendorsSnapshotDomainLogic
    {
        public VendorsSnapshotResponseDto GetVendorsSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            VendorsSnapshotResponseDto responseDto = new VendorsSnapshotResponseDto();
            return responseDto;
        }
    }
}

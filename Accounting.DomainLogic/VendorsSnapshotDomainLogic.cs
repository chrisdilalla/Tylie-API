using System;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;

namespace Accounting.DomainLogic
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

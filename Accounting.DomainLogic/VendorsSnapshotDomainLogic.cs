using System;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;

namespace Accounting.DomainLogic
{
    public interface IVendorsSnapshotDomainLogic
    {
        VendorsSnapshotResponseDto GetVendorsSnapshot(SnapshotForCompanyAndDateRequestDto inputDto);
    }

    public class VendorsSnapshotDomainLogic : IVendorsSnapshotDomainLogic
    {
        public VendorsSnapshotResponseDto GetVendorsSnapshot(SnapshotForCompanyAndDateRequestDto inputDto)
        {
            VendorsSnapshotResponseDto responseDto = new VendorsSnapshotResponseDto();
            return responseDto;
        }
    }
}

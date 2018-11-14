using System;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;

namespace TylieSageApi.DomainLogic
{
    public interface IVendorsSnapshotDomainLogic
    {
        VendorsSnapshotResponseDto GetVendorsSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class VendorsSnapshotDomainLogic : IVendorsSnapshotDomainLogic
    {
        private IVendorRepository _vendorRepository;

        public VendorsSnapshotDomainLogic()
        {
            _vendorRepository = new VendorRepository();
        }

        public VendorsSnapshotResponseDto GetVendorsSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            VendorsSnapshotResponseDto responseDto = _vendorRepository.GetByCompanyIdAndLastUpdate(companyID, lastUpdatedDate);
            return responseDto;
        }
    }
}

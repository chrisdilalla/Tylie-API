using System;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
{
    public interface IVendorsSnapshotDomainLogic
    {
        VendorsSnapshotResponseDto GetVendorsSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class VendorsSnapshotDomainLogic : IVendorsSnapshotDomainLogic
    {
        private IVendorRepository _vendorRepository;
        private SharedDomainLogic _sharedDomainLogic;

        public VendorsSnapshotDomainLogic()
        {
            _vendorRepository = new VendorRepository();
            _sharedDomainLogic = new SharedDomainLogic();
        }

        public VendorsSnapshotResponseDto GetVendorsSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            Func<VendorsSnapshotResponseDto> function = () => _vendorRepository.GetByCompanyIdAndLastUpdate(companyID, lastUpdatedDate); ;
            VendorsSnapshotResponseDto result = _sharedDomainLogic.GetDataAndLogTransaction(function, "Vendors snapshot", EventType.VendorsSnapshotDataRetrievalCompleted);
            return result;
        }
    }
}

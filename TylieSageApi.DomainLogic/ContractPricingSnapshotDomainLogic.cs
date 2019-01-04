using System;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic.Exceptions;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
{
    public interface IContractPricingSnapshotDomainLogic
    {
        ContractPricingSnapshotResponseDto GetContractPricingSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class ContractPricingSnapshotDomainLogic : IContractPricingSnapshotDomainLogic
    {
        private IContractPricingRepository _contractPricingRepository;
        private SharedDomainLogic _sharedDomainLogic;
        
        public ContractPricingSnapshotDomainLogic()
        {
            _contractPricingRepository = new ContractPricingRepository();
            _sharedDomainLogic = new SharedDomainLogic();
        }


        public ContractPricingSnapshotResponseDto GetContractPricingSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            Func<ContractPricingSnapshotResponseDto> function = () => _contractPricingRepository.GetByCompanyIdAndLastUpdate(companyID, lastUpdatedDate);
            ContractPricingSnapshotResponseDto result = _sharedDomainLogic.GetDataAndLogTransaction(function, "Contract pricing snapshot");
            return result;
        }
    }
}

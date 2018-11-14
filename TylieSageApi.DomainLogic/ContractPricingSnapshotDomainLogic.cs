using System;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.DomainLogic.Exceptions;

namespace TylieSageApi.DomainLogic
{
    public interface IContractPricingSnapshotDomainLogic
    {
        ContractPricingSnapshotResponseDto GetContractPricingSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class ContractPricingSnapshotDomainLogic : IContractPricingSnapshotDomainLogic
    {
        private IContractPricingRepository _contractPricingRepository;

        public ContractPricingSnapshotDomainLogic()
        {
            _contractPricingRepository = new ContractPricingRepository();
        }


        public ContractPricingSnapshotResponseDto GetContractPricingSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            ContractPricingSnapshotResponseDto responseDto = _contractPricingRepository.GetByCompanyId(companyID, lastUpdatedDate);
            return responseDto;
        }
    }
}

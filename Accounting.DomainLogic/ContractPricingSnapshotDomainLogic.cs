using System;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;

namespace Accounting.DomainLogic
{
    public interface IContractPricingSnapshotDomainLogic
    {
        ContractPricingSnapshotResponseDto GetContractPricingSnapshot(string companyID, DateTime lastUpdatedDate);
    }

    public class ContractPricingSnapshotDomainLogic : IContractPricingSnapshotDomainLogic
    {
        public ContractPricingSnapshotResponseDto GetContractPricingSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            ContractPricingSnapshotResponseDto responseDto = new ContractPricingSnapshotResponseDto();
            return responseDto;
        }
    }
}

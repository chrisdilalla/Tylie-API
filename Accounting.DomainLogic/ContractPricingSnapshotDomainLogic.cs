using System;
using Accounting.Data.DataTransferObjects.Request;
using Accounting.Data.DataTransferObjects.Response;

namespace Accounting.DomainLogic
{
    public interface IContractPricingSnapshotDomainLogic
    {
        ContractPricingSnapshotResponseDto GetContractPricingSnapshot(SnapshotForCompanyAndDateRequestDto inputDto);
    }

    public class ContractPricingSnapshotDomainLogic : IContractPricingSnapshotDomainLogic
    {
        public ContractPricingSnapshotResponseDto GetContractPricingSnapshot(SnapshotForCompanyAndDateRequestDto inputDto)
        {
            ContractPricingSnapshotResponseDto responseDto = new ContractPricingSnapshotResponseDto();
            return responseDto;
        }
    }
}

using System;
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
        public ContractPricingSnapshotResponseDto GetContractPricingSnapshot(string companyID, DateTime lastUpdatedDate)
        {
            ContractPricingSnapshotResponseDto responseDto = new ContractPricingSnapshotResponseDto();
            if (companyID == "400")
                throw new AccountingException("demo title", "company is wrong (demo code to be removed");
            if (companyID == "500")
            {
                int a = 0;
                int b = 5 / a;
            }
            return responseDto;
        }
    }
}

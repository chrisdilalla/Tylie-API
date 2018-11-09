using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response
{
    public class ContractPricingSnapshotResponseDto : BaseResponseDtoWithErrorLinks
    {
        public ContractPricingSnapshotResponseDto()
        {
            Data = new List<ContractPricingSnapshotItem>();
        }

        public IEnumerable<ContractPricingSnapshotItem> Data { get; set; }

        public class ContractPricingSnapshotItem
        {
            public string CompanyID { get; set; }
            public string Key { get; set; }
            public string CustID { get; set; }
            public string CustName { get; set; }
            public string Brand { get; set; }
            public string ItemID { get; set; }
            public string ShortDesc { get; set; }
            public string EffectiveDate { get; set; }
            public decimal Amount { get; set; }
            public string Status { get; set; }


        }
    }
}

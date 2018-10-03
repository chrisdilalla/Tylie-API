using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response
{
    public class ContractPricingSnapshotResponseDto : BaseResponseDtoWithErrorLinks
    {
        public IList<ContractPricingSnapshotItem> Data { get; set; }

        public class ContractPricingSnapshotItem
        {
            public string CompanyID { get; set; }
            public string Key { get; set; }
            public string CustID { get; set; }
            public string CustName { get; set; }
            public string Brand { get; set; }
            public string ItemID { get; set; }
            public string ShortDesc { get; set; }
            public decimal EffectiveDate { get; set; }
            public decimal Amount { get; set; }

        }
    }
}

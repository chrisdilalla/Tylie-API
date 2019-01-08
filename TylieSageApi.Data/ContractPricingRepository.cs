using System;
using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;

namespace TylieSageApi.Data
{
    public interface IContractPricingRepository
    {
        ContractPricingSnapshotResponseDto GetByCompanyIdAndLastUpdate(string companyId, DateTime lastUpdatedDate);
    }

    public class ContractPricingRepository : BaseRepository, IContractPricingRepository
    {
        public ContractPricingSnapshotResponseDto GetByCompanyIdAndLastUpdate(string companyId, DateTime lastUpdatedDate)
        {
            ContractPricingSnapshotResponseDto resultDto = new ContractPricingSnapshotResponseDto();
            var parameters = new {CompanyId = companyId, LastUpdatedDate  = lastUpdatedDate };
            IEnumerable<ContractPricingSnapshotResponseDto.ContractPricingSnapshotItem> list = Query<ContractPricingSnapshotResponseDto.ContractPricingSnapshotItem>(
                @"select c.Companyid,a.custitempricekey as [Key],Custid,CustName,AddrName as Brand, 
                        ItemID,ShortDesc,EffectiveDate,PriceOrAmtAdj as Amount,
                        case when ExpirationDate<getdate() then 'Inactive' else 'Active' end as Status
                    from timCustitemprice a inner join timpricebreak b on a.pricingkey=b.pricingkey 
                    inner join timitem c on a.itemkey=c.itemkey left join timItemDescription cc on c.itemkey=cc.ItemKey 
                    inner join tarCustomer d on a.CustKey=d.custkey 
                    inner join tciaddress e on a.CustAddrKey=e.AddrKey
                    where c.companyid=@CompanyId and c.UpdateDate>=@LastUpdatedDate",
                parameters);
            resultDto.Data = list;
            return resultDto;
        }
    }
}

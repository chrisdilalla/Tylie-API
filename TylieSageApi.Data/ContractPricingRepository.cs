using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface IContractPricingRepository
    {
        ContractPricingSnapshotResponseDto GetByCompanyId(string companyId);
    }

    public class ContractPricingRepository : BaseRepository, IContractPricingRepository
    {
        public ContractPricingSnapshotResponseDto GetByCompanyId(string companyId)
        {
            ContractPricingSnapshotResponseDto resultDto = new ContractPricingSnapshotResponseDto();
            var parameters = new {CompanyId = companyId};
            IEnumerable<ContractPricingSnapshotResponseDto.ContractPricingSnapshotItem> list = Query<ContractPricingSnapshotResponseDto.ContractPricingSnapshotItem>(
                @"select c.Companyid,a.custitempricekey as [Key],Custid,CustName,AddrName as Brand, 
                        ItemID,ShortDesc,EffectiveDate,PriceOrAmtAdj as Amount,
                        case when ExpirationDate<getdate() then 'Inactive' else 'Active' end as Status
                    from timCustitemprice a inner join timpricebreak b on a.pricingkey=b.pricingkey 
                    inner join timitem c on a.itemkey=c.itemkey left join timItemDescription cc on c.itemkey=cc.ItemKey 
                    inner join tarCustomer d on a.CustKey=d.custkey 
                    inner join tciaddress e on a.CustAddrKey=e.AddrKey",
                parameters);
            resultDto.Data = list;
            return resultDto;
        }
    }
}

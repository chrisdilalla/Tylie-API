using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface IVendorRepository
    {
        VendorsSnapshotResponseDto GetByCompanyIdAndLastUpdate(string companyId, DateTime lastUpdatedDate);
    }

    public class VendorRepository : BaseRepository, IVendorRepository
    {
        public VendorsSnapshotResponseDto GetByCompanyIdAndLastUpdate(string companyId, DateTime lastUpdatedDate)
        {
            var parameters = new {CompanyId = companyId, LastUpdatedDate = lastUpdatedDate };
            VendorsSnapshotResponseDto resultDto = new VendorsSnapshotResponseDto();
            IEnumerable<VendorsSnapshotResponseDto.VendorsSnapshotItem> list = Query<VendorsSnapshotResponseDto.VendorsSnapshotItem>(
                @"select a.Companyid as [CompanyId], a.vendkey as [Key], a.Vendid as [VendorID],a.Vendname as [Vendname],c.itemid as [ItemId],case 
                        when a.status =1 then 'Active' 
                        when a.status=2 then 'Inactive'
                        when a.status=3 then 'Temporary'
                        when a.status=4 then 'Deleted' end as Status 
                        from tapvendor a LEFT JOIN timvenditem b ON a.vendkey = b.vendkey 
                        LEFT JOIN timitem c on b.itemkey = c.itemkey
                    where a.companyid=@CompanyId and a.UpdateDate>=@LastUpdatedDate",
                parameters);
            resultDto.Data = list;
            return resultDto;
        }
    }
}

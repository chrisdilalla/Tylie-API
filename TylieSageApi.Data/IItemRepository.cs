using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface IItemRepository
    {
        ItemsSnapshotResponseDto GetByCompanyId(string companyId, DateTime lastUpdatedDate);
    }

    public class ItemRepository: BaseRepository, IItemRepository
    {
        public ItemsSnapshotResponseDto GetByCompanyId(string companyId, DateTime lastUpdatedDate)
        {
            ItemsSnapshotResponseDto resultDto = new ItemsSnapshotResponseDto();
            var parameters = new { CompanyId = companyId, LastUpdatedDate = lastUpdatedDate };
            IEnumerable<ItemsSnapshotResponseDto.ItemsSnapshotItem> list = Query<ItemsSnapshotResponseDto.ItemsSnapshotItem>(
                @"select a.CompanyID, a.itemkey as [Key],ItemID,ShortDesc,STaxClassID,
                        case when status =1 then 'Active' 
                            when status=2 then 'Inactive' 
                            when status=3 then 'Discontinued' 
                            when status=4 then 'Deleted' end as Status 
                    from timitem a left join timItemDescription b on a.ItemKey=b.ItemKey 
                        left join tciSTaxClass c on a.STaxClassKey=c.STaxClassKey
                    where a.companyid=@CompanyId and a.UpdateDate>=@LastUpdatedDate",
                parameters);
            resultDto.Data = list;
            return resultDto;
        }
    }
}

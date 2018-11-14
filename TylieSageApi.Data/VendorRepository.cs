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
                @"select Companyid, vendkey as [Key], Vendid,Vendname, 
                        case when status =1 then 'Active' when status=2 then 'Inactive'
                        when status=3 then 'Temporary'
                        when status=4 then 'Deleted' end
                        as Status from tapvendor
                    where companyid=@CompanyId and UpdateDate>=@LastUpdatedDate",
                parameters);
            resultDto.Data = list;
            return resultDto;
        }
    }
}

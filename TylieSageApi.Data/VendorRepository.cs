using System;
using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;

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
                @"SELECT a.CompanyID AS CompanyId, a.VendKey AS [Key], a.VendID AS VendorID, a.VendName AS Vendname, c.ItemID AS ItemId, 
                    CASE WHEN a.status = 1 THEN 'Active' WHEN a.status = 2 THEN 'Inactive' WHEN a.status = 3 THEN 'Temporary' WHEN a.status = 4 THEN 'Deleted' END AS Status 
                    from tapvendor AS a LEFT OUTER JOIN 
                    timVendItem AS b ON a.VendKey = b.VendKey LEFT OUTER JOIN 
                    timItem AS c ON b.ItemKey = c.ItemKey LEFT OUTER JOIN 
                    timvenditem_ext AS d ON b.ItemKey = d.Itemkey AND b.VendKey = d.Vendkey 
                    WHERE (a.CompanyID = @CompanyId) AND (a.UpdateDate >= @LastUpdatedDate) AND (ISNULL(d.NAFlag, 0) <> 1)",
                parameters);
            resultDto.Data = list;
            return resultDto;
        }
    }
}

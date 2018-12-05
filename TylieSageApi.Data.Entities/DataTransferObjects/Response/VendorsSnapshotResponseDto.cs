using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response
{
    public class VendorsSnapshotResponseDto : BaseResponseDto
    {
        public IEnumerable<VendorsSnapshotItem> Data { get; set; }

        public class VendorsSnapshotItem
        {
            public string CompanyID { get; set; }
            public string Key { get; set; }
            public string ItemID { get; set; }
            public string VendorID { get; set; }
            public string Vendname { get; set; }
            public string Status { get; set; }
        }
    }
}

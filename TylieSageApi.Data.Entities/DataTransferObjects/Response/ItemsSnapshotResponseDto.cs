using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response
{
    public class ItemsSnapshotResponseDto : BaseResponseDto
    {
        public IEnumerable<ItemsSnapshotItem> Data { get; set; }

        public class ItemsSnapshotItem
        {
            public string CompanyID { get; set; }
            public string Key { get; set; }
            public string ItemID { get; set; }
            public string ShortDesc { get; set; }
            public string STaxClassID { get; set; }
            public string Status { get; set; }
        }
    }
}

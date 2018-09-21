using System.Collections.Generic;
using Accounting.Data.DataTransferObjects.Response.Abstract;

namespace Accounting.Data.DataTransferObjects.Response
{
    public class ItemsSnapshotResponseDto : BaseResponseDtoWithErrorLinks
    {
        public IList<ItemsSnapshotItem> Data { get; set; }

        public class ErrorLink
        {
            public string Rel { get; set; }
            public string Href { get; set; }
        }

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

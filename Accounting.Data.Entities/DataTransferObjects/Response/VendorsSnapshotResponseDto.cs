using System.Collections.Generic;
using Accounting.Data.DataTransferObjects.Response.Abstract;

namespace Accounting.Data.DataTransferObjects.Response
{
    public class VendorsSnapshotResponseDto : BaseResponseDtoWithErrorLinks
    {
        public IList<VendorsSnapshotItem> Data { get; set; }

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

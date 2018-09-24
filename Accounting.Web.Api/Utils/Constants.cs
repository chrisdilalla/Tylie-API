using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Metadata.W3cXsd2001;
using System.Web;

namespace Accounting.Utils
{
    public class Constants
    {
        public const string RouteWithCompanyAndDate =
            @"{companyID}/{lastUpdatedDate:regex(\d{4}-\d{2}-\d{2})}";
    }
}
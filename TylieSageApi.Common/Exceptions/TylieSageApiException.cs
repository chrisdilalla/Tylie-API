using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TylieSageApi.Common.Exceptions
{
    public class TylieSageApiException: Exception
    {
        protected TylieSageApiException(string message):
            base(message)
        {
        }
    }
}

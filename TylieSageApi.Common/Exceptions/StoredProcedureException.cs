using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TylieSageApi.Common.Exceptions
{
    public class StoredProcedureException: TylieSageApiException
    {
        public StoredProcedureException(string storedProcName, int errorCodeFromDb):
            base($"An error has occurred while executing the stored procedure '{storedProcName}'. Errpr code: '{errorCodeFromDb}'")
        {
        }
    }
}

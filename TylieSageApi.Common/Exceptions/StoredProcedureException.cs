using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace TylieSageApi.Common.Exceptions
{
    public class StoredProcedureException: TylieSageApiException
    {
        public StoredProcedureException(string storedProcName, int errorCodeFromDb):
            this(storedProcName, errorCodeFromDb, String.Empty)
        {
        }

        public StoredProcedureException(string storedProcName, int errorCodeFromDb, string errorMessage) :
            base($"An error has occurred while executing the stored procedure '{storedProcName}'. Error code: '{errorCodeFromDb}'. Message:'{errorMessage}'")
        {
        }
    }
}

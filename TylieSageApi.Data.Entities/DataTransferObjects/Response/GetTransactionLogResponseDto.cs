using System.Collections.Generic;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data.Entities.DataTransferObjects.Response
{
    public class GetTransactionLogResponseDto
    {
        public IList<TransactionLog> TransactionLogEntries { get; set; }
    }
}

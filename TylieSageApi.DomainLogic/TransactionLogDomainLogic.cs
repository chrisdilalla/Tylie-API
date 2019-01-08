using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Request;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
{
    public interface ITransactionLogDomainLogic
    {
        GetTransactionLogResponseDto GetLogEntries(Guid transitId);
    }

    public class TransactionLogDomainLogic : ITransactionLogDomainLogic
    {
        private ITransactionLogRepository _transactionLogRepository;

        public TransactionLogDomainLogic()
        {
            _transactionLogRepository = new TransactionLogRepository();
        }

        public GetTransactionLogResponseDto GetLogEntries(Guid transitId)
        {
            GetTransactionLogResponseDto result = new GetTransactionLogResponseDto();
            result.TransactionLogEntries = _transactionLogRepository.ReadByTransitId(transitId);
            return result;
        }
    }
}

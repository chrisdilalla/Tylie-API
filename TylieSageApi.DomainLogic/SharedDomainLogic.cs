using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Common;
using TylieSageApi.Data;
using TylieSageApi.Data.Entities.DataTransferObjects.Response;
using TylieSageApi.Data.Entities.DataTransferObjects.Response.Base;
using TylieSageApi.Data.Entities.Entities;
using TylieSageApi.DomainLogic.Utils;

namespace TylieSageApi.DomainLogic
{
    internal sealed class SharedDomainLogic
    {
        private ITransactionLogRepository _transactionLogRepository;
        private IUtils _utils;
        internal SharedDomainLogic()
        {
            _transactionLogRepository = new TransactionLogRepository();
            _utils = new Utils.Utils();
        }

        internal T GetDataAndLogTransaction<T>(Func<T> function, string dataName) where T: BaseResponseDto
        {
            TransactionLog transactionLog;
            T result = default(T);
            string transitId = null;

            try
            {
                transitId = _utils.GetGuidString();
                result = function();
                result.TransitID = transitId;
                transactionLog = new TransactionLog(transitId, EventType.SalesOrderDataInsert,
                    $"{dataName} data retrieved successfully");
                _transactionLogRepository.AddRecord(transactionLog);
            }
            catch (Exception exception)
            {
                string errorTitle = $"{dataName} data retrieval error";
                transactionLog = new TransactionLog(transitId, EventType.SalesOrderDataInsert,
                    $"{errorTitle} {exception.Message}");
                _transactionLogRepository.AddRecord(transactionLog);
                result?.AddErrorsFromException(errorTitle, exception);
            }
            return result;
        }
    }
}

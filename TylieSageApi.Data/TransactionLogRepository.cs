using System;
using System.Collections.Generic;
using System.Linq;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface ITransactionLogRepository
    {
        void AddRecord(TransactionLog record);
        IList<TransactionLog> ReadByTransitId(Guid transitId);
    }

    public class TransactionLogRepository: BaseRepository, ITransactionLogRepository
    {
        public TransactionLogRepository():
            base("Tylie_TransactionLog")
        {
        }

        public void AddRecord(TransactionLog record)
        {
            string sql = $@"insert into {TableName} (TransitID, [TimeStamp], EventType, EventDetails)
                values (@TransitID, @TimeStamp, @EventType, @EventDetails)";
            Execute(sql, record);
        }

        public IList<TransactionLog> ReadByTransitId(Guid transitId)
        {
            string sql = $@"select * from {TableName} where TransitId = @TransitId";
            IList<TransactionLog> result = Query<TransactionLog>(sql, new { TransitId = transitId.ToString() }).ToList();
            return result;
        }
    }
}

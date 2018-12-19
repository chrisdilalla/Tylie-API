using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TylieSageApi.Data.Entities.Entities;

namespace TylieSageApi.Data
{
    public interface ITransactionLogRepository
    {
        void AddRecord(TransactionLog record);
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
    }
}

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace TylieSageApi.Data
{
    public abstract class BaseRepository
    {
        private static volatile string _connectionString;
        private static readonly object ConnectionStringLock;
        private string _tableName;
        protected string TableName => _tableName;

        static BaseRepository()
        {
            ConnectionStringLock = new object();
        }

        protected BaseRepository(string tableName)
        {
            _tableName = tableName;
        }

        protected BaseRepository()
        {
        }

        protected virtual IEnumerable<T> Query<T>(string sql, object param = null,
            IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = default(int?),
            CommandType? commandType = default(CommandType?))
        {
            using (SqlConnection sqlConnection = GetSqlConnection())
            {
                return sqlConnection.Query<T>(sql, param, transaction, buffered, commandTimeout, commandType);
            }
        }

        //
        // Summary:
        //     Execute parameterized SQL
        //
        // Returns:
        //     Number of rows affected
        public virtual int Execute(string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = default(int?), CommandType? commandType = default(CommandType?))
        {
            using (SqlConnection sqlConnection = GetSqlConnection())
            {
                return sqlConnection.Execute(sql, param, transaction, commandTimeout, commandType);
            }
        }

        protected SqlConnection GetSqlConnection()
        {
            SqlConnection sqlConnection = new SqlConnection(_connectionString);
            return sqlConnection;
        }

        public static void SetConnectionString(string connectionString)
        {
            if (_connectionString == null)
            {
                lock (ConnectionStringLock)
                {
                    if (_connectionString == null)
                        _connectionString = connectionString;
                }
            }
        }
    }
}

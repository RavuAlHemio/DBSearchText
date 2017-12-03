using System;
using System.Collections.Generic;
using System.Data;
using DBSearchText.Common;

namespace DBSearchText.DB.Sqlite
{
    public class SqliteDBConnectionFactory : IDBConnectionFactory
    {
        private static Lazy<SqliteDBConnectionFactory> _instance =
            new Lazy<SqliteDBConnectionFactory>(() => new SqliteDBConnectionFactory());

        protected SqliteDBConnectionFactory()
        {
        }

        public static SqliteDBConnectionFactory Instance => _instance.Value;

        public virtual IDBConnection GetNewConnection(string connectionString)
            => new SqliteDBConnection(connectionString);
    }
}

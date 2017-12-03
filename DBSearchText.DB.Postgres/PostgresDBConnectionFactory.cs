using System;
using System.Collections.Generic;
using System.Data;
using DBSearchText.Common;

namespace DBSearchText.DB.Postgres
{
    public class PostgresDBConnectionFactory : IDBConnectionFactory
    {
        private static Lazy<PostgresDBConnectionFactory> _instance =
            new Lazy<PostgresDBConnectionFactory>(() => new PostgresDBConnectionFactory());

        protected PostgresDBConnectionFactory()
        {
        }

        public static PostgresDBConnectionFactory Instance => _instance.Value;

        public virtual IDBConnection GetNewConnection(string connectionString)
            => new PostgresDBConnection(connectionString);
    }
}

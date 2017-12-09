using System;
using System.Collections.Generic;
using System.Data;
using DBSearchText.Common;

namespace DBSearchText.DB.SQLServer
{
    public class SQLServerDBConnectionFactory : IDBConnectionFactory
    {
        private static Lazy<SQLServerDBConnectionFactory> _instance =
            new Lazy<SQLServerDBConnectionFactory>(() => new SQLServerDBConnectionFactory());

        protected SQLServerDBConnectionFactory()
        {
        }

        public static SQLServerDBConnectionFactory Instance => _instance.Value;

        public virtual IDBConnection GetNewConnection(string connectionString)
            => new SQLServerDBConnection(connectionString);
    }
}

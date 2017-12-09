using System;
using System.Collections.Generic;
using System.Data;
using DBSearchText.Common;

namespace DBSearchText.DB.MySQL
{
    public class MySQLDBConnectionFactory : IDBConnectionFactory
    {
        private static Lazy<MySQLDBConnectionFactory> _instance =
            new Lazy<MySQLDBConnectionFactory>(() => new MySQLDBConnectionFactory());

        protected MySQLDBConnectionFactory()
        {
        }

        public static MySQLDBConnectionFactory Instance => _instance.Value;

        public virtual IDBConnection GetNewConnection(string connectionString)
            => new MySQLDBConnection(connectionString);
    }
}

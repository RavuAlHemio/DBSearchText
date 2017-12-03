using System;
using System.Collections.Generic;
using System.Data;
using DBSearchText.Common;

namespace DBSearchText.DB.Oracle
{
    public class OracleDBConnectionFactory : IDBConnectionFactory
    {
        private static Lazy<OracleDBConnectionFactory> _instance =
            new Lazy<OracleDBConnectionFactory>(() => new OracleDBConnectionFactory());

        protected OracleDBConnectionFactory()
        {
        }

        public static OracleDBConnectionFactory Instance => _instance.Value;

        public virtual IDBConnection GetNewConnection(string connectionString)
            => new OracleDBConnection(connectionString);
    }
}

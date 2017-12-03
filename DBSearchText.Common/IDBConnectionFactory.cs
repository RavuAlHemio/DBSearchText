using System;
using System.Collections.Generic;

namespace DBSearchText.Common
{
    public interface IDBConnectionFactory
    {
        IDBConnection GetNewConnection(string connectionString);
    }
}

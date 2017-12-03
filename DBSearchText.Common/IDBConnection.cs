using System;
using System.Collections.Generic;

namespace DBSearchText.Common
{
    public interface IDBConnection : IDisposable
    {
        /// <summary>
        /// Obtains an IEnumerable of the names of all tables known by the database engine.
        /// </summary>
        IEnumerable<TableName> GetTableNames();

        /// <summary>
        /// Obtains a table definition for the table with the given name.
        /// </summary>
        TableDefinition GetTableDefinitionByName(TableName name);

        /// <summary>
        /// Obtains an IEnumerable of table row matches where the given substring is contained in
        /// the value of a textual column.
        /// </summary>
        IEnumerable<TextMatch> GetSubstringMatches(TableDefinition tableDef, string substring);
    }
}

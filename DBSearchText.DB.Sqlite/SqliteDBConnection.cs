using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using DBSearchText.Common;
using Microsoft.Data.Sqlite;

namespace DBSearchText.DB.Sqlite
{
    public class SqliteDBConnection : IDBConnection, IDisposable
    {
        protected SqliteConnection Connection { get; set; }

        private bool _disposed = false;

        public SqliteDBConnection(string connectionString)
        {
            Connection = new SqliteConnection(connectionString);
            Connection.Open();
        }

        public virtual IEnumerable<TableName> GetTableNames()
        {
            using (SqliteCommand command = Connection.CreateCommand())
            {
                command.CommandText = "SELECT name FROM SQLITE_MASTER WHERE type='table' ORDER BY 'name'";
                using (SqliteDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        yield return new TableNameT(name);
                    }
                }
            }
        }

        public virtual TableDefinition GetTableDefinitionByName(TableName tableName)
        {
            if (tableName.Table == null)
            {
                throw new ArgumentException($"{nameof(tableName.Table)} must not be null", nameof(tableName));
            }

            using (SqliteCommand command = Connection.CreateCommand())
            {
                command.CommandText = $"PRAGMA table_info('{tableName.Table.EscapeSQLString()}')";

                var primaryColumnNames = new List<string>();
                var textColumnNames = new List<string>();

                using (SqliteDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        // column id, name, type, not null, default value, primary key
                        string colName = reader.GetString(1);
                        string colType = reader.GetString(2);
                        bool isPrimKey = reader.GetBoolean(5);

                        if (isPrimKey)
                        {
                            primaryColumnNames.Add(colName);
                        }

                        if (IsTextColumnType(colType))
                        {
                            textColumnNames.Add(colName);
                        }
                    }
                }

                return new TableDefinition(
                    tableName,
                    primaryColumnNames,
                    textColumnNames
                );
            }
        }

        public virtual IEnumerable<TextMatch> GetSubstringMatches(TableDefinition tableDef, string substring)
        {
            if (tableDef.Name.Table == null)
            {
                throw new ArgumentException($"{nameof(tableDef.Name.Table)} must not be null", nameof(tableDef));
            }

            if (tableDef.TextColumnNames.Count == 0)
            {
                return Enumerable.Empty<TextMatch>();
            }

            var pertinentColumns = new SortedSet<string>();
            pertinentColumns.UnionWith(tableDef.PrimaryKeyColumnNames);
            pertinentColumns.UnionWith(tableDef.TextColumnNames);
            string pertinentColumnsString = pertinentColumns
                .Select(c => $"\"{c.EscapeSQLIdentifier()}\"")
                .JoinWith(", ");

            IEnumerable<string> criteria = tableDef.TextColumnNames
                .Select(tcn => $"\"{tcn.EscapeSQLIdentifier()}\" LIKE '%{substring.EscapeSQLLikeString()}%' ESCAPE '\\'");

            string escTable = tableDef.Name.Table.EscapeSQLIdentifier();
            string query = $@"
                SELECT {pertinentColumnsString}
                FROM ""{escTable}""
                WHERE {criteria.JoinWith(" OR ")}
            ";
            return DBSearchTextUtils.StandardColumnMatchWithQuery(Connection, tableDef, query, substring);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                // dispose of managed resources
                Connection.Dispose();
            }

            // destroy unmanaged resources

            _disposed = true;
        }

        [Pure]
        protected virtual bool IsTextColumnType(string typeName)
        {
            // "Datatypes In SQLite Version 3", section 3.1

            string upperTypeName = typeName.ToUpperInvariant();

            // rule 1: *INT* => INTEGER
            // we need this because earlier rules have precedence over later ones ("CHARINT" => INTEGER)
            if (upperTypeName.Contains("INT"))
            {
                return false;
            }

            // rule 2: *CHAR* *CLOB* *TEXT* => TEXT
            if (upperTypeName.Contains("CHAR") || upperTypeName.Contains("CLOB") || upperTypeName.Contains("TEXT"))
            {
                return true;
            }

            // rule 3: *BLOB* => BLOB
            // rule 4: *REAL* *FLOA* *DOUB* => REAL
            // rule 5: otherwise NUMERIC
            // none of those are textual, thus:
            return false;
        }
    }
}

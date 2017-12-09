using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using DBSearchText.Common;
using Oracle.ManagedDataAccess.Client;

namespace DBSearchText.DB.Oracle
{
    public class OracleDBConnection : IDBConnection, IDisposable
    {
        protected OracleConnection Connection { get; set; }

        private bool _disposed = false;

        public OracleDBConnection(string connectionString)
        {
            Connection = new OracleConnection(connectionString);
            Connection.Open();
        }

        public virtual IEnumerable<TableName> GetTableNames()
        {
            using (OracleCommand command = Connection.CreateCommand())
            {
                command.CommandText = "SELECT OWNER, TABLE_NAME FROM ALL_TABLES";
                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string schema = reader.GetString(0);
                        string table = reader.GetString(1);

                        yield return new TableNameST(schema, table);
                    }
                }
            }
        }

        public virtual TableDefinition GetTableDefinitionByName(TableName tableName)
        {
            if (tableName.Schema == null)
            {
                throw new ArgumentException($"{nameof(tableName.Schema)} must not be null", nameof(tableName));
            }
            if (tableName.Table == null)
            {
                throw new ArgumentException($"{nameof(tableName.Table)} must not be null", nameof(tableName));
            }

            var pKeyColumnNames = new List<string>();
            var textColumnNames = new List<string>();
            using (OracleCommand command = Connection.CreateCommand())
            {
                command.CommandText = $@"
                    SELECT atc.COLUMN_NAME, atc.DATA_TYPE, CASE WHEN ac.CONSTRAINT_NAME IS NULL THEN 0 ELSE 1 END IS_PRIMKEY
                    FROM ALL_TAB_COLUMNS atc
                        LEFT OUTER JOIN (
                            ALL_CONSTRAINTS ac
                            INNER JOIN ALL_CONS_COLUMNS acc ON
                                acc.OWNER = ac.OWNER
                                AND acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME
                                AND acc.TABLE_NAME = ac.TABLE_NAME
                        ) ON
                            ac.OWNER = atc.OWNER
                            AND ac.TABLE_NAME = atc.TABLE_NAME
                            AND ac.CONSTRAINT_TYPE = 'P'
                            AND acc.COLUMN_NAME = atc.COLUMN_NAME
                    WHERE
                        atc.OWNER = :schemaName
                        AND atc.TABLE_NAME = :tableName
                ";
                command.Parameters.Add("schemaName", OracleDbType.Varchar2).Value = tableName.Schema;
                command.Parameters.Add("tableName", OracleDbType.Varchar2).Value = tableName.Table;

                using (OracleDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string typeName = reader.GetString(1);
                        bool isPrimaryKey = (reader.GetInt32(2) != 0);

                        if (isPrimaryKey)
                        {
                            pKeyColumnNames.Add(columnName);
                        }
                        if (IsTextColumnType(typeName))
                        {
                            textColumnNames.Add(columnName);
                        }
                    }
                }
            }

            return new TableDefinition(tableName, pKeyColumnNames, textColumnNames);
        }

        public virtual IEnumerable<TextMatch> GetSubstringMatches(TableDefinition tableDef, string substring)
        {
            if (tableDef.Name.Schema == null)
            {
                throw new ArgumentException($"{nameof(tableDef.Name.Schema)} must not be null", nameof(tableDef));
            }
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
                .Select(c => $"CAST(\"{c.EscapeSQLIdentifier()}\" AS VARCHAR2(4000)) \"{c.EscapeSQLIdentifier()}\"")
                .JoinWith(", ");

            IEnumerable<string> criteria = tableDef.TextColumnNames
                .Select(tcn => $"\"{tcn.EscapeSQLIdentifier()}\" LIKE '%{substring.EscapeSQLLikeString()}%' ESCAPE '\\'");

            string escSchema = tableDef.Name.Schema.EscapeSQLIdentifier();
            string escTable = tableDef.Name.Table.EscapeSQLIdentifier();
            string query = $@"
                SELECT {pertinentColumnsString}
                FROM ""{escSchema}"".""{escTable}""
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
            => typeName == "CHAR" || typeName == "CLOB" || typeName == "VARCHAR" || typeName == "VARCHAR2";
    }
}

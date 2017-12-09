using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.Contracts;
using System.Linq;
using DBSearchText.Common;

namespace DBSearchText.DB.SQLServer
{
    public class SQLServerDBConnection : IDBConnection, IDisposable
    {
        protected SqlConnection Connection { get; set; }

        private bool _disposed = false;

        public SQLServerDBConnection(string connectionString)
        {
            Connection = new SqlConnection(connectionString);
            Connection.Open();
        }

        public virtual IEnumerable<TableName> GetTableNames()
        {
            var dbNames = new List<string>();
            using (SqlCommand databasesCmd = Connection.CreateCommand())
            {
                databasesCmd.CommandText = @"SELECT name FROM sys.databases";
                using (SqlDataReader reader = databasesCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string database = reader.GetString(0);
                        dbNames.Add(database);
                    }
                }
            }

            foreach (string dbName in dbNames)
            {
                using (SqlCommand tablesCmd = Connection.CreateCommand())
                {
                    tablesCmd.CommandText = $@"
                        SELECT s.name AS schema_name, o.name AS table_name
                        FROM [{EscapeSQLServerIdentifier(dbName)}].sys.objects AS o
                            INNER JOIN [{EscapeSQLServerIdentifier(dbName)}].sys.schemas AS s ON s.schema_id = o.schema_id
                        WHERE o.type = 'U'
                    ";
                    using (SqlDataReader reader = tablesCmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string schema = reader.GetString(0);
                            string table = reader.GetString(1);

                            yield return new TableNameDST(dbName, schema, table);
                        }
                    }
                }
            }
        }

        public virtual TableDefinition GetTableDefinitionByName(TableName tableName)
        {
            if (tableName.Database == null)
            {
                throw new ArgumentException($"{nameof(tableName.Database)} must not be null", nameof(tableName));
            }
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
            using (SqlCommand command = Connection.CreateCommand())
            {
                string escDB = EscapeSQLServerIdentifier(tableName.Database);
                command.CommandText = $@"
                    SELECT c.name AS column_name, t.name AS type_name, CASE WHEN ic.column_id IS NULL THEN 0 ELSE 1 END AS is_primary_key
                    FROM [{escDB}].sys.objects AS o
                        INNER JOIN [{escDB}].sys.schemas AS s ON s.schema_id = o.schema_id
                        INNER JOIN [{escDB}].sys.columns AS c ON c.object_id = o.object_id
                        INNER JOIN [{escDB}].sys.types AS t ON t.user_type_id = c.user_type_id
                        LEFT OUTER JOIN (
                            [{escDB}].sys.indexes AS i
                            INNER JOIN [{escDB}].sys.index_columns AS ic ON ic.index_id = i.index_id AND ic.object_id = i.object_id
                        ) ON i.object_id = o.object_id AND i.is_primary_key = 1 AND ic.column_id = c.column_id
                    WHERE
                        s.name = @Schema
                        AND o.name = @Table
                    ORDER BY
                        c.column_id
                ";
                command.Parameters.Add("@Schema", SqlDbType.NVarChar).Value = tableName.Schema;
                command.Parameters.Add("@Table", SqlDbType.NVarChar).Value = tableName.Table;

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string typeName = reader.GetString(1);
                        bool isPrimaryKey = (reader.GetInt32(2) == 1);

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
            if (tableDef.Name.Database == null)
            {
                throw new ArgumentException($"{nameof(tableDef.Name.Database)} must not be null", nameof(tableDef));
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
                .Select(c => $"[{EscapeSQLServerIdentifier(c)}]")
                .JoinWith(", ");

            IEnumerable<string> criteria = tableDef.TextColumnNames
                .Select(tcn => $"[{EscapeSQLServerIdentifier(tcn)}] LIKE '%{substring.EscapeSQLLikeString()}%' ESCAPE '\\'");

            string escDB = tableDef.Name.Database.EscapeSQLIdentifier();
            string escSchema = tableDef.Name.Schema.EscapeSQLIdentifier();
            string escTable = tableDef.Name.Table.EscapeSQLIdentifier();
            string query = $@"
                SELECT {pertinentColumnsString}
                FROM [{escDB}].[{escSchema}].[{escTable}]
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
            => typeName.EndsWith("char") || typeName.EndsWith("text");

        [Pure]
        public static string EscapeSQLServerIdentifier(string identifier)
            => identifier.Replace("]", "]]");
    }
}

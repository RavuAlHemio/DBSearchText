using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using DBSearchText.Common;
using Npgsql;
using NpgsqlTypes;

namespace DBSearchText.DB.Postgres
{
    public class PostgresDBConnection : IDBConnection, IDisposable
    {
        protected NpgsqlConnection Connection { get; set; }

        private bool _disposed = false;

        public PostgresDBConnection(string connectionString)
        {
            Connection = new NpgsqlConnection(connectionString);
            Connection.Open();
        }

        public virtual IEnumerable<TableName> GetTableNames()
        {
            #if POSTGRESQL_CROSS_DATABASE_SUPPORT
            var databaseNames = new List<string>();
            using (NpgsqlCommand command = Connection.CreateCommand())
            {
                command.CommandText = "SELECT datname FROM pg_catalog.pg_database";
                using (NpgsqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string name = reader.GetString(0);
                        databaseNames.Add(name);
                    }
                }
            }

            foreach (string databaseName in databaseNames)
            {
                var tableNames = new List<string>();
                using (NpgsqlCommand command = Connection.CreateCommand())
                {
                    command.CommandText = $@"
                        SELECT n.nspname, c.relname
                        FROM ""{databaseName.EscapeSQLIdentifier()}"".pg_catalog.pg_class AS c
                        INNER JOIN ""{databaseName.EscapeSQLIdentifier()}"".pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE c.relkind IN ('r', 'p')
                            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                        ORDER BY
                            n.nspname,
                            c.relname
                    ";
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string schema = reader.GetString(0);
                            string table = reader.GetString(1);

                            yield return new TableNameDST(databaseName, schema, table);
                        }
                    }
                }
            }
            #else
            using (NpgsqlCommand command = Connection.CreateCommand())
            {
                command.CommandText = $@"
                    SELECT n.nspname, c.relname
                    FROM pg_catalog.pg_class AS c
                    INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r', 'p')
                        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY
                        n.nspname,
                        c.relname
                ";
                using (NpgsqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string schema = reader.GetString(0);
                        string table = reader.GetString(1);

                        yield return new TableNameST(schema, table);
                    }
                }
            }
            #endif
        }

        public virtual TableDefinition GetTableDefinitionByName(TableName tableName)
        {
            #if POSTGRESQL_CROSS_DATABASE_SUPPORT
            if (tableName.Database == null)
            {
                throw new ArgumentException($"{nameof(tableName.Database)} must not be null", nameof(tableName));
            }
            #endif
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
            using (NpgsqlCommand command = Connection.CreateCommand())
            {
                // FIXME: doesn't check for expression-bodied primary keys
                #if POSTGRESQL_CROSS_DATABASE_SUPPORT
                command.CommandText = $@"
                    SELECT a.attname, t.typname, CASE WHEN i.indrelid IS NULL THEN FALSE ELSE TRUE END AS isprimkey
                    FROM ""{tableName.Database.EscapeSQLIdentifier()}"".pg_catalog.pg_class AS c
                    INNER JOIN ""{tableName.Database.EscapeSQLIdentifier()}"".pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
                    INNER JOIN ""{tableName.Database.EscapeSQLIdentifier()}"".pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                    INNER JOIN ""{tableName.Database.EscapeSQLIdentifier()}"".pg_catalog.pg_type AS t ON t.oid = a.atttypid
                    LEFT OUTER JOIN ""{tableName.Database.EscapeSQLIdentifier()}"".pg_catalog.pg_index AS i ON i.indrelid = c.oid AND a.attnum = ANY(i.indkey) AND i.indisprimary = TRUE
                    WHERE n.nspname = @Schema
                        AND c.relname = @Table
                        AND c.relkind IN ('r', 'p')
                        AND a.attnum >= 0
                    ORDER BY
                        n.nspname,
                        c.relname,
                        a.attnum
                ";
                #else
                command.CommandText = $@"
                    SELECT a.attname, t.typname, CASE WHEN i.indrelid IS NULL THEN FALSE ELSE TRUE END AS isprimkey
                    FROM pg_catalog.pg_class AS c
                    INNER JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
                    INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                    INNER JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
                    LEFT OUTER JOIN pg_catalog.pg_index AS i ON i.indrelid = c.oid AND a.attnum = ANY(i.indkey) AND i.indisprimary = TRUE
                    WHERE n.nspname = @Schema
                        AND c.relname = @Table
                        AND c.relkind IN ('r', 'p')
                        AND a.attnum >= 0
                    ORDER BY
                        n.nspname,
                        c.relname,
                        a.attnum
                ";
                #endif
                command.Parameters.Add("@Schema", NpgsqlDbType.Varchar).Value = tableName.Schema;
                command.Parameters.Add("@Table", NpgsqlDbType.Varchar).Value = tableName.Table;

                using (NpgsqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string typeName = reader.GetString(1);
                        bool isPrimaryKey = reader.GetBoolean(2);

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
            #if POSTGRESQL_CROSS_DATABASE_SUPPORT
            if (tableDef.Name.Database == null)
            {
                throw new ArgumentException($"{nameof(tableDef.Name.Database)} must not be null", nameof(tableDef));
            }
            #endif
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
                .Select(c => $"\"{c.EscapeSQLIdentifier()}\"")
                .JoinWith(", ");

            IEnumerable<string> criteria = tableDef.TextColumnNames
                .Select(tcn => $"\"{tcn.EscapeSQLIdentifier()}\" LIKE '%{substring.EscapeSQLLikeString()}%' ESCAPE '\\'");

            #if POSTGRESQL_CROSS_DATABASE_SUPPORT
            string escDB = tableDef.Name.Database.EscapeSQLIdentifier();
            #endif
            string escSchema = tableDef.Name.Schema.EscapeSQLIdentifier();
            string escTable = tableDef.Name.Table.EscapeSQLIdentifier();

            #if POSTGRESQL_CROSS_DATABASE_SUPPORT
            string query = $@"
                SELECT {pertinentColumnsString}
                FROM ""{escDB}"".""{escSchema}"".""{escTable}""
                WHERE {criteria.JoinWith(" OR ")}
            ";
            #else
            string query = $@"
                SELECT {pertinentColumnsString}
                FROM ""{escSchema}"".""{escTable}""
                WHERE {criteria.JoinWith(" OR ")}
            ";
            #endif
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
            => typeName == "char" || typeName == "varchar" || typeName == "text";
    }
}

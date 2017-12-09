using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using DBSearchText.Common;
using MySql.Data.MySqlClient;

namespace DBSearchText.DB.MySQL
{
    public class MySQLDBConnection : IDBConnection, IDisposable
    {
        protected MySqlConnection Connection { get; set; }

        private bool _disposed = false;

        public MySQLDBConnection(string connectionString)
        {
            Connection = new MySqlConnection(connectionString);
            Connection.Open();
        }

        public virtual IEnumerable<TableName> GetTableNames()
        {
            using (MySqlCommand command = Connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE='BASE TABLE'
                ";
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string database = reader.GetString(0);
                        string table = reader.GetString(1);

                        yield return new TableNameDT(database, table);
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
            if (tableName.Table == null)
            {
                throw new ArgumentException($"{nameof(tableName.Table)} must not be null", nameof(tableName));
            }

            var pKeyColumnNames = new List<string>();
            var textColumnNames = new List<string>();
            using (MySqlCommand command = Connection.CreateCommand())
            {
                command.CommandText = $@"
                    SELECT C.COLUMN_NAME, C.DATA_TYPE, CASE WHEN KCU.COLUMN_NAME IS NULL THEN FALSE ELSE TRUE END AS IS_PRIMARY_KEY
                    FROM INFORMATION_SCHEMA.COLUMNS AS C
                    LEFT OUTER JOIN (
                        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
                            ON KCU.CONSTRAINT_CATALOG = TC.CONSTRAINT_CATALOG
                            AND KCU.CONSTRAINT_SCHEMA = TC.CONSTRAINT_SCHEMA
                            AND KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
                            AND KCU.TABLE_SCHEMA = TC.TABLE_SCHEMA
                            AND KCU.TABLE_NAME = TC.TABLE_NAME
                    )
                        ON TC.TABLE_SCHEMA = C.TABLE_SCHEMA
                        AND TC.TABLE_NAME = C.TABLE_NAME
                        AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        AND KCU.COLUMN_NAME = C.COLUMN_NAME
                    WHERE C.TABLE_SCHEMA = @Database
                        AND C.TABLE_NAME = @Table
                    ORDER BY
                        C.ORDINAL_POSITION
                ";
                command.Parameters.Add("@Database", MySqlDbType.VarChar).Value = tableName.Database;
                command.Parameters.Add("@Table", MySqlDbType.VarChar).Value = tableName.Table;

                using (MySqlDataReader reader = command.ExecuteReader())
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
                .Select(c => $"`{EscapeMySQLIdentifier(c)}`")
                .JoinWith(", ");

            IEnumerable<string> criteria = tableDef.TextColumnNames
                .Select(tcn => $"`{EscapeMySQLIdentifier(tcn)}` LIKE '%{substring.EscapeSQLLikeString()}%' ESCAPE '\\\\'");

            string escDB = tableDef.Name.Database.EscapeSQLIdentifier();
            string escTable = tableDef.Name.Table.EscapeSQLIdentifier();
            string query = $@"
                SELECT {pertinentColumnsString}
                FROM `{escDB}`.`{escTable}`
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
        public static string EscapeMySQLIdentifier(string identifier)
            => identifier.Replace("`", "``");
    }
}

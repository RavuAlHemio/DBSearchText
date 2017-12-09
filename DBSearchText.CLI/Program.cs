using System;
using System.Collections.Generic;
using System.Linq;
using DBSearchText.Common;

namespace DBSearchText.CLI
{
    class Program
    {
        static int Main(string[] args)
        {
            RegisterPlugins();

            if (args.Length != 3)
            {
                ShowUsage();
                return 1;
            }

            string module = args[0];
            string connString = args[1];
            string substring = args[2];

            string separator = new string('=', 40);

            using (var connection = PluginCollection.Instance.GetNewConnection(module, connString))
            {
                // fetch the full list (not everyone supports multiple active result sets)
                List<TableName> tableNames = connection.GetTableNames().ToList();
                foreach (TableName name in tableNames)
                {
                    string dottedTableName = name.NameComponents
                        .JoinWith(".");

                    TableDefinition def = connection.GetTableDefinitionByName(name);
                    foreach (TextMatch match in connection.GetSubstringMatches(def, substring))
                    {
                        string primKeyVals = match.RowPrimaryKey
                            .Select(rpk => $"{rpk.Key}={rpk.Value}")
                            .JoinWith(", ")
                        ;

                        Console.WriteLine(separator);
                        Console.WriteLine($"Table: {dottedTableName}");
                        Console.WriteLine($"Primary key values: {primKeyVals}");
                        Console.WriteLine($"Matching column: {match.MatchingColumnName}");
                        Console.WriteLine($"Matching value: {match.MatchingColumnValue}");
                    }
                }
            }

            return 0;
        }

        static void ShowUsage()
        {
            Console.WriteLine("Usage: DBSearchText DBPLUGIN CONNECTIONSTRING SUBSTRING");
            Console.WriteLine("    Connects to a database engine (according to DBPLUGIN) using CONNECTIONSTRING");
            Console.WriteLine("    and searches all textual columns in all tables for SUBSTRING.");
            Console.WriteLine();
            Console.WriteLine("    Supported database plugins:");
            foreach (string module in PluginCollection.Instance.PluginNames)
            {
                Console.WriteLine($"    * {module}");
            }
        }

        static void RegisterPlugins()
        {
            // FIXME: implement scan-and-load
            #if DB_PLUGIN_MYSQL
            PluginCollection.Instance.RegisterPlugin("mysql", DBSearchText.DB.MySQL.MySQLDBConnectionFactory.Instance);
            #endif
            #if DB_PLUGIN_ORACLE
            PluginCollection.Instance.RegisterPlugin("oracle", DBSearchText.DB.Oracle.OracleDBConnectionFactory.Instance);
            #endif
            #if DB_PLUGIN_POSTGRESQL
            PluginCollection.Instance.RegisterPlugin("postgresql", DBSearchText.DB.Postgres.PostgresDBConnectionFactory.Instance);
            #endif
            #if DB_PLUGIN_SQLITE
            PluginCollection.Instance.RegisterPlugin("sqlite", DBSearchText.DB.Sqlite.SqliteDBConnectionFactory.Instance);
            #endif
            #if DB_PLUGIN_SQLSERVER
            PluginCollection.Instance.RegisterPlugin("sqlserver", DBSearchText.DB.SQLServer.SQLServerDBConnectionFactory.Instance);
            #endif
        }
    }
}

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data.Common;
using System.Diagnostics.Contracts;
using System.Linq;

namespace DBSearchText.Common
{
    public static class DBSearchTextUtils
    {
        [Pure]
        public static bool SequenceEqualOrdinal(this IEnumerable<string> mine, IEnumerable<string> theirs)
        {
            return mine.SequenceEqual(theirs, StringComparer.Ordinal);
        }

        [Pure]
        public static int CompareOrdinal(KeyValuePair<string, string> left, KeyValuePair<string, string> right)
        {
            int comparison;
            
            comparison = string.Compare(left.Key, right.Key, StringComparison.Ordinal);
            if (comparison != 0)
            {
                return comparison;
            }

            comparison = string.Compare(left.Value, right.Value, StringComparison.Ordinal);
            if (comparison != 0)
            {
                return comparison;
            }

            return 0;
        }

        [Pure]
        public static bool DictionaryEqualOrdinal(this IReadOnlyDictionary<string, string> mine,
                IReadOnlyDictionary<string, string> theirs)
        {
            if (mine == null)
            {
                return (theirs == null);
            }
            
            if (theirs == null)
            {
                // mine != null
                return false;
            }

            var mineSorted = new List<KeyValuePair<string, string>>(mine);
            mineSorted.Sort(CompareOrdinal);

            var theirsSorted = new List<KeyValuePair<string, string>>(theirs);
            theirsSorted.Sort(CompareOrdinal);

            return mineSorted.Zip(theirsSorted, (m, t) => CompareOrdinal(m, t) == 0)
                .All(eq => eq);
        }

        [Pure]
        public static string EscapeSQLIdentifier(this string sqlIdentifier)
            => sqlIdentifier.Replace("\"", "\"\"");

        [Pure]
        public static string EscapeSQLString(this string sqlString)
            => sqlString.Replace("'", "''");

        [Pure]
        public static string EscapeSQLLikeString(this string sqlLikeString, char escapeChar = '\\')
            => sqlLikeString.Replace($"{escapeChar}", $"{escapeChar}{escapeChar}")
                .Replace("%", $"{escapeChar}%")
                .Replace("_", $"{escapeChar}_")
                .Replace("[", $"{escapeChar}[")
            ;

        [Pure]
        public static string JoinWith(this IEnumerable<string> strings, string glue)
            => string.Join(glue, strings);

        public static IEnumerable<TextMatch> StandardColumnMatchWithQuery(DbConnection conn, TableDefinition tableDef,
                string query, string substring)
        {
            using (DbCommand command = conn.CreateCommand())
            {
                command.CommandText = query;
                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var pKeyBuilder = ImmutableSortedDictionary.CreateBuilder<string, string>();
                        foreach (string pKeyColName in tableDef.PrimaryKeyColumnNames)
                        {
                            string pKeyColVal = reader.GetString(reader.GetOrdinal(pKeyColName));
                            pKeyBuilder[pKeyColName] = pKeyColVal;
                        }
                        ImmutableSortedDictionary<string, string> pKey = pKeyBuilder.ToImmutable();

                        foreach (string textColName in tableDef.TextColumnNames)
                        {
                            int ordinal = reader.GetOrdinal(textColName);
                            if (reader.IsDBNull(ordinal))
                            {
                                continue;
                            }

                            string textColVal = reader.GetString(ordinal);
                            if (textColVal.Contains(substring))
                            {
                                yield return new TextMatch(pKey, textColName, textColVal);
                            }
                        }
                    }
                }
            }
        }
    }
}

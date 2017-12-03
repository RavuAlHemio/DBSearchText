using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace DBSearchText.Common
{
    public class TextMatch : IEquatable<TextMatch>
    {
        public ImmutableSortedDictionary<string, string> RowPrimaryKey { get; }
        public string MatchingColumnName { get; }
        public string MatchingColumnValue { get; }

        public TextMatch(IEnumerable<KeyValuePair<string, string>> rowPrimaryKey, string matchingColumnName,
                string matchingColumnValue)
        {
            RowPrimaryKey = rowPrimaryKey.ToImmutableSortedDictionary();
            MatchingColumnName = matchingColumnName;
            MatchingColumnValue = matchingColumnValue;
        }

        public bool Equals(TextMatch other)
        {
            if (other == null)
            {
                return false;
            }

            if (!this.RowPrimaryKey.DictionaryEqualOrdinal(other.RowPrimaryKey))
            {
                return false;
            }

            if (this.MatchingColumnName != other.MatchingColumnName)
            {
                return false;
            }

            if (this.MatchingColumnValue != other.MatchingColumnValue)
            {
                return false;
            }

            return true;
        }

        public override bool Equals(object/*?*/ obj)
        {
            if (obj == null)
            {
                return false;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return Equals((TextMatch)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = 0;

                foreach (KeyValuePair<string, string> entry in RowPrimaryKey)
                {
                    result = (result * 397) ^ entry.Key.GetHashCode();
                    result = (result * 397) ^ entry.Value.GetHashCode();
                }
                result = (result * 397) ^ MatchingColumnName.GetHashCode();
                result = (result * 397) ^ MatchingColumnValue.GetHashCode();

                return result;
            }
        }
    }
}

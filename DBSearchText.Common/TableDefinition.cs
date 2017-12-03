using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;

namespace DBSearchText.Common
{
    public class TableDefinition : IEquatable<TableDefinition>
    {
        public TableName Name { get; }
        public ImmutableSortedSet<string> PrimaryKeyColumnNames { get; }
        public ImmutableSortedSet<string> TextColumnNames { get; }

        public TableDefinition(TableName name, IEnumerable<string> primaryKeyColumnNames,
                IEnumerable<string> textColumnNames)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (primaryKeyColumnNames == null)
            {
                throw new ArgumentNullException(nameof(primaryKeyColumnNames));
            }
            if (textColumnNames == null)
            {
                throw new ArgumentNullException(nameof(textColumnNames));
            }
            Contract.EndContractBlock();
            Contract.Requires(Contract.ForAll(primaryKeyColumnNames, cn => !string.IsNullOrEmpty(cn)));
            Contract.Requires(Contract.ForAll(textColumnNames, cn => !string.IsNullOrEmpty(cn)));

            Name = name;
            PrimaryKeyColumnNames = primaryKeyColumnNames.ToImmutableSortedSet();
            TextColumnNames = textColumnNames.ToImmutableSortedSet();

            if (PrimaryKeyColumnNames.Any(cn => string.IsNullOrEmpty(cn)))
            {
                throw new ArgumentException(
                    $"none of the elements of {nameof(primaryKeyColumnNames)} may be null",
                    nameof(primaryKeyColumnNames)
                );
            }
            if (TextColumnNames.Any(cn => string.IsNullOrEmpty(cn)))
            {
                throw new ArgumentException(
                    $"none of the elements of {nameof(textColumnNames)} may be null",
                    nameof(textColumnNames)
                );
            }
        }

        public bool Equals(TableDefinition other)
        {
            if (other == null)
            {
                return false;
            }

            if (!this.Name.Equals(other.Name))
            {
                return false;
            }

            if (!this.PrimaryKeyColumnNames.SequenceEqualOrdinal(other.PrimaryKeyColumnNames))
            {
                return false;
            }

            if (!this.TextColumnNames.SequenceEqualOrdinal(other.TextColumnNames))
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

            return Equals((TableDefinition)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = 0;

                result = (result * 397) ^ Name.GetHashCode();
                foreach (string cn in PrimaryKeyColumnNames)
                {
                    result = (result * 397) ^ cn.GetHashCode();
                }
                foreach (string cn in TextColumnNames)
                {
                    result = (result * 397) ^ cn.GetHashCode();
                }

                return result;
            }
        }
    }
}

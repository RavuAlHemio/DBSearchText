using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;

namespace DBSearchText.Common
{
    public abstract class TableName : IEquatable<TableName>
    {
        /// <summary>
        /// The name of the database, or <c>null</c> if the database engine does not support the concept of databases.
        ///
        /// In general, a database is a collection of tables and other related objects, all stored independently of
        /// other databases but accessed via the same resource (e.g. a database server).
        /// </summary>
        public virtual string Database => null;

        /// <summary>
        /// The name of the schema, or <c>null</c> if the database engine does not support the concept of schemas.
        ///
        /// In general, a schema is a subdivision of a database which groups together database objects into namespaces
        /// and, via access control mechanisms, allows to grant different roles different perspectives of the same
        /// database.
        /// </summary>
        public virtual string Schema => null;

        /// <summary>
        /// The name of the table, or <c>null</c> if the database engine does not support the concept of tables, which
        /// is very rare.
        ///
        /// A table is a container of data of common structure (defined by the table's columns) and often common or
        /// related meaning. Entries in tables are known as rows.
        /// </summary>
        public virtual string Table => null;

        /// <summary>
        /// The raw components of the table's name.
        /// </summary>
        public ImmutableArray<string> NameComponents { get; }

        public TableName(IEnumerable<string> nameComponents)
        {
            if (nameComponents == null)
            {
                throw new ArgumentNullException(nameof(nameComponents));
            }
            Contract.EndContractBlock();
            Contract.Requires(Contract.ForAll(nameComponents, nc => nc != null));

            NameComponents = ImmutableArray.CreateRange(nameComponents);
            if (NameComponents.Any(nc => nc == null))
            {
                throw new ArgumentException(
                    $"none of the elements of {nameof(nameComponents)} may be null",
                    nameof(nameComponents)
                );
            }
        }

        public bool Equals(TableName other)
        {
            if (object.ReferenceEquals(other, null))
            {
                return false;
            }

            if (!this.NameComponents.SequenceEqualOrdinal(other.NameComponents))
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

            return Equals((TableName)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int result = 0;
                foreach (string nc in NameComponents)
                {
                    result = (result * 397) ^ nc.GetHashCode();
                }
                return result;
            }
        }

        public static bool operator==(TableName left, TableName right)
        {
            return left.Equals(right);
        }

        public static bool operator!=(TableName left, TableName right)
        {
            return !(left == right);
        }

        [ContractInvariantMethod]
        private void ObjectInvariant()
        {
            Contract.Invariant(Contract.ForAll(NameComponents, nc => nc != null));
        }
    }
}

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;

namespace DBSearchText.Common
{
    public class TableNameDST : TableName
    {
        public override string Database => NameComponents[0];
        public override string Schema => NameComponents[1];
        public override string Table => NameComponents[2];

        protected TableNameDST(IEnumerable<string> nameComponents)
            : base(nameComponents)
        {
        }

        public TableNameDST(string database, string schema, string table)
            : base(MakeNameComponents(database, schema, table))
        {
            Contract.Requires(database != null);
            Contract.Requires(schema != null);
            Contract.Requires(table != null);
        }

        private static string[] MakeNameComponents(string database, string schema, string table)
        {
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }
            if (schema == null)
            {
                throw new ArgumentNullException(nameof(schema));
            }
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }
            Contract.EndContractBlock();
            Contract.Ensures(Contract.Result<string[]>() != null);
            Contract.Ensures(Contract.Result<string[]>().Length == 3);
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[0], database, StringComparison.Ordinal));
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[1], schema, StringComparison.Ordinal));
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[2], table, StringComparison.Ordinal));

            return new string[] { database, schema, table };
        }

        [ContractInvariantMethod]
        private void ObjectInvariant()
        {
            Contract.Invariant(Database != null);
            Contract.Invariant(Schema != null);
            Contract.Invariant(Table != null);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;

namespace DBSearchText.Common
{
    public class TableNameST : TableName
    {
        public override string Database => null;
        public override string Schema => NameComponents[0];
        public override string Table => NameComponents[1];

        protected TableNameST(IEnumerable<string> nameComponents)
            : base(nameComponents)
        {
        }

        public TableNameST(string schema, string table)
            : base(MakeNameComponents(schema, table))
        {
            Contract.Requires(schema != null);
            Contract.Requires(table != null);
        }

        private static string[] MakeNameComponents(string schema, string table)
        {
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
            Contract.Ensures(Contract.Result<string[]>().Length == 2);
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[0], schema, StringComparison.Ordinal));
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[1], table, StringComparison.Ordinal));

            return new string[] { schema, table };
        }

        [ContractInvariantMethod]
        private void ObjectInvariant()
        {
            Contract.Invariant(Schema != null);
            Contract.Invariant(Table != null);
        }
    }
}

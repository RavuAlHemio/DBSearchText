using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;

namespace DBSearchText.Common
{
    public class TableNameT : TableName
    {
        public override string Table => NameComponents[0];

        protected TableNameT(IEnumerable<string> nameComponents)
            : base(nameComponents)
        {
        }

        public TableNameT(string table)
            : base(MakeNameComponents(table))
        {
            Contract.Requires(table != null);
        }

        private static string[] MakeNameComponents(string table)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }
            Contract.EndContractBlock();
            Contract.Ensures(Contract.Result<string[]>() != null);
            Contract.Ensures(Contract.Result<string[]>().Length == 1);
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[0], table, StringComparison.Ordinal));

            return new string[] { table };
        }

        [ContractInvariantMethod]
        private void ObjectInvariant()
        {
            Contract.Invariant(Table != null);
        }
    }
}

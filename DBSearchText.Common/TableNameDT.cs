using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;
using System.Linq;

namespace DBSearchText.Common
{
    public class TableNameDT : TableName
    {
        public override string Database => NameComponents[0];

        public override string Table => NameComponents[1];

        protected TableNameDT(IEnumerable<string> nameComponents)
            : base(nameComponents)
        {
        }

        public TableNameDT(string database, string table)
            : base(MakeNameComponents(database, table))
        {
            Contract.Requires(database != null);
            Contract.Requires(table != null);
        }

        private static string[] MakeNameComponents(string database, string table)
        {
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }
            Contract.EndContractBlock();
            Contract.Ensures(Contract.Result<string[]>() != null);
            Contract.Ensures(Contract.Result<string[]>().Length == 2);
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[0], database, StringComparison.Ordinal));
            Contract.Ensures(string.Equals(Contract.Result<string[]>()[1], table, StringComparison.Ordinal));

            return new string[] { database, table };
        }

        [ContractInvariantMethod]
        private void ObjectInvariant()
        {
            Contract.Invariant(Database != null);
            Contract.Invariant(Table != null);
        }
    }
}

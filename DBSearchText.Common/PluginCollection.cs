using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Contracts;

namespace DBSearchText.Common
{
    public sealed class PluginCollection
    {
        private static Lazy<PluginCollection> _instance = new Lazy<PluginCollection>(() => new PluginCollection());

        private Dictionary<string, IDBConnectionFactory> _pluginRegistry;
        private ImmutableSortedSet<string> _cachedPluginNames;

        public static PluginCollection Instance => _instance.Value;

        public ImmutableSortedSet<string> PluginNames => (_cachedPluginNames == null)
            ? (_cachedPluginNames = _pluginRegistry.Keys.ToImmutableSortedSet())
            : _cachedPluginNames;

        private PluginCollection()
        {
            _pluginRegistry = new Dictionary<string, IDBConnectionFactory>();
            _cachedPluginNames = null;
        }

        public void RegisterPlugin(string name, IDBConnectionFactory factory)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (factory == null)
            {
                throw new ArgumentNullException(nameof(factory));
            }
            Contract.EndContractBlock();

            _cachedPluginNames = null;
            _pluginRegistry[name] = factory;
        }

        public IDBConnection/*?*/ GetNewConnection(string plugin, string connectionString)
        {
            if (plugin == null)
            {
                throw new ArgumentNullException(nameof(plugin));
            }
            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }
            Contract.EndContractBlock();

            IDBConnectionFactory factory;
            if (!_pluginRegistry.TryGetValue(plugin, out factory))
            {
                return null;
            }
            Contract.Assert(factory != null);

            return factory.GetNewConnection(connectionString);
        }

        [ContractInvariantMethod]
        private void ObjectInvariant()
        {
            Contract.Invariant(Contract.ForAll(_pluginRegistry, kvp => kvp.Key != null && kvp.Value != null));
        }
    }
}

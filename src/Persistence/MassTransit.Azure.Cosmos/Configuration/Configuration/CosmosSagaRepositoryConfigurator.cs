namespace MassTransit.Configuration
{
    using System;
    using System.Collections.Generic;
    using AzureCosmos;
    using AzureCosmos.Saga;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Saga;


    public class CosmosSagaRepositoryConfigurator<TSaga> :
        ICosmosSagaRepositoryConfigurator<TSaga>,
        ISpecification
        where TSaga : class, ISaga
    {
        string _clientName;
        Func<IServiceProvider, ICosmosCollectionIdFormatter> _collectionIdFormatter;
        Action<ItemRequestOptions> _itemRequestOptions;
        Action<QueryRequestOptions> _queryRequestOptions;
        Action<ISagaRepositoryRegistrationConfigurator<TSaga>> _registerClientFactory;

        public CosmosSagaRepositoryConfigurator()
        {
            _collectionIdFormatter = _ => KebabCaseCollectionIdFormatter.Instance;

            _registerClientFactory = RegisterSystemTextJsonClientFactory;
        }

        public void ConfigureEmulator()
        {
            EndpointUri = AzureCosmosEmulatorConstants.EndpointUri;
            Key = AzureCosmosEmulatorConstants.Key;
        }

        public void SetCollectionIdFormatter(ICosmosCollectionIdFormatter collectionIdFormatter)
        {
            if (collectionIdFormatter == null)
                throw new ArgumentNullException(nameof(collectionIdFormatter));
            SetCollectionIdFormatter(_ => collectionIdFormatter);
        }

        public string DatabaseId { get; set; }

        public string CollectionId
        {
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                    throw new ArgumentException(nameof(value));

                _collectionIdFormatter = _ => new DefaultCollectionIdFormatter(value);
            }
        }

        public string EndpointUri { get; set; }
        public string Key { get; set; }

        public void ConfigureItemRequestOptions(Action<ItemRequestOptions> cfg)
        {
            _itemRequestOptions = cfg ?? throw new ArgumentNullException(nameof(cfg));
        }

        public void ConfigureQueryRequestOptions(Action<QueryRequestOptions> cfg)
        {
            _queryRequestOptions = cfg ?? throw new ArgumentNullException(nameof(cfg));
        }

        public void UseClientFactory(string clientName)
        {
            if (string.IsNullOrWhiteSpace(clientName))
                throw new ArgumentException(nameof(clientName));

            _clientName = clientName;
        }

        public void UseNewtonsoftJson()
        {
            _registerClientFactory = RegisterNewtonsoftJsonClientFactory;
        }

        public void SetCollectionIdFormatter(Func<IServiceProvider, ICosmosCollectionIdFormatter> collectionIdFormatterFactory)
        {
            _collectionIdFormatter = collectionIdFormatterFactory ?? throw new ArgumentNullException(nameof(collectionIdFormatterFactory));
        }

        public IEnumerable<ValidationResult> Validate()
        {
            if (string.IsNullOrWhiteSpace(DatabaseId))
                yield return this.Failure("DatabaseId", "must be specified");

            if (_collectionIdFormatter == null)
                yield return this.Failure("CollectionIdFormatter", "must be specified");
        }

        public void Register(ISagaRepositoryRegistrationConfigurator<TSaga> configurator)
        {
            if (_clientName == null)
                _registerClientFactory(configurator);

            configurator.TryAddSingleton(DatabaseContextFactory);
            configurator.RegisterSagaRepository<TSaga, DatabaseContext<TSaga>, SagaConsumeContextFactory<DatabaseContext<TSaga>, TSaga>,
                CosmosSagaRepositoryContextFactory<TSaga>>();
        }

        void RegisterNewtonsoftJsonClientFactory(ISagaRepositoryRegistrationConfigurator<TSaga> configurator)
        {
            configurator.TryAddSingleton<ICosmosClientFactory>(provider => new NewtonsoftJsonCosmosClientFactory(EndpointUri, Key));
        }

        void RegisterSystemTextJsonClientFactory(ISagaRepositoryRegistrationConfigurator<TSaga> configurator)
        {
            configurator.TryAddSingleton<ICosmosClientFactory>(provider => new SystemTextJsonCosmosClientFactory(EndpointUri, Key));
        }

        DatabaseContext<TSaga> DatabaseContextFactory(IServiceProvider provider)
        {
            var clientFactory = provider.GetRequiredService<ICosmosClientFactory>();
            var client = clientFactory.GetCosmosClient<TSaga>(_clientName);

            var collectionIdFormatter = _collectionIdFormatter(provider);
            var container = client.GetContainer(DatabaseId, collectionIdFormatter.Saga<TSaga>());

            return new CosmosDatabaseContext<TSaga>(container, _itemRequestOptions, _queryRequestOptions);
        }
    }
}

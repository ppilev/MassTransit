namespace MassTransit.RedisIntegration.Saga
{
    using System;
    using System.Threading.Tasks;
    using Context;
    using Logging;
    using MassTransit.Saga;

    public class RedisLoadedSagaRepositoryContext<TSaga, TMessage> :
       ConsumeContextScope<TMessage>,
       SagaRepositoryContext<TSaga, TMessage>,
       IAsyncDisposable
       where TSaga : class, ISagaVersion
       where TMessage : class
    {
        readonly ConsumeContext<TMessage> _consumeContext;
        private readonly SagaRepositoryContext<TSaga, TMessage> _loadedRepositoryContext;
        readonly DatabaseContext<TSaga> _context;
        readonly ISagaConsumeContextFactory<DatabaseContext<TSaga>, TSaga> _factory;

        public RedisLoadedSagaRepositoryContext(
            SagaRepositoryContext<TSaga, TMessage> loadedRepositoryContext,
            DatabaseContext<TSaga> context, ConsumeContext<TMessage> consumeContext,
            ISagaConsumeContextFactory<DatabaseContext<TSaga>, TSaga> factory)
            : base(consumeContext)
        {
            _loadedRepositoryContext = loadedRepositoryContext;
            _context = context;
            _consumeContext = consumeContext;
            _factory = factory;
        }

        public ValueTask DisposeAsync()
        {
            return _context.DisposeAsync();
        }

        public Task<SagaConsumeContext<TSaga, TMessage>> Add(TSaga instance)
        {
            return _factory.CreateSagaConsumeContext(_context, _consumeContext, instance, SagaConsumeContextMode.Add);
        }

        public async Task<SagaConsumeContext<TSaga, TMessage>> Insert(TSaga instance)
        {
            try
            {
                await _context.Insert(_consumeContext.SerializerContext, instance).ConfigureAwait(false);

                _consumeContext.LogInsert<TSaga, TMessage>(instance.CorrelationId);

                return await _factory.CreateSagaConsumeContext(_context, _consumeContext, instance, SagaConsumeContextMode.Insert).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _consumeContext.LogInsertFault<TSaga, TMessage>(ex, instance.CorrelationId);

                return default;
            }
        }

        public async Task<SagaConsumeContext<TSaga, TMessage>> Load(Guid correlationId)
        {
            return await _loadedRepositoryContext.Load(correlationId);
        }

        public Task Save(SagaConsumeContext<TSaga> context)
        {
            return _context.Add(context);
        }

        public Task Update(SagaConsumeContext<TSaga> context)
        {
            return _context.Update(context);
        }

        public Task Delete(SagaConsumeContext<TSaga> context)
        {
            return _context.Delete(context);
        }

        public Task Discard(SagaConsumeContext<TSaga> context)
        {
            return Task.CompletedTask;
        }

        public Task Undo(SagaConsumeContext<TSaga> context)
        {
            return Task.CompletedTask;
        }

        public Task<SagaConsumeContext<TSaga, T>> CreateSagaConsumeContext<T>(ConsumeContext<T> consumeContext, TSaga instance, SagaConsumeContextMode mode)
            where T : class
        {
            return _factory.CreateSagaConsumeContext(_context, consumeContext, instance, mode);
        }
    }
}

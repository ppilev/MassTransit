namespace MassTransit.RedisIntegration.Saga
{
    using System;
    using System.Threading.Tasks;


    public interface DatabaseContext<TSaga> :
        IAsyncDisposable
        where TSaga : class, ISagaVersion
    {
        Task Add(SagaConsumeContext<TSaga> context);

        Task Insert(IObjectDeserializer deserializer, TSaga instance);

        Task<TSaga> Load(IObjectDeserializer deserializer, Guid correlationId);

        Task<TSaga> Load(IObjectDeserializer deserializer, string redisKey);

        Task Update(SagaConsumeContext<TSaga> context);

        Task Delete(SagaConsumeContext<TSaga> context);
    }
}

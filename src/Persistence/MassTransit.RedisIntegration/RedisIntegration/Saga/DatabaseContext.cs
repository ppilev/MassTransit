namespace MassTransit.RedisIntegration.Saga
{
    using System;
    using System.Threading.Tasks;


    public interface DatabaseContext<TSaga> :
        IAsyncDisposable
        where TSaga : class, ISagaVersion
    {
        Task Add(SagaConsumeContext<TSaga> context);

        Task Insert(TSaga instance);

        Task<TSaga> Load(Guid correlationId);

        Task<TSaga> Load(string redisKey);

        Task Update(SagaConsumeContext<TSaga> context);

        Task Delete(SagaConsumeContext<TSaga> context);
    }
}

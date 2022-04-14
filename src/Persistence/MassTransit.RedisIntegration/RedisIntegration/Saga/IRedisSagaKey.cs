namespace MassTransit.RedisIntegration.Saga
{
    public interface IRedisSagaKey
    {
        public string Key { get; }
    }
}

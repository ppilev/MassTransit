
namespace MassTransit.RedisIntegration
{
    using MassTransit.RedisIntegration.Saga;
    using System;

    public static class RedisIntegrationExtensions
    {
        public static IEventCorrelationConfigurator<TSaga, TMessage> CorrelateByRedisKey<TSaga, TMessage>(
            this IEventCorrelationConfigurator<TSaga, TMessage> configurator,
            Func<ConsumeContext<TMessage>, string> selector)
            where TSaga : class, SagaStateMachineInstance, IRedisSagaKey
            where TMessage : class
        {
            return configurator.CorrelateBy(x => "", context => selector(context));
        }
    }
}

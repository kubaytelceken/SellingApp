using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.RabbitMQ
{
    internal class EventBusRabbitMQ : BaseEventBus
    {
        RabbitMQPersistentConnection persistentConnection;
        private readonly IConnectionFactory connectionFactory;
        private readonly IModel consumerChannel;
        public EventBusRabbitMQ(EventBusConfig config, IServiceProvider serviceProvider) : base(config, serviceProvider)
        {
            if (config.Connection != null)
            {
                var connJson = JsonConvert.SerializeObject(EventBusConfig.Connection, new JsonSerializerSettings()
                {
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                });
                connectionFactory = JsonConvert.DeserializeObject<ConnectionFactory>(connJson);
            }
            else
            {
                connectionFactory = new ConnectionFactory();
            }
            persistentConnection = new RabbitMQPersistentConnection(connectionFactory, config.ConnectionRetryCount);
            consumerChannel = CreateConsumerChannel();

            subscriptionManager.OnEventRemoved += SubscriptionManager_OnEventRemoved;
        }

        private void SubscriptionManager_OnEventRemoved(object? sender, string e)
        {
            var eventName = ProcessEventName(e);
            if(!persistentConnection.IsConnected)
            {
                persistentConnection.TryConnect();
            }
            consumerChannel.QueueUnbind(queue: eventName, exchange: EventBusConfig.DefaultTopicName, routingKey: eventName);
            if (subscriptionManager.IsEmpty)
            {
                consumerChannel.Close();
            }
        }

        public override void Publish(IntegrationEvent @event)
        {



           
        }



        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name; // example  : OrderCreatedIntegrationEvent
            eventName = ProcessEventName(eventName); // example : OrderCreated

            if (!subscriptionManager.HasSubscriptionForEvent(eventName))
            {
                if (!persistentConnection.IsConnected)
                {
                    persistentConnection.TryConnect();
                }

                consumerChannel.QueueDeclare(queue: GetSubName(eventName), durable: true, exclusive: false, autoDelete: false, arguments: null);

                consumerChannel.QueueBind(queue: GetSubName(eventName), exchange: EventBusConfig.DefaultTopicName, routingKey: eventName);
            }
            subscriptionManager.AddSubscription<T, TH>();

            StartBasicConsume(eventName);
        }

        public override void UnSubscribe<T, TH>()
        {
            subscriptionManager.RemoveSubscription<T, TH>();
        }


        private IModel CreateConsumerChannel()
        {
            if (!persistentConnection.IsConnected)
            {
                persistentConnection.TryConnect();
            }
            var channel = persistentConnection.CreateModel();
            channel.ExchangeDeclare(exchange: EventBusConfig.DefaultTopicName, type: "direct");
            return channel;
        }


        private void StartBasicConsume(string eventName)
        {
            if (consumerChannel != null)
            {
                var consumer = new EventingBasicConsumer(consumerChannel);
                consumer.Received += Consumer_Received;
                consumerChannel.BasicConsume(queue: GetSubName(eventName), autoAck: false, consumer: consumer);
            }
        }

        private async void Consumer_Received(object? sender, BasicDeliverEventArgs e)
        {
            var eventName = e.RoutingKey;
            eventName = ProcessEventName(eventName);

            var message = Encoding.UTF8.GetString(e.Body.Span);

            try
            {
                await ProcessEvent(eventName, message);
            }
            catch (Exception ex)
            {


            }
            consumerChannel.BasicAck(e.DeliveryTag, multiple: false);
        }


    }
}

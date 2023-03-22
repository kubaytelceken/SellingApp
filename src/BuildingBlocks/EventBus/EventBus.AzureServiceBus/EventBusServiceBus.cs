using EventBus.Base;
using EventBus.Base.Events;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.AzureServiceBus
{
    public class EventBusServiceBus : BaseEventBus
    {
        private ITopicClient _topicClient;
        private ManagementClient _managementClient;
        private ILogger _logger;

        public EventBusServiceBus(EventBusConfig config,IServiceProvider serviceProvider) : base(config,serviceProvider)
        {
            _logger = serviceProvider.GetService(typeof(ILogger<EventBusServiceBus>)) as ILogger<EventBusServiceBus>;
            _managementClient = new ManagementClient(config.EventBusConnectionString);
            _topicClient = createTopicClient();
        }

        private ITopicClient createTopicClient()
        {
            if(_topicClient == null || _topicClient.IsClosedOrClosing)
            {
                _topicClient = new TopicClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, RetryPolicy.Default);
            }

            //Ensure that topic already exists
            if (!_managementClient.TopicExistsAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult())
            {
               _managementClient.CreateTopicAsync(EventBusConfig.DefaultTopicName).GetAwaiter().GetResult();
            }

            return _topicClient;

        }
        public override void Publish(IntegrationEvent @event)
        {
            var eventName = @event.GetType().Name; // example  : OrderCreatedIntegrationEvent
            eventName = ProcessEventName(eventName); // example : OrderCreated

            var eventStr = JsonConvert.SerializeObject(@event);
            var bodyArr = Encoding.UTF8.GetBytes(eventStr);
            var message = new Message()
            {
                MessageId = Guid.NewGuid().ToString(),
                Body = bodyArr,
                Label = eventName
            };
            _topicClient.SendAsync(message).GetAwaiter().GetResult();
        }

        public override void Subscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            eventName = ProcessEventName(eventName);
            if (!subscriptionManager.HasSubscriptionForEvent(eventName))
            {
              var subsciptionClient =  CreateSubscriptionClientIfNotExists(eventName);
                RegisterSubscriptionClientMessageHandler(subsciptionClient);
            }

            _logger.LogInformation("Subscription to event {EventName} with {EventHandler}", eventName, typeof(TH).Name);
            subscriptionManager.AddSubscription<T, TH>();
        }

        private void RegisterSubscriptionClientMessageHandler(ISubscriptionClient subscriptionClient)
        {
            subscriptionClient.RegisterMessageHandler(async (message, token) =>
            {
                var eventName = $"{message.Label}";
                var messageData = Encoding.UTF8.GetString(message.Body);
                if (await ProcessEvent(ProcessEventName(eventName), messageData))
                {
                    await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                }
            }, new MessageHandlerOptions(ExceptionReceiveHandler) { MaxConcurrentCalls =10,AutoComplete = false });
        }

        private Task ExceptionReceiveHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            var ex = exceptionReceivedEventArgs.Exception;
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;

            _logger.LogError(ex, "ERROR handling message : {ExceptionMessage} - Context {@ExceptionContext}", ex.Message, context);
            return Task.CompletedTask;
        }
        public override void UnSubscribe<T, TH>()
        {
            var eventName = typeof(T).Name;
            try
            {
                var subscriptionClient = CreateSubscriptionClient(eventName);
                subscriptionClient.RemoveRuleAsync(eventName).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogWarning("The messaging entity {eventName} Could not be found", eventName);
               
            }

            _logger.LogInformation("Unsubscribing from event  {EventName}", eventName);
            subscriptionManager.RemoveSubscription<T, TH>();
        }

        private void RemoveDefaultRule(SubscriptionClient subscriptionClient)
        {
            try
            {
                subscriptionClient.RemoveRuleAsync(RuleDescription.DefaultRuleName).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {

                _logger.LogWarning("The messaging entity {DefaultRuleName} Could not be found.", RuleDescription.DefaultRuleName);
            }
        }

        private ISubscriptionClient CreateSubscriptionClientIfNotExists(string eventName)
        {
            var subClient = CreateSubscriptionClient(eventName);
            var exists = _managementClient.SubscriptionExistsAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
            if (!exists)
            {
                _managementClient.CreateSubscriptionAsync(EventBusConfig.DefaultTopicName, GetSubName(eventName)).GetAwaiter().GetResult();
                RemoveDefaultRule(subClient);
            }
            CreateRuleIfNotExists(ProcessEventName(eventName),subClient);
            return subClient;
        }

        private void CreateRuleIfNotExists(string eventName,ISubscriptionClient subscriptionClient)
        {
            bool ruleExits;
            try
            {
                var rule = _managementClient.GetRuleAsync(EventBusConfig.DefaultTopicName, eventName, eventName).GetAwaiter().GetResult();
                ruleExits = rule != null;

            }
            catch (Exception ex)
            {

                ruleExits = false;
            }

            if (!ruleExits)
            {
                subscriptionClient.AddRuleAsync(new RuleDescription
                {
                    Filter = new CorrelationFilter { Label = eventName },
                    Name = eventName
                }).GetAwaiter().GetResult();
            }
        }
        private SubscriptionClient CreateSubscriptionClient(string eventName)
        {
            return new SubscriptionClient(EventBusConfig.EventBusConnectionString, EventBusConfig.DefaultTopicName, GetSubName(eventName));
        }

        public override void Dispose()
        {
            base.Dispose();
            _topicClient.CloseAsync().GetAwaiter().GetResult();
            _managementClient.CloseAsync().GetAwaiter().GetResult();
            _topicClient = null;
            _managementClient = null;
        }
    }
}

using System;
using Microsoft.WindowsAzure.ServiceRuntime;
using SignalR.Infrastructure;
using SignalR.MessageBus;

namespace SignalR.WindowsAzureServiceBus
{
    public static class DependencyResolverExtensions
    {

        public static IDependencyResolver UseWindowsAzureServiceBus(this IDependencyResolver resolver,
                                                                    string serviceBusNamespace,
                                                                    string serviceBusAccount,
                                                                    string serviceBusAccountKey,
                                                                    string topicPathPrefix,
                                                                    int numberOfTopics)
        {
            var instanceId = Environment.MachineName + "_" + GetRoleInstanceNumber().ToString();

            return UseWindowsAzureServiceBus(resolver, 
                                             serviceBusNamespace, 
                                             serviceBusAccount, 
                                             serviceBusAccountKey, 
                                             topicPathPrefix, 
                                             numberOfTopics,
                                             instanceId);
        }

        public static IDependencyResolver UseWindowsAzureServiceBus(this IDependencyResolver resolver,
                                                                    string serviceBusNamespace,
                                                                    string serviceBusAccount,
                                                                    string serviceBusAccountKey,
                                                                    string topicPathPrefix,
                                                                    int numberOfTopics,
                                                                    string instanceId)
        {
            var bus = new Lazy<ServiceBusMessageBus>(() => new ServiceBusMessageBus(topicPathPrefix, 
                                                                                    numberOfTopics, 
                                                                                    serviceBusNamespace, 
                                                                                    serviceBusAccount, 
                                                                                    serviceBusAccountKey, 
                                                                                    instanceId));
            resolver.Register(typeof(IMessageBus), () => bus.Value);
            return resolver;
        }

        private static int GetRoleInstanceNumber()
        {
            var roleInstanceId = RoleEnvironment.CurrentRoleInstance.Id;
            var li1 = roleInstanceId.LastIndexOf(".");
            var li2 = roleInstanceId.LastIndexOf("_");
            var roleInstanceNo = roleInstanceId.Substring(Math.Max(li1, li2) + 1);
            return Int32.Parse(roleInstanceNo);
        }
    }
}

// 
//  (c) Microsoft Corporation. All rights reserved.
//  
namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using Microsoft.ServiceBus.Messaging;

    public partial class ServiceBusMessageBus
    {
        class SendAsyncResult : AsyncResult
        {
            readonly string clientId;
            readonly string eventKey;
            readonly ServiceBusMessageBus that;
            readonly object value;
            int retryCount = 0;

            public SendAsyncResult(ServiceBusMessageBus that, string eventKey, object value, string clientId, AsyncCallback callback, object state)
                : base(callback, state)
            {
                this.that = that;
                this.eventKey = eventKey;
                this.value = value;
                this.clientId = clientId;
                this.Run();
            }

            void Run()
            {
                var message = new BrokeredMessage();
                message.Properties.Add(EventKeysPropertyName, this.eventKey);
                message.Properties.Add(ValuePropertyName, this.value.ToString());
                var targetPartition = Math.Abs(this.clientId.GetHashCode())%this.that.NumberOfTopics;
                var client = this.that.topicClients[targetPartition];

                client.BeginSend(
                                 message,
                                 (a) =>
                                     {
                                         try
                                         {
                                             client.EndSend(a);
                                             this.Complete(false);
                                         }
                                         catch (Exception e)
                                         {
                                             Trace.TraceError("Error sending to topic '{0}' with {1}", client.Path, e);
                                             if (++this.retryCount <= 3)
                                             {
                                                 this.Run();
                                             }
                                             else
                                             {
                                                 this.Complete(false, e);
                                             }
                                         }
                                     },
                                 null);
            }

            public static void End(IAsyncResult ar)
            {
                End<SendAsyncResult>(ar);
            }
        }
    }
}
// 
//  (c) Microsoft Corporation. All rights reserved.
//  
namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Collections;
    using Microsoft.ServiceBus.Messaging;

    public partial class ServiceBusMessageBus
    {
        class HandleReceivedMessageAsyncResult : AsyncResult
        {
            readonly ServiceBusMessageBusCache cache;
            readonly BrokeredMessage message;
            IEnumerator enumerator;

            public HandleReceivedMessageAsyncResult(BrokeredMessage message, ServiceBusMessageBusCache cache, AsyncCallback callback, object state)
                : base(callback, state)
            {
                this.message = message;
                this.cache = cache;
                this.Run();
            }

            void Run()
            {
                try
                {
                    object eventKeysObject;
                    if (this.message.Properties.TryGetValue(EventKeysPropertyName, out eventKeysObject) &&
                        eventKeysObject is string)
                    {
                        var eventKeys = ((string) eventKeysObject).Split(',');
                        this.enumerator = eventKeys.GetEnumerator();
                        if (this.enumerator.MoveNext())
                        {
                            this.cache.BeginAdd(new Message(((string) this.enumerator.Current).Trim(), this.message.Properties[ValuePropertyName]),
                                                (ulong) this.message.SequenceNumber,
                                                this.DoneAdding,
                                                null);
                        }
                    }
                    else
                    {
                        this.Complete(false);
                    }
                }
                catch (Exception e)
                {
                    this.Complete(false, e);
                }
            }

            void DoneAdding(IAsyncResult ar)
            {
                try
                {
                    this.cache.EndAdd(ar);
                    if (this.enumerator.MoveNext())
                    {
                        this.cache.BeginAdd(new Message(((string) this.enumerator.Current).Trim(), this.message.Properties[ValuePropertyName]),
                                            (ulong) this.message.SequenceNumber,
                                            this.DoneAdding,
                                            null);
                    }
                    else
                    {
                        this.Complete(false);
                    }
                }
                catch (Exception e)
                {
                    this.Complete(false, e);
                }
            }

            public static void End(IAsyncResult ar)
            {
                End<HandleReceivedMessageAsyncResult>(ar);
            }
        }
    }
}
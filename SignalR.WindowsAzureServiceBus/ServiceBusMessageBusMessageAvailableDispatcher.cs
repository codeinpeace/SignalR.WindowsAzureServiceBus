// 
//  (c) Microsoft Corporation. All rights reserved.
//  
namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public partial class ServiceBusMessageBus
    {
        class MessageAvailableDispatcher
        {
            readonly Action messageAvailable;
            int done = 0;

            public MessageAvailableDispatcher(Action messageAvailable, IEnumerable<ServiceBusMessageBusCache> caches, IEnumerable<string> keys, CancellationToken cancellationToken)
            {
                this.messageAvailable = messageAvailable;
                foreach (var cache in caches)
                {
                    cache.RegisterMessageAvailableNotification(keys, this.MessageAvailableInternal, cancellationToken);
                }
            }

            void MessageAvailableInternal()
            {
                if (Interlocked.CompareExchange(ref this.done, 1, 0) == 0)
                {
                    this.messageAvailable();
                }
            }
        }
    }
}
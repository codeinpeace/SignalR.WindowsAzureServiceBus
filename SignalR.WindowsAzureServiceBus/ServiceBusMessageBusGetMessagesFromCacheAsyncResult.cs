// 
//  (c) Microsoft Corporation. All rights reserved.
//  
namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using SignalR.MessageBus;

    public partial class ServiceBusMessageBus
    {
        class GetMessagesFromCacheAsyncResult : TypedAsyncResult<MessageResult>
        {
            readonly CompositeCursor compositeCursor;
            readonly IEnumerable<string> keys;
            readonly int maxMessages;
            readonly CancellationToken cancellationToken;
            readonly ServiceBusMessageBus that;
            MessageAvailableDispatcher mad;

            public GetMessagesFromCacheAsyncResult(
                ServiceBusMessageBus that,
                IEnumerable<string> keys,
                CompositeCursor compositeCursor,
                int maxMessages,
                CancellationToken cancellationToken,
                AsyncCallback callback,
                object state)
                : base(callback, state)
            {
                this.that = that;
                this.keys = keys;
                this.compositeCursor = compositeCursor;
                this.maxMessages = maxMessages;
                this.cancellationToken = cancellationToken;
                this.mad = new MessageAvailableDispatcher(this.MessageAvailable, that.caches, keys, cancellationToken);
                this.cancellationToken.Register(Cancel);
            }

            void Cancel()
            {
                if (!this.IsCompleted)
                {
                    this.Complete(new MessageResult(new List<Message>(), compositeCursor.Cookie), false);
                }
            }

            void MessageAvailable()
            {
                var msgs = this.that.FetchAvailableMessagesFromCache(this.compositeCursor, this.keys, this.maxMessages);
                var result = new MessageResult(msgs, this.compositeCursor.Cookie);
                this.Complete(result, false);
            }

            public static MessageResult End(IAsyncResult ar)
            {
                return TypedAsyncResult<MessageResult>.End(ar);
            }
        }
    }
}
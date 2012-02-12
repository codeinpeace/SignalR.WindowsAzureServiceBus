// 
//  (c) Microsoft Corporation. All rights reserved.
//  
namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using SignalR.MessageBus;

    /// <summary>
    ///   This class implements signaling scale-out for SignalR through the Service Bus. 
    ///   The assumption for this class is that the message storage is external in some
    ///   storage facility such as SQL Azure or Azure Storage and that only the signals
    ///   are routed/distributed via Service Bus.
    /// </summary>
    public partial class ServiceBusMessageBus : IMessageBus, IDisposable
    {
        const string ValuePropertyName = "Value";
        const string EventKeysPropertyName = "EventKeys";

        static readonly Random rnd = new Random();
        readonly TimeSpan cacheTtl = TimeSpan.FromSeconds(30);
        readonly List<ServiceBusMessageBusCache> caches;
        readonly string instanceId; // the instance-id of this node in the scale-out fabric
        readonly List<MessagingFactory> messagingFactories; // factory
        readonly object openMutex = new object(); // lock object guarding the open state
        readonly string serviceBusAccount; // Service Bus account ("owner")
        readonly string serviceBusAccountKey; // Service Bus key
        readonly string serviceBusNamespace; // Service Bus namespace
        readonly List<SubscriptionClient> subscriptionClients;
        readonly List<TopicClient> topicClients;
        bool isOpen; // flag for whether we're 'open', i.e. have a valid mesaging factory
        NamespaceManager namespaceManager;

        public ServiceBusMessageBus(string topicPathPrefix, int numberOfTopics, string serviceBusNamespace, string serviceBusAccount, string serviceBusAccountKey, string instanceId)
        {
            this.TopicPathPrefix = topicPathPrefix;
            this.NumberOfTopics = numberOfTopics;
            this.messagingFactories = new List<MessagingFactory>();
            this.caches = new List<ServiceBusMessageBusCache>();
            this.topicClients = new List<TopicClient>();
            this.subscriptionClients = new List<SubscriptionClient>();
            this.instanceId = instanceId ?? Environment.MachineName;

            if (serviceBusNamespace != null)
            {
                this.serviceBusNamespace = serviceBusNamespace;
            }
            else
            {
                throw new ArgumentNullException("serviceBusNamespace");
            }
            if (serviceBusAccount != null)
            {
                this.serviceBusAccount = serviceBusAccount;
            }
            else
            {
                throw new ArgumentNullException("serviceBusAccount");
            }
            if (serviceBusAccountKey != null)
            {
                this.serviceBusAccountKey = serviceBusAccountKey;
            }
            else
            {
                throw new ArgumentNullException("serviceBusAccountKey");
            }
        }

        public int NumberOfTopics { get; set; }
        public string TopicPathPrefix { get; set; }

        public void Dispose()
        {
            this.Close();
            GC.SuppressFinalize(this);
        }


        public Task<MessageResult> GetMessages(IEnumerable<string> eventKeys, string id, CancellationToken cancellationToken)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(55);
            int maxMessages = 100;


            // let's make sure we've got the message pump into the caches running
            this.EnsureOpen();

            var compositeCursor = new CompositeCursor(id, this.NumberOfTopics);
            // grab what we can synchronously from the cache
            var messages = this.FetchAvailableMessagesFromCache(compositeCursor, eventKeys, maxMessages);

            if (messages.Count == 0)
            {
                // cache is drained for this client, so we'll have to wait for new messages 
                return new TaskFactory<MessageResult>().FromAsync(
                    (cb, s) => this.BeginGetMessagesFromCache(eventKeys, compositeCursor, maxMessages, cancellationToken, cb, s),
                    this.EndGetMessagesFromCache, null);
            }

            // got a batch of messages, let's return it as a synchronous result
            var r = new TaskCompletionSource<MessageResult>();
            if (cancellationToken.IsCancellationRequested)
            {
                r.SetCanceled();
            }
            else
            {
                r.SetResult(new MessageResult(messages, compositeCursor.Cookie));
            }
            return r.Task;
        }

        public Task Send(string connectionId, string eventKey, object value)
        {
            // makes sure we've got the topics set up; this will block for a bit if they're not
            this.EnsureOpen();

            // send
            return Task.Factory.FromAsync(this.BeginSendViaTopic, this.EndSendViaTopic, eventKey, value, connectionId, null);
        }

        IList<Message> FetchAvailableMessagesFromCache(CompositeCursor compositeCursor, IEnumerable<string> eventKeys, int maxMessages)
        {
            var messages = new List<Message>();
            bool gotAny;
            do
            {
                gotAny = false;
                for (var i = 0; i < this.NumberOfTopics; i++)
                {
                    if (compositeCursor.CursorInitFlags[i])
                    {
                        var hwmi = compositeCursor.Cursors[i];
                        Message msg;
                        if (this.caches[i].TryGetNext(eventKeys, ref hwmi, out msg))
                        {
                            gotAny = true;
                            messages.Add(msg);
                            compositeCursor.Cursors[i] = hwmi;
                            if (messages.Count >= maxMessages)
                            {
                                // have enough?
                                // cheat to get out of the outer loop 
                                gotAny = false;
                                // and GTFO
                                break;
                            }
                        }
                        else
                        {
                            // try does advance the cursor
                            compositeCursor.Cursors[i] = hwmi;
                        }
                    }
                    else
                    {
                        ushort cursor;

                        if (caches[i].TryGetTopOfSequenceCursor(out cursor))
                        {
                            compositeCursor.CursorInitFlags[i] = true;
                            compositeCursor.Cursors[i] = cursor;
                        }
                    }
                }
            }
            while (gotAny);
            messages.Sort((m1, m2) => m1.Created.CompareTo(m2.Created));
            return messages;
        }

        static int s = 0;

        void DoneReceivingFromSubscription(IAsyncResult ar)
        {
            var subscriptionClient = (SubscriptionClient)((object[])ar.AsyncState)[0];
            var topicNumber = (int)((object[])ar.AsyncState)[1];

            try
            {
                var message = subscriptionClient.EndReceive(ar);
                if (message != null)
                {
                    Interlocked.Increment(ref s);
                    this.BeginHandleReceivedMessage(message, topicNumber, this.DoneHandleReceivedMessage, new object[] { subscriptionClients[topicNumber], topicNumber });
                    return;
                }
            }
            catch (Exception e)
            {
                Trace.TraceError
                    ("Warning, failure receiving from subscription '{0}/{1}' with {2}", subscriptionClient.TopicPath, subscriptionClient.Name, e.ToString());
                Trace.TraceWarning(e.ToString());
            }

            try
            {
                subscriptionClient.BeginReceive(this.DoneReceivingFromSubscription, new object[] { subscriptionClients[topicNumber], topicNumber });
            }
            catch (Exception e)
            {
                Trace.TraceError
                    ("Error attempting to receive from subscription '{0}/{1}' with {2}", subscriptionClient.TopicPath, subscriptionClient.Name, e.ToString());
            }
        }

        void DoneHandleReceivedMessage(IAsyncResult ar)
        {
            var subscriptionClient = (SubscriptionClient)((object[])ar.AsyncState)[0];
            var topicNumber = (int)((object[])ar.AsyncState)[1];

            try
            {
                this.EndHandleReceivedMessage(ar);
            }
            catch (Exception e)
            {
                Trace.TraceError
                    ("Error handling received message from subscription '{0}/{1}' with {2}", subscriptionClient.TopicPath, subscriptionClient.Name, e.ToString());
            }

            try
            {
                subscriptionClient.BeginReceive(this.DoneReceivingFromSubscription, new object[] { subscriptionClients[topicNumber], topicNumber });
            }
            catch (Exception e)
            {
                Trace.TraceError
                    ("Error attempting to receive from subscription '{0}/{1}' with {2}", subscriptionClient.TopicPath, subscriptionClient.Name, e.ToString());
            }
        }

        IAsyncResult BeginGetMessagesFromCache(IEnumerable<string> eventKeys, CompositeCursor id, int maxMessages, CancellationToken cancellationToken, AsyncCallback callback, object state)
        {
            return new GetMessagesFromCacheAsyncResult(this, eventKeys, id, maxMessages, cancellationToken, callback, state);
        }

        MessageResult EndGetMessagesFromCache(IAsyncResult arg)
        {
            return GetMessagesFromCacheAsyncResult.End(arg);
        }

        IAsyncResult BeginHandleReceivedMessage(BrokeredMessage message, int topicId, AsyncCallback cb, object state)
        {
            return new HandleReceivedMessageAsyncResult(message, this.caches[topicId], cb, state);
        }

        void EndHandleReceivedMessage(IAsyncResult asyncResult)
        {
            HandleReceivedMessageAsyncResult.End(asyncResult);
        }

        IAsyncResult BeginSendViaTopic(string eventKey, object value, string clientId, AsyncCallback cb, object state)
        {
            return new SendAsyncResult(this, eventKey, value, clientId, cb, state);
        }

        void EndSendViaTopic(IAsyncResult ar)
        {
            SendAsyncResult.End(ar);
        }

        void EnsureOpen()
        {
            lock (this.openMutex)
            {
                if (!this.isOpen)
                {
                    var messagingServiceUri = ServiceBusEnvironment.CreateServiceUri("sb", this.serviceBusNamespace, string.Empty);
                    var managementServiceUri = ServiceBusEnvironment.CreateServiceUri("https", this.serviceBusNamespace, string.Empty);
                    var tokenProvider = TokenProvider.CreateSharedSecretTokenProvider(this.serviceBusAccount, this.serviceBusAccountKey);
                    this.namespaceManager = new NamespaceManager(managementServiceUri, tokenProvider);
                    for (var i = 0; i < this.NumberOfTopics; i++)
                    {
                        this.EnsurePartitionTopicSubscription(tokenProvider, messagingServiceUri, i);
                    }

                    for (var topicNumber = 0; topicNumber < this.NumberOfTopics; topicNumber++)
                    {
                        try
                        {
                            subscriptionClients[topicNumber].BeginReceive(this.DoneReceivingFromSubscription, new object[] { subscriptionClients[topicNumber], topicNumber });
                        }
                        catch (Exception e)
                        {
                            Trace.TraceError
                                ("Error attempting to receive from subscription '{0}/{1}' with {2}",
                                 subscriptionClients[topicNumber].TopicPath,
                                 subscriptionClients[topicNumber].Name,
                                 e.ToString());
                            throw;
                        }
                    }
                    this.isOpen = true;
                }
            }
        }

        void EnsurePartitionTopicSubscription(TokenProvider tokenProvider, Uri messagingServiceUri, int topicNumber)
        {
            var topicName = string.Format("{0}_{1}", this.TopicPathPrefix, topicNumber + 1);

            try
            {
                if (!this.namespaceManager.TopicExists(topicName))
                {
                    this.namespaceManager.CreateTopic(topicName);
                }
                this.messagingFactories.Add(MessagingFactory.Create(messagingServiceUri, tokenProvider));
                this.topicClients.Add(this.messagingFactories[topicNumber].CreateTopicClient(topicName));
                if (!this.namespaceManager.SubscriptionExists(topicName, this.instanceId))
                {
                    this.namespaceManager.CreateSubscription(topicName, this.instanceId);
                }
                var subscriptionClient = this.messagingFactories[topicNumber].CreateSubscriptionClient(topicName, this.instanceId, ReceiveMode.ReceiveAndDelete);
                subscriptionClient.PrefetchCount = 1;
                this.subscriptionClients.Add(subscriptionClient);

                this.caches.Add(new ServiceBusMessageBusCache(topicNumber, this.cacheTtl));
            }
            catch (Exception e)
            {
                Trace.TraceError("Error managing topic '{0}' with {1}", topicName, e.ToString());
                throw;
            }
        }


        void Close()
        {
            lock (this.openMutex)
            {
                if (this.isOpen)
                {
                    Exception lastException = null;
                    if (this.subscriptionClients != null)
                    {
                        foreach (var subscriptionClient in this.subscriptionClients)
                        {
                            try
                            {
                                subscriptionClient.Close();
                            }
                            catch (Exception e)
                            {
                                lastException = e;
                                Trace.TraceError
                                    ("Error closing subscription'{0}/{1}' with {2}", subscriptionClient.TopicPath, subscriptionClient.Name, e.ToString());
                                subscriptionClient.Abort();
                            }
                        }
                    }

                    if (this.topicClients != null)
                    {
                        foreach (var topicClient in this.topicClients)
                        {
                            try
                            {
                                topicClient.Close();
                            }
                            catch (Exception e)
                            {
                                lastException = e;
                                Trace.TraceError("Error closing topic'{0}' with {1}", topicClient.Path, e.ToString());
                                topicClient.Abort();
                            }
                        }
                    }

                    if (this.messagingFactories != null)
                    {
                        foreach (var messagingFactory in this.messagingFactories)
                        {
                            try
                            {
                                messagingFactory.Close();
                            }
                            catch (Exception e)
                            {
                                lastException = e;
                                Trace.TraceError("Error closing messaging factory {0}", e.ToString());
                                messagingFactory.Abort();
                            }
                        }
                    }
                    this.isOpen = false;

                    if (lastException != null)
                    {
                        throw new InvalidOperationException("Error closing signal bus", lastException);
                    }
                }
            }

        }

        class CompositeCursor
        {
            const int PreambleLength = sizeof(byte) + sizeof(ushort);
            readonly int capacity;
            public ushort[] Cursors { get; set; }
            public bool[] CursorInitFlags { get; set; }

            public CompositeCursor(string cookie, int capacity)
            {
                this.capacity = capacity;
                SetCookieInternal(cookie);
            }

            void SetCookieInternal(string cookie)
            {
                this.Cursors = new ushort[capacity];
                this.CursorInitFlags = new bool[capacity];
                if (!string.IsNullOrWhiteSpace(cookie))
                {
                    try
                    {
                        var cookieData = Convert.FromBase64String(Uri.UnescapeDataString(cookie));
                        if (cookieData.Length > PreambleLength)
                        {
                            // check the leading checksum first
                            byte chksum = cookieData[0];
                            byte cchksum = 0xff;
                            for (int i = 1; i < cookieData.Length; i++)
                            {
                                cchksum = (byte)(cchksum ^ cookieData[i]);
                            }
                            if (cchksum != chksum)
                            {
                                // not a valid cookie
                                return;
                            }
                            // decode the init marks bitfield
                            int storedCapacity = BitConverter.ToUInt16(cookieData, 1);
                            int initMarkBytes = (storedCapacity / 8) + 1;
                            if (cookieData.Length - PreambleLength - initMarkBytes > 0)
                            {
                                for (int i = 0, c = 0; i < initMarkBytes && c < storedCapacity && c < capacity; i++)
                                {
                                    for (int j = 0; j < 8 && c < storedCapacity && c < capacity; j++, c++)
                                    {
                                        var bitmask = (byte)(1 << j);
                                        this.CursorInitFlags[c] = ((cookieData[2 + initMarkBytes] & bitmask) == bitmask);
                                    }
                                }
                                // decode the cursors
                                for (int k = 0, j = PreambleLength + initMarkBytes; j + 1 < cookieData.Length && k < capacity && k < storedCapacity; j += sizeof(ushort), k++)
                                {
                                    this.Cursors[k] = (ushort)BitConverter.ToUInt16(cookieData, j);
                                }
                            }
                            else
                            {
                                // not a valid cookie
                                return;
                            }
                        }
                        else
                        {
                            // not a valid cookie
                            return;
                        }
                    }
                    catch (FormatException)
                    {
                        // bad cookies are treated as if we had no cookie
                    }
                }
            }

            string GetCookieInternal()
            {
                var cookieData = new byte[PreambleLength + ((capacity / 8) + 1) + capacity * sizeof(ushort)];
                // store the capacity
                Array.ConstrainedCopy(BitConverter.GetBytes(capacity), 0, cookieData, 1, sizeof(ushort));
                // store the marks
                for (int k = 0; k < capacity; k++)
                {
                    cookieData[PreambleLength + (k / 8)] = (byte)(cookieData[PreambleLength + (k / 8)] | ((this.CursorInitFlags[k] ? 1 : 0) << (k % 8)));
                    Array.ConstrainedCopy(BitConverter.GetBytes(this.Cursors[k]), 0, cookieData, PreambleLength + ((capacity / 8) + 1 + k * sizeof(ushort)), sizeof(ushort));
                }
                byte cchksum = 0xff;
                for (int i = 1; i < cookieData.Length; i++)
                {
                    cchksum = (byte)(cchksum ^ cookieData[i]);
                }
                cookieData[0] = cchksum;
                return Uri.EscapeDataString(Convert.ToBase64String(cookieData));
            }

            public string Cookie
            {
                get
                {
                    return this.GetCookieInternal();
                }
                set
                {
                    this.SetCookieInternal(value);
                }
            }
        }
    }
}
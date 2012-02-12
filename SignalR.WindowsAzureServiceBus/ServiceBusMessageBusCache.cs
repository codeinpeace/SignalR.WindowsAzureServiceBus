// 
//  (c) Microsoft Corporation. All rights reserved.
//  
namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using SignalR.MessageBus;

    public partial class ServiceBusMessageBus
    {
        class ServiceBusMessageBusCache
        {
            const int CapacityLimit = 20000;
            readonly SortedList<ulong, CachedMessage> messageIndex;
            readonly InputQueue<CachedMessage> pendingAdds;
            readonly TimeSpan purgeInterval = TimeSpan.FromMilliseconds(500);
            readonly ReaderWriterLockSlim rwLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            readonly int cacheId;
            readonly TimeSpan ttl;
            DateTime lastPurge = DateTime.MinValue;
            InputQueue<NotificationRegistration> pendingNotifications;

            public ServiceBusMessageBusCache(int cacheId, TimeSpan ttl)
            {
                this.cacheId = cacheId;
                this.ttl = ttl;
                this.messageIndex = new SortedList<ulong, CachedMessage>();
                this.pendingAdds = new InputQueue<CachedMessage>();
                this.pendingNotifications = new InputQueue<NotificationRegistration>();
            }

            public TimeSpan Ttl { get { return this.ttl; } }

            public IAsyncResult BeginAdd(Message msg, ulong id, AsyncCallback cb, object state)
            {
                try
                {
                    this.rwLock.EnterWriteLock();
                    this.PurgeExpired();

                    var cmsg = new CachedMessage(msg.SignalKey, msg.Value, msg.Created, id);
                    if (this.messageIndex.Count + 1 < CapacityLimit)
                    {
                        // add and complete
                        this.AddToCache(cmsg);
                        return new CompletedAsyncResult(cb, state);
                    }
                    else
                    {
                        // move the 'Add' operation to pending until we've got room to 
                        // add the item and complete once the item has been dequeued
                        // calling Complete on the AR
                        var addAsyncResult = new AddAsyncResult(cb, state);
                        this.pendingAdds.EnqueueAndDispatch(cmsg, addAsyncResult.Complete);
                        return addAsyncResult;
                    }
                }
                finally
                {
                    this.rwLock.ExitWriteLock();
                }
            }

            public void EndAdd(IAsyncResult ar)
            {
                if (ar is CompletedAsyncResult)
                {
                    CompletedAsyncResult.End(ar);
                }
                else
                {
                    AddAsyncResult.End(ar);
                }
            }

            void PurgeExpired()
            {
                var now = DateTime.UtcNow;
                if (this.pendingAdds.PendingCount > 0 ||
                    now > this.lastPurge + this.purgeInterval)
                {
                    try
                    {
                        this.rwLock.EnterWriteLock();
                        this.lastPurge = now;
                        for (int i = 0; i < messageIndex.Count; i++)
                        {
                            if (messageIndex.Values[i].Expired)
                            {
                                this.messageIndex.Remove(messageIndex.Values[i].Id);
                                i--;
                            }
                        }
                    }
                    finally
                    {
                        this.rwLock.ExitWriteLock();
                    }

                    this.UnblockPending();
                }
            }

            void UnblockPending()
            {
                try
                {
                    this.rwLock.EnterUpgradeableReadLock();
                    while (this.messageIndex.Count + 1 < CapacityLimit)
                    {
                        CachedMessage cmsg;
                        if (this.pendingAdds.Dequeue(TimeSpan.Zero, out cmsg))
                        {
                            this.AddToCache(cmsg);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                finally
                {
                    this.rwLock.ExitUpgradeableReadLock();
                }
            }

            void AddToCache(CachedMessage msg)
            {
                try
                {
                    this.rwLock.EnterWriteLock();
                    this.messageIndex.Add(msg.Id, msg);
                }
                finally
                {
                    this.rwLock.ExitWriteLock();
                }

                // swap the pending gets queue so that we can drain the old 
                // one and re-enqueue what we can't service right now
                var copiedPendingNotifications = Interlocked.Exchange(ref this.pendingNotifications, new InputQueue<NotificationRegistration>());
                for (; ; )
                {
                    NotificationRegistration notificationRegistration;
                    if (copiedPendingNotifications.Dequeue(TimeSpan.Zero, out notificationRegistration))
                    {
                        if (!notificationRegistration.Cancelled)
                        {
                            if (notificationRegistration.EventKey.Equals(msg.SignalKey))
                            {
                                SimpleIOThreadScheduler.ScheduleCallback(
                                                                         (pg) => ((NotificationRegistration)pg).Callback(),
                                                                         (e) => true,
                                                                         notificationRegistration);
                            }
                            else
                            {
                                this.pendingNotifications.EnqueueWithoutDispatch(notificationRegistration, null);
                            }
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }

            public bool TryGetTopOfSequenceCursor(out ushort cursor)
            {
                cursor = 0;
                try
                {
                    this.rwLock.EnterReadLock();
                    if (this.messageIndex.Count > 0)
                    {
                        cursor = (ushort)(messageIndex.Keys[messageIndex.Count - 1] & 0xffff);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
                finally
                {
                    this.rwLock.ExitReadLock();
                }
            }

            public bool TryGetNext(IEnumerable<string> eventKeys, ref ushort hwmp, out Message msg)
            {
                this.PurgeExpired();

                msg = null;
                try
                {
                    this.rwLock.EnterReadLock();
                    if (this.messageIndex.Count > 0)
                    {
                        var hwm = (this.messageIndex.Keys[this.messageIndex.Count - 1] / 0x10000) * 0x10000 + hwmp;

                        // binary search to the right index
                        int i = 0, l = 0, r = this.messageIndex.Count - 1;
                        while (l <= r)
                        {
                            i = l + (r - l) / 2;
                            var v = this.messageIndex.Values[i].Id;
                            // check to see if value is equal to item in array
                            if (hwm == v)
                            {
                                break;
                            }
                            if (hwm < v)
                            {
                                r = i - 1;
                            }
                            else
                            {
                                l = i + 1;
                            }
                        }

                        if (i > l)
                        {
                            i = l;
                        }

                        // get the next message if there is one left
                        for (; i < this.messageIndex.Count; i++)
                        {
                            if (this.messageIndex.Values[i].Id <= hwm )
                            {
                                continue;
                            }
                            if (!eventKeys.Contains(this.messageIndex.Values[i].SignalKey))
                            {
                                hwmp = (ushort)(this.messageIndex.Values[i].Id & 0xffff);
                                continue;
                            }
                            var cmsg = this.messageIndex.Values[i];
                            msg = cmsg;
                            hwmp = (ushort)(cmsg.Id & 0xffff);
                            return true;
                        }
                    }
                }
                finally
                {
                    this.rwLock.ExitReadLock();
                }
                return false;
            }

            public void RegisterMessageAvailableNotification(IEnumerable<string> keys, Action messageAvailable, CancellationToken cancellationToken)
            {
                foreach (var key in keys)
                {
                    this.pendingNotifications.EnqueueAndDispatch(new NotificationRegistration(cancellationToken) { Callback = messageAvailable, EventKey = key });
                }
            }


            class AddAsyncResult : AsyncResult
            {
                public AddAsyncResult(AsyncCallback callback, object state)
                    : base(callback, state)
                {
                }

                public void Complete()
                {
                    base.Complete(false);
                }

                public static void End(IAsyncResult ar)
                {
                    End<AddAsyncResult>(ar);
                }
            }

            class NotificationRegistration
            {
                readonly CancellationToken cancellationToken;

                public NotificationRegistration(CancellationToken cancellationToken)
                {
                    this.cancellationToken = cancellationToken;
                }

                public bool Cancelled
                {
                    get { return cancellationToken.IsCancellationRequested; }
                }

                public string EventKey { get; set; }
                public Action Callback { get; set; }
            }
        }
    }
}
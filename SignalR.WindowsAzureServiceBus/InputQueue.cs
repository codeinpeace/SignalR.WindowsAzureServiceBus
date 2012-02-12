// 
//  (c) Microsoft Corporation. All rights reserved.
//  
namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.ServiceModel;
    using System.Threading;

    // ItemDequeuedCallback is called as an item is dequeued from the InputQueue.  The 
    // InputQueue lock is not held during the callback.  However, the user code will
    // not be notified of the item being available until the callback returns.  If you
    // are not sure if the callback will block for a long time, then first call 
    // IOThreadScheduler.ScheduleCallback to get to a "safe" thread.
    public delegate void ItemDequeuedCallback();

    /// <summary>
    ///   Handles asynchronous interactions between producers and consumers. 
    ///   Producers can dispatch available data to the input queue, 
    ///   where it will be dispatched to a waiting consumer or stored until a
    ///   consumer becomes available. Consumers can synchronously or asynchronously
    ///   request data from the queue, which will be returned when data becomes
    ///   available.
    /// </summary>
    /// <typeparam name = "T">The concrete type of the consumer objects that are waiting for data.</typeparam>
    public class InputQueue<T> : IDisposable where T : class
    {
        //Stores items that are waiting to be consumed.
        static WaitCallback completeOutstandingReadersCallback;
        static WaitCallback completeWaitersFalseCallback;
        static WaitCallback completeWaitersTrueCallback;
        static WaitCallback onDispatchCallback;
        static WaitCallback onInvokeDequeuedCallback;
        readonly ItemQueue<T> itemQueue;

        //Each IQueueReader represents some consumer that is waiting for
        //items to appear in the queue. The readerQueue stores them
        //in an ordered list so consumers get serviced in a FIFO manner.
        readonly Queue<IQueueReader> readerQueue;

        //Each IQueueWaiter represents some waiter that is waiting for
        //items to appear in the queue.  When any item appears, all
        //waiters are signalled.
        readonly List<IQueueWaiter> waiterList;

        //Represents the current state of the InputQueue
        //as it transitions through its lifecycle.
        QueueState queueState;

        public InputQueue()
        {
            this.itemQueue = new ItemQueue<T>();
            this.readerQueue = new Queue<IQueueReader>();
            this.waiterList = new List<IQueueWaiter>();
            this.queueState = QueueState.Open;
        }

        public int PendingCount
        {
            get
            {
                lock (this.ThisLock)
                {
                    return this.itemQueue.ItemCount;
                }
            }
        }

        object ThisLock { get { return this.itemQueue; } }

        public void Dispose()
        {
            this.Dispose(true);

            GC.SuppressFinalize(this);
        }

        public IAsyncResult BeginDequeue(TimeSpan timeout, AsyncCallback callback, object state)
        {
            var item = default(Item<T>);

            lock (this.ThisLock)
            {
                if (this.queueState == QueueState.Open)
                {
                    if (this.itemQueue.HasAvailableItem)
                    {
                        item = this.itemQueue.DequeueAvailableItem();
                    }
                    else
                    {
                        var reader = new AsyncQueueReader(this, timeout, callback, state);
                        this.readerQueue.Enqueue(reader);
                        return reader;
                    }
                }
                else if (this.queueState == QueueState.Shutdown)
                {
                    if (this.itemQueue.HasAvailableItem)
                    {
                        item = this.itemQueue.DequeueAvailableItem();
                    }
                    else if (this.itemQueue.HasAnyItem)
                    {
                        var reader = new AsyncQueueReader(this, timeout, callback, state);
                        this.readerQueue.Enqueue(reader);
                        return reader;
                    }
                }
            }

            InvokeDequeuedCallback(item.DequeuedCallback);
            return new TypedCompletedAsyncResult<T>(item.GetValue(), callback, state);
        }

        public IAsyncResult BeginWaitForItem(TimeSpan timeout, AsyncCallback callback, object state)
        {
            lock (this.ThisLock)
            {
                if (this.queueState == QueueState.Open)
                {
                    if (!this.itemQueue.HasAvailableItem)
                    {
                        var waiter = new AsyncQueueWaiter(timeout, callback, state);
                        this.waiterList.Add(waiter);
                        return waiter;
                    }
                }
                else if (this.queueState == QueueState.Shutdown)
                {
                    if (!this.itemQueue.HasAvailableItem && this.itemQueue.HasAnyItem)
                    {
                        var waiter = new AsyncQueueWaiter(timeout, callback, state);
                        this.waiterList.Add(waiter);
                        return waiter;
                    }
                }
            }

            return new TypedCompletedAsyncResult<bool>(true, callback, state);
        }

        static void CompleteOutstandingReadersCallback(object state)
        {
            var outstandingReaders = (IQueueReader[]) state;

            for (var i = 0; i < outstandingReaders.Length; i++)
            {
                outstandingReaders[i].Set(default(Item<T>));
            }
        }

        static void CompleteWaitersFalseCallback(object state)
        {
            CompleteWaiters(false, (IQueueWaiter[]) state);
        }

        static void CompleteWaitersTrueCallback(object state)
        {
            CompleteWaiters(true, (IQueueWaiter[]) state);
        }

        static void CompleteWaiters(bool itemAvailable, IQueueWaiter[] waiters)
        {
            for (var i = 0; i < waiters.Length; i++)
            {
                waiters[i].Set(itemAvailable);
            }
        }

        static void CompleteWaitersLater(bool itemAvailable, IQueueWaiter[] waiters)
        {
            if (itemAvailable)
            {
                if (completeWaitersTrueCallback == null)
                    completeWaitersTrueCallback = new WaitCallback(CompleteWaitersTrueCallback);

                ThreadPool.QueueUserWorkItem(completeWaitersTrueCallback, waiters);
            }
            else
            {
                if (completeWaitersFalseCallback == null)
                    completeWaitersFalseCallback = new WaitCallback(CompleteWaitersFalseCallback);

                ThreadPool.QueueUserWorkItem(completeWaitersFalseCallback, waiters);
            }
        }

        void GetWaiters(out IQueueWaiter[] waiters)
        {
            if (this.waiterList.Count > 0)
            {
                waiters = this.waiterList.ToArray();
                this.waiterList.Clear();
            }
            else
            {
                waiters = null;
            }
        }

        public void Close()
        {
            ((IDisposable) this).Dispose();
        }

        public void Shutdown()
        {
            IQueueReader[] outstandingReaders = null;
            lock (this.ThisLock)
            {
                if (this.queueState == QueueState.Shutdown)
                    return;

                if (this.queueState == QueueState.Closed)
                    return;

                this.queueState = QueueState.Shutdown;

                if (this.readerQueue.Count > 0 && this.itemQueue.ItemCount == 0)
                {
                    outstandingReaders = new IQueueReader[this.readerQueue.Count];
                    this.readerQueue.CopyTo(outstandingReaders, 0);
                    this.readerQueue.Clear();
                }
            }

            if (outstandingReaders != null)
            {
                for (var i = 0; i < outstandingReaders.Length; i++)
                {
                    outstandingReaders[i].Set(new Item<T>((Exception) null, null));
                }
            }
        }

        public T Dequeue(TimeSpan timeout)
        {
            T value;

            if (!this.Dequeue(timeout, out value))
            {
                throw new TimeoutException(string.Format("Dequeue timed out in {0}.", timeout));
            }

            return value;
        }

        public bool Dequeue(TimeSpan timeout, out T value)
        {
            WaitQueueReader reader = null;
            var item = new Item<T>();

            lock (this.ThisLock)
            {
                if (this.queueState == QueueState.Open)
                {
                    if (this.itemQueue.HasAvailableItem)
                    {
                        item = this.itemQueue.DequeueAvailableItem();
                    }
                    else
                    {
                        reader = new WaitQueueReader(this);
                        this.readerQueue.Enqueue(reader);
                    }
                }
                else if (this.queueState == QueueState.Shutdown)
                {
                    if (this.itemQueue.HasAvailableItem)
                    {
                        item = this.itemQueue.DequeueAvailableItem();
                    }
                    else if (this.itemQueue.HasAnyItem)
                    {
                        reader = new WaitQueueReader(this);
                        this.readerQueue.Enqueue(reader);
                    }
                    else
                    {
                        value = default(T);
                        return true;
                    }
                }
                else // queueState == QueueState.Closed
                {
                    value = default(T);
                    return true;
                }
            }

            if (reader != null)
            {
                return reader.Wait(timeout, out value);
            }
            else
            {
                InvokeDequeuedCallback(item.DequeuedCallback);
                value = item.GetValue();
                return true;
            }
        }

        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                var dispose = false;

                lock (this.ThisLock)
                {
                    if (this.queueState != QueueState.Closed)
                    {
                        this.queueState = QueueState.Closed;
                        dispose = true;
                    }
                }

                if (dispose)
                {
                    while (this.readerQueue.Count > 0)
                    {
                        var reader = this.readerQueue.Dequeue();
                        reader.Set(default(Item<T>));
                    }

                    while (this.itemQueue.HasAnyItem)
                    {
                        var item = this.itemQueue.DequeueAnyItem();
                        item.Dispose();
                        InvokeDequeuedCallback(item.DequeuedCallback);
                    }

                    this.itemQueue.Close();
                }
            }
        }

        public void Dispatch()
        {
            IQueueReader reader = null;
            var item = new Item<T>();
            IQueueReader[] outstandingReaders = null;
            IQueueWaiter[] waiters = null;
            var itemAvailable = true;

            lock (this.ThisLock)
            {
                itemAvailable = !((this.queueState == QueueState.Closed) || (this.queueState == QueueState.Shutdown));
                this.GetWaiters(out waiters);

                if (this.queueState != QueueState.Closed)
                {
                    this.itemQueue.MakePendingItemAvailable();

                    if (this.readerQueue.Count > 0)
                    {
                        item = this.itemQueue.DequeueAvailableItem();
                        reader = this.readerQueue.Dequeue();

                        if (this.queueState == QueueState.Shutdown && this.readerQueue.Count > 0 && this.itemQueue.ItemCount == 0)
                        {
                            outstandingReaders = new IQueueReader[this.readerQueue.Count];
                            this.readerQueue.CopyTo(outstandingReaders, 0);
                            this.readerQueue.Clear();

                            itemAvailable = false;
                        }
                    }
                }
            }

            if (outstandingReaders != null)
            {
                if (completeOutstandingReadersCallback == null)
                    completeOutstandingReadersCallback = new WaitCallback(CompleteOutstandingReadersCallback);

                ThreadPool.QueueUserWorkItem(completeOutstandingReadersCallback, outstandingReaders);
            }

            if (waiters != null)
            {
                CompleteWaitersLater(itemAvailable, waiters);
            }

            if (reader != null)
            {
                InvokeDequeuedCallback(item.DequeuedCallback);
                reader.Set(item);
            }
        }

        //Ends an asynchronous Dequeue operation.
        public T EndDequeue(IAsyncResult result)
        {
            T value;

            if (!this.EndDequeue(result, out value))
            {
                throw new TimeoutException("Asynchronous Dequeue operation timed out.");
            }

            return value;
        }

        public bool EndDequeue(IAsyncResult result, out T value)
        {
            var typedResult = result as TypedCompletedAsyncResult<T>;

            if (typedResult != null)
            {
                value = TypedCompletedAsyncResult<T>.End(result);
                return true;
            }

            return AsyncQueueReader.End(result, out value);
        }

        public bool EndWaitForItem(IAsyncResult result)
        {
            var typedResult = result as TypedCompletedAsyncResult<bool>;
            if (typedResult != null)
            {
                return TypedCompletedAsyncResult<bool>.End(result);
            }

            return AsyncQueueWaiter.End(result);
        }

        public void EnqueueAndDispatch(T item)
        {
            this.EnqueueAndDispatch(item, null);
        }

        public void EnqueueAndDispatch(T item, ItemDequeuedCallback dequeuedCallback)
        {
            EnqueueAndDispatch(item, dequeuedCallback, true);
        }

        public void EnqueueAndDispatch(Exception exception, ItemDequeuedCallback dequeuedCallback, bool canDispatchOnThisThread)
        {
            Debug.Assert(exception != null, "exception parameter should not be null");
            this.EnqueueAndDispatch(new Item<T>(exception, dequeuedCallback), canDispatchOnThisThread);
        }

        public void EnqueueAndDispatch(T item, ItemDequeuedCallback dequeuedCallback, bool canDispatchOnThisThread)
        {
            Debug.Assert(item != null, "item parameter should not be null");
            this.EnqueueAndDispatch(new Item<T>(item, dequeuedCallback), canDispatchOnThisThread);
        }

        void EnqueueAndDispatch(Item<T> item, bool canDispatchOnThisThread)
        {
            var disposeItem = false;
            IQueueReader reader = null;
            var dispatchLater = false;
            IQueueWaiter[] waiters = null;
            var itemAvailable = true;

            try
            {
                Monitor.Enter(this.ThisLock);

                itemAvailable = !((this.queueState == QueueState.Closed) || (this.queueState == QueueState.Shutdown));
                this.GetWaiters(out waiters);

                if (this.queueState == QueueState.Open)
                {
                    if (canDispatchOnThisThread)
                    {
                        if (this.readerQueue.Count == 0)
                        {
                            try
                            {
                                Monitor.Exit(this.ThisLock);
                                this.itemQueue.AcquireThrottle();
                            }
                            finally
                            {
                                Monitor.Enter(this.ThisLock);
                            }
                            this.itemQueue.EnqueueAvailableItem(item);
                        }
                        else
                        {
                            reader = this.readerQueue.Dequeue();
                        }
                    }
                    else
                    {
                        if (this.readerQueue.Count == 0)
                        {
                            try
                            {
                                Monitor.Exit(this.ThisLock);
                                this.itemQueue.AcquireThrottle();
                            }
                            finally
                            {
                                Monitor.Enter(this.ThisLock);
                            }
                            this.itemQueue.EnqueueAvailableItem(item);
                        }
                        else
                        {
                            try
                            {
                                Monitor.Exit(this.ThisLock);
                                this.itemQueue.AcquireThrottle();
                            }
                            finally
                            {
                                Monitor.Enter(this.ThisLock);
                            }
                            this.itemQueue.EnqueuePendingItem(item);
                            dispatchLater = true;
                        }
                    }
                }
                else // queueState == QueueState.Closed || queueState == QueueState.Shutdown
                {
                    disposeItem = true;
                }
            }
            finally
            {
                Monitor.Exit(this.ThisLock);
            }

            if (waiters != null)
            {
                if (canDispatchOnThisThread)
                {
                    CompleteWaiters(itemAvailable, waiters);
                }
                else
                {
                    CompleteWaitersLater(itemAvailable, waiters);
                }
            }

            if (reader != null)
            {
                InvokeDequeuedCallback(item.DequeuedCallback);
                reader.Set(item);
            }

            if (dispatchLater)
            {
                if (onDispatchCallback == null)
                {
                    onDispatchCallback = new WaitCallback(OnDispatchCallback);
                }

                ThreadPool.QueueUserWorkItem(onDispatchCallback, this);
            }
            else if (disposeItem)
            {
                InvokeDequeuedCallback(item.DequeuedCallback);
                item.Dispose();
            }
        }

        public bool EnqueueWithoutDispatch(T item, ItemDequeuedCallback dequeuedCallback)
        {
            Debug.Assert(item != null, "EnqueueWithoutDispatch: item parameter should not be null");
            return this.EnqueueWithoutDispatch(new Item<T>(item, dequeuedCallback));
        }

        public bool EnqueueWithoutDispatch(Exception exception, ItemDequeuedCallback dequeuedCallback)
        {
            Debug.Assert(exception != null, "EnqueueWithoutDispatch: exception parameter should not be null");
            return this.EnqueueWithoutDispatch(new Item<T>(exception, dequeuedCallback));
        }

        // This will not block, however, Dispatch() must be called later if this function
        // returns true.
        bool EnqueueWithoutDispatch(Item<T> item)
        {
            try
            {
                Monitor.Enter(this.ThisLock);
                // Open
                if (this.queueState != QueueState.Closed && this.queueState != QueueState.Shutdown)
                {
                    if (this.readerQueue.Count == 0)
                    {
                        try
                        {
                            Monitor.Exit(this.ThisLock);
                            this.itemQueue.AcquireThrottle();
                        }
                        finally
                        {
                            Monitor.Enter(this.ThisLock);
                        }
                        this.itemQueue.EnqueueAvailableItem(item);
                        return false;
                    }
                    else
                    {
                        try
                        {
                            Monitor.Exit(this.ThisLock);
                            this.itemQueue.AcquireThrottle();
                        }
                        finally
                        {
                            Monitor.Enter(this.ThisLock);
                        }
                        this.itemQueue.EnqueuePendingItem(item);
                        return true;
                    }
                }
            }
            finally
            {
                Monitor.Exit(this.ThisLock);
            }

            item.Dispose();
            InvokeDequeuedCallbackLater(item.DequeuedCallback);
            return false;
        }

        static void OnDispatchCallback(object state)
        {
            ((InputQueue<T>) state).Dispatch();
        }

        static void InvokeDequeuedCallbackLater(ItemDequeuedCallback dequeuedCallback)
        {
            if (dequeuedCallback != null)
            {
                if (onInvokeDequeuedCallback == null)
                {
                    onInvokeDequeuedCallback = OnInvokeDequeuedCallback;
                }

                ThreadPool.QueueUserWorkItem(onInvokeDequeuedCallback, dequeuedCallback);
            }
        }

        static void InvokeDequeuedCallback(ItemDequeuedCallback dequeuedCallback)
        {
            if (dequeuedCallback != null)
            {
                dequeuedCallback();
            }
        }

        static void OnInvokeDequeuedCallback(object state)
        {
            var dequeuedCallback = (ItemDequeuedCallback) state;
            dequeuedCallback();
        }

        bool RemoveReader(IQueueReader reader)
        {
            lock (this.ThisLock)
            {
                if (this.queueState == QueueState.Open || this.queueState == QueueState.Shutdown)
                {
                    var removed = false;

                    for (var i = this.readerQueue.Count; i > 0; i--)
                    {
                        var temp = this.readerQueue.Dequeue();
                        if (ReferenceEquals(temp, reader))
                        {
                            removed = true;
                        }
                        else
                        {
                            this.readerQueue.Enqueue(temp);
                        }
                    }

                    return removed;
                }
            }

            return false;
        }

        public bool WaitForItem(TimeSpan timeout)
        {
            WaitQueueWaiter waiter = null;
            var itemAvailable = false;

            lock (this.ThisLock)
            {
                if (this.queueState == QueueState.Open)
                {
                    if (this.itemQueue.HasAvailableItem)
                    {
                        itemAvailable = true;
                    }
                    else
                    {
                        waiter = new WaitQueueWaiter();
                        this.waiterList.Add(waiter);
                    }
                }
                else if (this.queueState == QueueState.Shutdown)
                {
                    if (this.itemQueue.HasAvailableItem)
                    {
                        itemAvailable = true;
                    }
                    else if (this.itemQueue.HasAnyItem)
                    {
                        waiter = new WaitQueueWaiter();
                        this.waiterList.Add(waiter);
                    }
                    else
                    {
                        return false;
                    }
                }
                else // queueState == QueueState.Closed
                {
                    return true;
                }
            }

            if (waiter != null)
            {
                return waiter.Wait(timeout);
            }
            else
            {
                return itemAvailable;
            }
        }

        class AsyncQueueReader : AsyncResult, IQueueReader
        {
            static readonly TimerCallback timerCallback = new TimerCallback(TimerCallback);

            readonly InputQueue<T> inputQueue;
            readonly Timer timer;
            bool expired;
            T item;

            public AsyncQueueReader(InputQueue<T> inputQueue, TimeSpan timeout, AsyncCallback callback, object state)
                : base(callback, state)
            {
                this.inputQueue = inputQueue;
                if (timeout != TimeSpan.MaxValue)
                {
                    this.timer = new Timer(timerCallback, this, timeout, TimeSpan.FromMilliseconds(-1));
                }
            }

            public void Set(Item<T> item)
            {
                this.item = item.Value;
                if (this.timer != null)
                {
                    this.timer.Change(-1, -1);
                }
                this.Complete(false, item.Exception);
            }

            public static bool End(IAsyncResult result, out T value)
            {
                var readerResult = End<AsyncQueueReader>(result);

                if (readerResult.expired)
                {
                    value = default(T);
                    return false;
                }
                else
                {
                    value = readerResult.item;
                    return true;
                }
            }

            static void TimerCallback(object state)
            {
                var thisPtr = (AsyncQueueReader) state;
                if (thisPtr.inputQueue.RemoveReader(thisPtr))
                {
                    thisPtr.expired = true;
                    thisPtr.Complete(false);
                }
            }
        }

        class AsyncQueueWaiter : AsyncResult, IQueueWaiter
        {
            static readonly TimerCallback timerCallback = new TimerCallback(TimerCallback);
            readonly object thisLock = new object();
            readonly Timer timer;
            bool itemAvailable;

            public AsyncQueueWaiter(TimeSpan timeout, AsyncCallback callback, object state)
                : base(callback, state)
            {
                if (timeout != TimeSpan.MaxValue)
                {
                    this.timer = new Timer(timerCallback, this, timeout, TimeSpan.FromMilliseconds(-1));
                }
            }

            object ThisLock { get { return this.thisLock; } }

            public void Set(bool itemAvailable)
            {
                bool timely;

                lock (this.ThisLock)
                {
                    timely = (this.timer == null) || this.timer.Change(-1, -1);
                    this.itemAvailable = itemAvailable;
                }

                if (timely)
                {
                    this.Complete(false);
                }
            }

            public static bool End(IAsyncResult result)
            {
                var waiterResult = End<AsyncQueueWaiter>(result);
                return waiterResult.itemAvailable;
            }

            static void TimerCallback(object state)
            {
                var thisPtr = (AsyncQueueWaiter) state;
                thisPtr.Complete(false);
            }
        }

        interface IQueueReader
        {
            void Set(Item<T> item);
        }

        interface IQueueWaiter
        {
            void Set(bool itemAvailable);
        }

        enum QueueState
        {
            Open,
            Shutdown,
            Closed
        }

        class WaitQueueReader : IQueueReader
        {
            readonly InputQueue<T> inputQueue;
            readonly object thisLock = new object();
            readonly ManualResetEvent waitEvent;
            Exception exception;
            T item;

            public WaitQueueReader(InputQueue<T> inputQueue)
            {
                this.inputQueue = inputQueue;
                this.waitEvent = new ManualResetEvent(false);
            }

            object ThisLock { get { return this.thisLock; } }

            public void Set(Item<T> item)
            {
                lock (this.ThisLock)
                {
                    Debug.Assert(this.item == null, "InputQueue.WaitQueueReader.Set: (this.item == null)");
                    Debug.Assert(this.exception == null, "InputQueue.WaitQueueReader.Set: (this.exception == null)");

                    this.exception = item.Exception;
                    this.item = item.Value;
                    this.waitEvent.Set();
                }
            }

            public bool Wait(TimeSpan timeout, out T value)
            {
                var isSafeToClose = false;
                try
                {
                    if (timeout == TimeSpan.MaxValue)
                    {
                        this.waitEvent.WaitOne();
                    }
                    else if (!this.waitEvent.WaitOne(timeout, false))
                    {
                        if (this.inputQueue.RemoveReader(this))
                        {
                            value = default(T);
                            isSafeToClose = true;
                            return false;
                        }
                        else
                        {
                            this.waitEvent.WaitOne();
                        }
                    }

                    isSafeToClose = true;
                }
                finally
                {
                    if (isSafeToClose)
                    {
                        this.waitEvent.Close();
                    }
                }

                value = this.item;
                return true;
            }
        }


        class WaitQueueWaiter : IQueueWaiter
        {
            readonly object thisLock = new object();
            readonly ManualResetEvent waitEvent;
            bool itemAvailable;

            public WaitQueueWaiter()
            {
                this.waitEvent = new ManualResetEvent(false);
            }

            object ThisLock { get { return this.thisLock; } }

            public void Set(bool itemAvailable)
            {
                lock (this.ThisLock)
                {
                    this.itemAvailable = itemAvailable;
                    this.waitEvent.Set();
                }
            }

            public bool Wait(TimeSpan timeout)
            {
                if (timeout == TimeSpan.MaxValue)
                {
                    this.waitEvent.WaitOne();
                }
                else if (!this.waitEvent.WaitOne(timeout, false))
                {
                    return false;
                }

                return this.itemAvailable;
            }
        }
    }

    public struct Item<T> where T : class
    {
        readonly ItemDequeuedCallback dequeuedCallback;
        readonly Exception exception;
        readonly T value;

        public Item(T value, ItemDequeuedCallback dequeuedCallback)
            : this(value, null, dequeuedCallback)
        {
        }

        public Item(Exception exception, ItemDequeuedCallback dequeuedCallback)
            : this(null, exception, dequeuedCallback)
        {
        }

        Item(T value, Exception exception, ItemDequeuedCallback dequeuedCallback)
        {
            this.value = value;
            this.exception = exception;
            this.dequeuedCallback = dequeuedCallback;
        }

        public Exception Exception { get { return this.exception; } }

        public T Value { get { return this.value; } }

        public ItemDequeuedCallback DequeuedCallback { get { return this.dequeuedCallback; } }

        public void Dispose()
        {
            if (this.value != null)
            {
                if (this.value is IDisposable)
                {
                    ((IDisposable) this.value).Dispose();
                }
                else if (this.value is ICommunicationObject)
                {
                    ((ICommunicationObject) this.value).Abort();
                }
            }
        }

        public T GetValue()
        {
            if (this.exception != null)
            {
                throw this.exception;
            }

            return this.value;
        }
    }

    public class ItemQueue<T> where T : class
    {
        int head;
        Item<T>[] items;
        int pendingCount;
        int totalCount;

        public ItemQueue()
        {
            this.items = new Item<T>[1];
        }

        public bool HasAvailableItem { get { return this.totalCount > this.pendingCount; } }

        public bool HasAnyItem { get { return this.totalCount > 0; } }

        public int ItemCount { get { return this.totalCount; } }

        public virtual void Close()
        {
        }

        public Item<T> DequeueAvailableItem()
        {
            if (this.totalCount == this.pendingCount)
            {
                Debug.Assert(false, "ItemQueue does not contain any available items");
                throw new Exception("Internal Error");
            }
            return this.DequeueItemCore();
        }

        public Item<T> DequeueAnyItem()
        {
            if (this.pendingCount == this.totalCount)
                this.pendingCount--;
            return this.DequeueItemCore();
        }

        internal virtual void EnqueueItemCore(Item<T> item)
        {
            if (this.totalCount == this.items.Length)
            {
                var newItems = new Item<T>[this.items.Length*2];
                for (var i = 0; i < this.totalCount; i++)
                    newItems[i] = this.items[(this.head + i)%this.items.Length];
                this.head = 0;
                this.items = newItems;
            }
            var tail = (this.head + this.totalCount)%this.items.Length;
            this.items[tail] = item;
            this.totalCount++;
        }

        internal virtual Item<T> DequeueItemCore()
        {
            if (this.totalCount == 0)
            {
                Debug.Assert(false, "ItemQueue does not contain any items");
                throw new Exception("Internal Error");
            }
            var item = this.items[this.head];
            this.items[this.head] = new Item<T>();
            this.totalCount--;
            this.head = (this.head + 1)%this.items.Length;
            return item;
        }

        public void EnqueuePendingItem(Item<T> item)
        {
            this.EnqueueItemCore(item);
            this.pendingCount++;
        }

        public void EnqueueAvailableItem(Item<T> item)
        {
            this.EnqueueItemCore(item);
        }

        public void MakePendingItemAvailable()
        {
            if (this.pendingCount == 0)
            {
                Debug.Assert(false, "ItemQueue does not contain any pending items");
                throw new Exception("Internal Error");
            }
            this.pendingCount--;
        }

        internal virtual void AcquireThrottle()
        {
            return;
        }
    }
}
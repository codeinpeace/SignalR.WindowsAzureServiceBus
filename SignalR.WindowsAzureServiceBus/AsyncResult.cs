// 
//  (c) Microsoft Corporation. All rights reserved.
//  

namespace SignalR.WindowsAzureServiceBus
{
    using System;
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    ///   A generic base class for IAsyncResult implementations
    ///   that wraps a ManualResetEvent.
    /// </summary>
    abstract class AsyncResult : IAsyncResult
    {
        readonly AsyncCallback callback;
        readonly object state;
        readonly object thisLock;
        bool completedSynchronously;
        bool endCalled;
        Exception exception;
        bool isCompleted;
        ManualResetEvent manualResetEvent;

        protected AsyncResult(AsyncCallback callback, object state)
        {
            this.callback = callback;
            this.state = state;
            this.thisLock = new object();
        }

        object ThisLock { get { return this.thisLock; } }

        public object AsyncState { get { return this.state; } }

        public WaitHandle AsyncWaitHandle
        {
            get
            {
                if (this.manualResetEvent != null)
                {
                    return this.manualResetEvent;
                }

                lock (this.ThisLock)
                {
                    if (this.manualResetEvent == null)
                    {
                        this.manualResetEvent = new ManualResetEvent(this.isCompleted);
                    }
                }

                return this.manualResetEvent;
            }
        }

        public bool CompletedSynchronously { get { return this.completedSynchronously; } }

        public bool IsCompleted { get { return this.isCompleted; } }

        // Call this version of complete when your asynchronous operation is complete.  This will update the state
        // of the operation and notify the callback.
        protected void Complete(bool completedSynchronously)
        {
            if (this.isCompleted)
            {
                // It's a bug to call Complete twice.
                throw new InvalidOperationException("Cannot call Complete twice");
            }

            this.completedSynchronously = completedSynchronously;

            if (completedSynchronously)
            {
                // If we completedSynchronously, then there's no chance that the manualResetEvent was created so
                // we don't need to worry about a race
                Debug.Assert(this.manualResetEvent == null, "No ManualResetEvent should be created for a synchronous AsyncResult.");
                this.isCompleted = true;
            }
            else
            {
                lock (this.ThisLock)
                {
                    this.isCompleted = true;
                    if (this.manualResetEvent != null)
                    {
                        this.manualResetEvent.Set();
                    }
                }
            }

            // If the callback throws, there is a bug in the callback implementation
            if (this.callback != null)
            {
                this.callback(this);
            }
        }

        // Call this version of complete if you raise an exception during processing.  In addition to notifying
        // the callback, it will capture the exception and store it to be thrown during AsyncResult.End.
        protected void Complete(bool completedSynchronously, Exception exception)
        {
            this.exception = exception;
            this.Complete(completedSynchronously);
        }

        // End should be called when the End function for the asynchronous operation is complete.  It
        // ensures the asynchronous operation is complete, and does some common validation.
        protected static TAsyncResult End<TAsyncResult>(IAsyncResult result) where TAsyncResult : AsyncResult
        {
            if (result == null)
            {
                throw new ArgumentNullException("result");
            }

            var asyncResult = result as TAsyncResult;

            if (asyncResult == null)
            {
                throw new ArgumentException("Invalid async result.", "result");
            }

            if (asyncResult.endCalled)
            {
                throw new InvalidOperationException("Async object already ended.");
            }

            asyncResult.endCalled = true;

            if (!asyncResult.isCompleted)
            {
                asyncResult.AsyncWaitHandle.WaitOne();
            }

            if (asyncResult.manualResetEvent != null)
            {
                asyncResult.manualResetEvent.Close();
            }

            if (asyncResult.exception != null)
            {
                throw asyncResult.exception;
            }

            return asyncResult;
        }
    }

    //An AsyncResult that completes as soon as it is instantiated.
    class CompletedAsyncResult : AsyncResult
    {
        public CompletedAsyncResult(AsyncCallback callback, object state)
            : base(callback, state)
        {
            this.Complete(true);
        }

        public static void End(IAsyncResult result)
        {
            End<CompletedAsyncResult>(result);
        }
    }

    //A strongly typed AsyncResult
    abstract class TypedAsyncResult<T> : AsyncResult
    {
        T data;

        protected TypedAsyncResult(AsyncCallback callback, object state)
            : base(callback, state)
        {
        }

        public T Data { get { return this.data; } }

        protected void Complete(T data, bool completedSynchronously)
        {
            this.data = data;
            this.Complete(completedSynchronously);
        }

        public static T End(IAsyncResult result)
        {
            var typedResult = End<TypedAsyncResult<T>>(result);
            return typedResult.Data;
        }
    }

    class TypedCompletedAsyncResult<T> : TypedAsyncResult<T>
    {
        public TypedCompletedAsyncResult(T result, AsyncCallback callback, object state)
            : base(callback, state)
        {
            this.Complete(result, true);
        }

        
    }
}
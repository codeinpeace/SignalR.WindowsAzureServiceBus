using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SignalR.WindowsAzureServiceBus
{
    class CachedMessage : Message
    {
        readonly ulong id;

        public CachedMessage(string signalKey, object signalValue, DateTime created, ulong id)
            :base(signalKey, signalValue, created)
        {
            this.id = id;
        }

        public ulong Id { get { return this.id; } }
    }
    
}

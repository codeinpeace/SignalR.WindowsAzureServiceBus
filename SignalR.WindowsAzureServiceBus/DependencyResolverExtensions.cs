//---------------------------------------------------------------------------------
// Copyright (c) 2012, Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//---------------------------------------------------------------------------------

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

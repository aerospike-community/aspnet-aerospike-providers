/* 
 * Copyright 2012-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Web;
using System.Web.SessionState;
using Aerospike.Client;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Aerospike.Web
{
	internal abstract class AerospikeCache
	{
		internal AerospikeClient client;
		private string ns;
		private string set;
		private string app;

		public AerospikeCache(ProviderConfiguration config)
		{
			this.ns = config.Namespace;
			this.set = config.Set;
			this.app = config.ApplicationName + '_';

			ClientPolicy policy = new ClientPolicy();
			policy.user = config.User;
			policy.password = config.Password;
			policy.maxConnsPerNode = config.MaxConnsPerNode;
			policy.maxSocketIdle = config.MaxSocketIdle;
			policy.tendInterval = config.TendInterval;
			policy.failIfNotConnected = false;
			policy.timeout = config.ConnectionTimeout;

			policy.readPolicyDefault.timeout = config.OperationTimeout;
			policy.readPolicyDefault.maxRetries = config.MaxRetries;
			policy.readPolicyDefault.sleepBetweenRetries = config.SleepBetweenRetries;

			policy.writePolicyDefault.timeout = config.OperationTimeout;
			policy.writePolicyDefault.maxRetries = config.MaxRetries;
			policy.writePolicyDefault.sleepBetweenRetries = config.SleepBetweenRetries;

			// Host Format:
			//   host1:port1[,host2:port2]...
			// 
			// Host Examples:
			//   nodeone:3000
			//   192.168.1.1:3000
			//   192.168.1.1:3000,192.168.1.2:3000,192.168.1.3:3000
			string[] seeds = config.Host.Split(',');
			Host[] hosts = new Host[seeds.Length];
			int count = 0;

			foreach (string seed in seeds)
			{
				string[] hostPort = seed.Split(':');
				string host = hostPort[0].Trim();
				int port = Convert.ToInt32(hostPort[1]);
				hosts[count++] = new Host(host, port);
			}

			client = new AerospikeClient(policy, hosts);
		}

		public AerospikeClient Client
		{
			get { return client; }
		}

		public void CreateSessionData(string sessionId, int sessionTimeout)
		{
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);

			client.Put(policy, key,
				new Bin("Locked", false),
				new Bin("LockId", 0),
				new Bin("LockTime", DateTime.UtcNow.Ticks),
				new Bin("SessionTimeout", sessionTimeout)
				);
		}

		public void WriteSessionData(string sessionId, int sessionTimeout, ISessionStateItemCollection items)
		{
			Dictionary<string, Value> map = SessionUtility.Serialize(items);
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);

			client.Put(policy, key,
				new Bin("Locked", false),
				new Bin("LockId", 0),
				new Bin("LockTime", DateTime.UtcNow.Ticks),
				new Bin("SessionTimeout", sessionTimeout),
				new Bin("SessionItems", map)
				);
		}

		public SessionStateStoreData ParseSessionStore(HttpContext context, IDictionary map, ISessionStateItemCollection items, int sessionTimeout, ref SessionStateActions actions)
		{
			if (map == null)
			{
				actions = SessionStateActions.InitializeItem;
				return SessionUtility.CreateStoreData(context, items, sessionTimeout / 60);
			}

			foreach (DictionaryEntry entry in map)
			{
				string name = (string)entry.Key;

				if (name != null)
				{
					items[name] = SessionUtility.Deserialize((byte[])entry.Value);
				}
			}

			items.Dirty = false;
			return SessionUtility.CreateStoreData(context, items, sessionTimeout / 60);
		}

		public Key GetKey(string sessionId)
		{
			return new Key(ns, set, app + sessionId);
		}

		public void Close()
		{
			client.Close();
		}

		public abstract SessionStateStoreData ReadSessionData(bool exclusive, HttpContext context, string sessionId, int requestTimeout, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions);
		public abstract void UpdateSessionData(string sessionId, long lockId, int sessionTimeout, ISessionStateItemCollection items);
		public abstract void ReleaseItemExclusive(string sessionId, long lockId, int sessionTimeout);
		public abstract void ResetItemTimeout(string sessionId, int sessionTimeout);
		public abstract void RemoveItem(string sessionId, long lockId);
	}
}

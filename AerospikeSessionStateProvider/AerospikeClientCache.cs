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

namespace Aerospike.Web
{
	internal class AerospikeClientCache
	{
		private static DateTime Epoch = new DateTime(2010, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
		private const string SessionFile = "sessionstate.lua";
		private const string SessionPackage = "sessionstate";

		private AerospikeClient client;
		private string ns;
		private string set;
		private string app;

		public AerospikeClientCache(ProviderConfiguration config)
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

			try
			{
				if (!FindUDF())
				{
					RegisterUDF();
				}
			}
			catch (Exception)
			{
				client.Close();
				throw;
			}
		}

		public AerospikeClient Client
		{
			get { return client; }
		}

		private bool FindUDF()
		{
			Node node = client.Nodes[0];
			string response = Info.Request(null, node, "udf-list");
			string find = "filename=" + SessionFile;
			return response.IndexOf(find) >= 0;
		}

		private void RegisterUDF()
		{
			Assembly assembly = Assembly.GetExecutingAssembly();
			RegisterTask task = client.Register(null, assembly, "Aerospike.Web." + SessionFile, SessionFile, Language.LUA);
			task.Wait();
		}

		public SessionStateStoreData ReadSessionData(string sessionId, int timeout, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			locked = false;
			lockId = null;
			lockAge = TimeSpan.Zero;
			actions = 0;

			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.timeout = timeout;

			Key key = GetKey(sessionId);
			Record record = client.Get(policy, key);

			if (record == null)
			{
				return null;
			}

			locked = record.GetBool("Locked");
			lockId = record.GetValue("LockId");
			DateTime lockTime = new DateTime(record.GetLong("LockTime"), DateTimeKind.Utc);
			lockAge = DateTime.UtcNow.Subtract(lockTime);

			if (locked)
			{
				return null;
			}

			int sessionTimeout = record.GetInt("SessionTimeout");
			IDictionary map = (IDictionary)record.GetValue("SessionItems");
			return ParseSessionData(map, sessionTimeout, ref actions);
		}

		public SessionStateStoreData ReadSessionDataExclusive(string sessionId, int timeout, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			locked = false;
			lockId = null;
			lockAge = TimeSpan.Zero;
			actions = 0;

			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.timeout = timeout;

			Key key = GetKey(sessionId);
			IList results = (IList)client.Execute(policy, key, SessionPackage, "GetItemExclusive", Value.Get(DateTime.UtcNow.Ticks));
			
			if (results == null)
			{
				return null;
			}

			locked = ((long)results[0] == 0) ? false : true;
			lockId = results[1];
			DateTime lockTime = new DateTime((long)results[2], DateTimeKind.Utc);
			lockAge = DateTime.UtcNow.Subtract(lockTime);

			if (locked)
			{
				return null;
			}

			int sessionTimeout = (int)(long)results[3];
			IDictionary map = (IDictionary)results[4];
			return ParseSessionData(map, sessionTimeout, ref actions);
		}

		private SessionStateStoreData ParseSessionData(IDictionary map, int sessionTimeout,  ref SessionStateActions actions)
		{
			SessionStateItems items = new SessionStateItems();

			foreach (DictionaryEntry entry in map)
			{
				string name = (string)entry.Key;

				if (name != null)
				{
					items[name] = SessionUtility.GetObjectFromBytes((byte[])entry.Value);
				}
			}

			// Restore action flag from session data
			if (items["SessionStateActions"] != null)
			{
				actions = (SessionStateActions)items["SessionStateActions"];
			}

			items.Dirty = false;
			return new SessionStateStoreData(items, new HttpStaticObjectsCollection(), sessionTimeout / 60);
		}

		public void WriteSessionData(string sessionId, SessionStateItems items, int sessionTimeout)
		{
			Dictionary<string, Value> map = new Dictionary<string, Value>(items.Keys.Count);

			foreach (string sessionKey in items.Keys)
			{
				map[sessionKey] = Value.Get(SessionUtility.GetBytesFromObject(items[sessionKey]));
			}

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

		public void UpdateSessionData(string sessionId, long lockId, SessionStateItems items, int sessionTimeout)
		{
			List<string> delItems = new List<string>();
			SessionUtility.AppendRemoveItemsInList(items, delItems);

			Dictionary<String, Value> modItems = new Dictionary<String, Value>(items.Keys.Count);
			SessionUtility.AppendUpdatedOrNewItemsInList(items, modItems);

			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);
			client.Execute(policy, key, SessionPackage, "UpdateItemExclusive", Value.Get(lockId), Value.Get(sessionTimeout), Value.Get(delItems), Value.Get(modItems));
		}

		public void ReleaseItemExclusive(string sessionId, long lockId, int sessionTimeout)
		{
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);
			client.Execute(policy, key, SessionPackage, "ReleaseItemExclusive", Value.Get(lockId), Value.Get(sessionTimeout));
		}

		public void ResetItemTimeout(string sessionId, int sessionTimeout)
		{
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);
			client.Execute(policy, key, SessionPackage, "ResetItemTimeout", Value.Get(sessionTimeout));
		}

		public void RemoveItem(string sessionId, long lockId)
		{
			Key key = GetKey(sessionId);
			client.Execute(null, key, SessionPackage, "RemoveItem", Value.Get(lockId));
		}

		public Key GetKey(string sessionId)
		{
			return new Key(ns, set, app + sessionId);
		}

		public void Close()
		{
			client.Close();
		}
	}
}

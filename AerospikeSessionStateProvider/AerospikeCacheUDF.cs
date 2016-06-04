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
using System.IO;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Web;
using System.Web.SessionState;
using Aerospike.Client;

namespace Aerospike.Web
{
	internal class AerospikeCacheUDF : AerospikeCache
	{
		private const string SessionFile = "sessionstate.lua";
		private const string SessionPackage = "sessionstate";

		public AerospikeCacheUDF(ProviderConfiguration config) : base(config)
		{
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

		public override SessionStateStoreData ReadSessionData(bool exclusive, HttpContext context, string sessionId, int requestTimeout, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			locked = false;
			lockId = null;
			lockAge = TimeSpan.Zero;
			actions = SessionStateActions.None;

			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.timeout = requestTimeout;

			Key key = GetKey(sessionId);
			IDictionary map;
			int sessionTimeout;

			if (exclusive)
			{
				IList results = (IList)client.Execute(policy, key, SessionPackage, "GetItemExclusive", Value.Get(DateTime.UtcNow.Ticks));

				if (results == null)
				{
					return null;
				}

				locked = ((long)results[0] == 0) ? false : true;
				lockId = results[1];
				DateTime lockTime = new DateTime((long)results[2], DateTimeKind.Utc);
				lockAge = DateTime.UtcNow.Subtract(lockTime);
				sessionTimeout = (int)(long)results[3];

				if (locked)
				{
					return null;
				}

				// Check if session items was returned.
				if (results.Count >= 5)
				{
					map = (IDictionary)results[4];
				}
				else
				{
					map = null;
				}
			}
			else
			{
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

				sessionTimeout = record.GetInt("SessionTimeout");
				map = (IDictionary)record.GetValue("SessionItems");
			}

			SessionStateItems items = new SessionStateItems();
			return ParseSessionStore(context, map, items, sessionTimeout, ref actions);
		}

		public override void UpdateSessionData(string sessionId, long lockId, int sessionTimeout, ISessionStateItemCollection items)
		{
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);

			if (items != null)
			{
				List<string> delItems = new List<string>();
				AppendRemoveItemsInList((SessionStateItems)items, delItems);

				Dictionary<String, Value> modItems = new Dictionary<String, Value>(items.Keys.Count);
				AppendUpdatedOrNewItemsInList((SessionStateItems)items, modItems);

				client.Execute(policy, key, SessionPackage, "MergeItemExclusive", Value.Get(lockId), Value.Get(sessionTimeout), Value.Get(delItems), Value.Get(modItems));
			}
			else
			{
				Dictionary<string, Value> map = new Dictionary<string, Value>();
				client.Execute(policy, key, SessionPackage, "WriteItemExclusive", Value.Get(lockId), Value.Get(sessionTimeout), Value.Get(map));
			}
		}

		public override void ReleaseItemExclusive(string sessionId, long lockId, int sessionTimeout)
		{
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);
			client.Execute(policy, key, SessionPackage, "ReleaseItemExclusive", Value.Get(lockId), Value.Get(sessionTimeout));
		}

		public override void ResetItemTimeout(string sessionId, int sessionTimeout)
		{
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);
			client.Execute(policy, key, SessionPackage, "ResetItemTimeout", Value.Get(sessionTimeout));
		}

		public override void RemoveItem(string sessionId, long lockId)
		{
			Key key = GetKey(sessionId);
			client.Execute(null, key, SessionPackage, "RemoveItem", Value.Get(lockId));
		}

		private static void AppendRemoveItemsInList(SessionStateItems items, List<string> list)
		{
			if (items.GetDeletedKeys() != null && items.GetDeletedKeys().Count != 0)
			{
				foreach (string delKey in items.GetDeletedKeys())
				{
					list.Add(delKey);
				}
			}
		}

		private static void AppendUpdatedOrNewItemsInList(SessionStateItems items, Dictionary<string, Value> map)
		{
			if (items.GetModifiedKeys() != null && items.GetModifiedKeys().Count != 0)
			{
				using (MemoryStream memoryStream = new MemoryStream())
				{
					foreach (string key in items.GetModifiedKeys())
					{
						map[key] = Value.Get(SessionUtility.Serialize(memoryStream, items[key]));
					}
				}
			}
		}
	}
}

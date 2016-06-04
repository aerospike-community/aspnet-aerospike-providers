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
	internal class AerospikeCacheDefault : AerospikeCache
	{
		public AerospikeCacheDefault(ProviderConfiguration config) : base(config)
		{
		}

		public override SessionStateStoreData ReadSessionData(bool exclusive, HttpContext context, string sessionId, int requestTimeout, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			locked = false;
			lockId = null;
			lockAge = TimeSpan.Zero;
			actions = SessionStateActions.None;

			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.timeout = requestTimeout;

			// Read existing record.
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

			if (exclusive)
			{
				long lockIdLong = (long)lockId + 1;
				lockId = lockIdLong;

				// Initialize generation lock.
				policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
				policy.generation = record.generation;
				policy.expiration = sessionTimeout;

				try
				{
					// Write new lock values if generation lock succeeds.
					client.Put(policy, key,
						new Bin("Locked", true),
						new Bin("LockId", lockIdLong),
						new Bin("LockTime", DateTime.UtcNow.Ticks)
						);
				}
				catch (AerospikeException ae)
				{
					// Check for generation lock error.
					if (ae.Result == ResultCode.GENERATION_ERROR)
					{
						// Failed to lock.
						locked = true;
						lockAge = TimeSpan.Zero;
						return null;
					}

					// Throw an exception on all other errors.
					throw;
				}
			}

			IDictionary map = (IDictionary)record.GetValue("SessionItems");
			SessionStateItemCollection items = new SessionStateItemCollection();
			return ParseSessionStore(context, map, items, sessionTimeout, ref actions);
		}

		public override void UpdateSessionData(string sessionId, long lockId, int sessionTimeout, ISessionStateItemCollection items)
		{
			// Read record to determine if locked.
			Key key = GetKey(sessionId);
			int generation = ReadExclusive(key, lockId);

			// Do nothing if generation lock invalid.
			if (generation < 0)
			{
				return;
			}

			// Write session data.
			Dictionary<string, Value> map = SessionUtility.Serialize(items);
			WriteExclusive(key, generation, sessionTimeout,
				new Bin("Locked", false),
				new Bin("SessionTimeout", sessionTimeout),
				new Bin("SessionItems", map)
				);
		}

		public override void ReleaseItemExclusive(string sessionId, long lockId, int sessionTimeout)
		{
			// Read record to determine if locked.
			Key key = GetKey(sessionId);
			int generation = ReadExclusive(key, lockId);

			// Do nothing if generation lock invalid.
			if (generation < 0)
			{
				return;
			}

			// Write session data.
			WriteExclusive(key, generation, sessionTimeout,
				new Bin("Locked", false),
				new Bin("SessionTimeout", sessionTimeout)
				);
		}

		public override void ResetItemTimeout(string sessionId, int sessionTimeout)
		{
			// Write new session timeout only if record exists.
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
			policy.expiration = sessionTimeout;

			Key key = GetKey(sessionId);

			try
			{
				client.Put(policy, key, new Bin("SessionTimeout", sessionTimeout));
			}
			catch (AerospikeException ae)
			{
				// Check for record not found error.
				if (ae.Result == ResultCode.KEY_NOT_FOUND_ERROR)
				{
					return;
				}

				// Throw an exception on all other errors.
				throw;
			}
		}

		public override void RemoveItem(string sessionId, long lockId)
		{
			// Read record to determine if locked.
			Key key = GetKey(sessionId);
			int generation = ReadExclusive(key, lockId);

			// Do nothing if generation lock invalid.
			if (generation < 0)
			{
				return;
			}

			// Delete if record has not been updated since read.
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
			policy.generation = generation;

			try
			{
				client.Delete(policy, key);
			}
			catch (AerospikeException ae)
			{
				// Check for generation lock error.
				if (ae.Result == ResultCode.GENERATION_ERROR)
				{
					// Do nothing if record updated by someone else.
					return;
				}

				// Throw an exception on all other errors.
				throw;
			}
		}

		private int ReadExclusive(Key key, long lockId)
		{
			Record record = client.Get(null, key, "LockId");

			if (record == null)
			{
				return -1;
			}

			long id = record.GetLong("LockId");

			if (id != lockId)
			{
				return -1;
			}

			return record.generation;
		}

		private void WriteExclusive(Key key, int generation, int sessionTimeout, params Bin[] bins)
		{
			// Write and unlock if record has not been updated since read.
			WritePolicy policy = new WritePolicy(client.writePolicyDefault);
			policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
			policy.generation = generation;
			policy.expiration = sessionTimeout;

			try
			{
				client.Put(policy, key, bins);
			}
			catch (AerospikeException ae)
			{
				// Check for generation lock error.
				if (ae.Result == ResultCode.GENERATION_ERROR)
				{
					// Do nothing if record updated by someone else.
					return;
				}

				// Throw an exception on all other errors.
				throw;
			}
		}
	}
}

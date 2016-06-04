//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See LICENSE.md in the project root for license information.
//
using System;
using System.Text;
using System.Collections;
using System.Collections.Generic;
using System.Web.SessionState;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Aerospike.Client;

namespace Aerospike.Web.Test
{
	/// <summary>
	/// These tests assume an Aerospike Cluster has been deployed and is currently running.
	/// </summary>
	[TestClass]
	public class TestSessionState
	{
		public static Args args = Args.Instance;

		[TestMethod]
		public void DefaultSessionWriteCycle()
		{
			SessionWriteCycle(false);
		}

		[TestMethod]
		public void UDFSessionWriteCycle()
		{
			SessionWriteCycle(true);
		}

		private void SessionWriteCycle(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			session.CreateUninitializedItem(null, sessionId, 10);

			bool locked;
			TimeSpan lockAge;
			object lockId;
			SessionStateActions actions;
			SessionStateStoreData storeData = session.GetItemExclusive(null, sessionId, out locked, out lockAge, out lockId, out actions);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
			AerospikeClient client = cache.Client;
			Key key = cache.GetKey(sessionId);
			Record record = client.Get(null, key);
			long lid = record.GetLong("LockId");

			Assert.AreEqual((long)lockId, lid);

			storeData.Items["key"] = "value";
			session.SetAndReleaseItemExclusive(null, sessionId, storeData, lockId, false);

			record = client.Get(null, key);
			locked = record.GetBool("Locked");
			Assert.IsFalse(locked);
			IDictionary map = (IDictionary)record.GetValue("SessionItems");
			Assert.AreEqual(1, map.Count);
			object val = SessionUtility.Deserialize((byte[])map["key"]);
			Assert.AreEqual("value", val);

			session.ResetItemTimeout(null, sessionId);
			session.EndRequest(null);
			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultSessionReadCycle()
		{
			SessionReadCycle(false);
		}

		[TestMethod]
		public void UDFSessionReadCycle()
		{
			SessionReadCycle(true);
		}

		private void SessionReadCycle(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			session.CreateUninitializedItem(null, sessionId, 10);

			bool locked;
			TimeSpan lockAge;
			object lockId;
			SessionStateActions actions;
			SessionStateStoreData storeData = session.GetItem(null, sessionId, out locked, out lockAge, out lockId, out actions);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
			AerospikeClient client = cache.Client;
			Key key = cache.GetKey(sessionId);
			Record record = client.Get(null, key);
			long lid = record.GetLong("LockId");

			Assert.AreEqual(lid, 0);

			session.ResetItemTimeout(null, sessionId);
			session.EndRequest(null);
			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultSessionTimoutChangeFromGlobalAspx()
		{
			SessionTimoutChangeFromGlobalAspx(false);
		}

		[TestMethod]
		public void UDFSessionTimoutChangeFromGlobalAspx()
		{
			SessionTimoutChangeFromGlobalAspx(true);
		}

		private void SessionTimoutChangeFromGlobalAspx(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			session.CreateUninitializedItem(null, sessionId, 10);

			bool locked;
			TimeSpan lockAge;
			object lockId;
			SessionStateActions actions;
			SessionStateStoreData storeData = session.GetItemExclusive(null, sessionId, out locked, out lockAge, out lockId, out actions);

			storeData.Items["key"] = "value";
			storeData.Timeout = 5;
			session.SetAndReleaseItemExclusive(null, sessionId, storeData, lockId, false);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
			AerospikeClient client = cache.Client;
			Key key = cache.GetKey(sessionId);
			Record record = client.Get(null, key);

			int sessionTimeout = record.GetInt("SessionTimeout");
			Assert.AreEqual(300, sessionTimeout);

			IDictionary map = (IDictionary)record.GetValue("SessionItems");
			Assert.AreEqual(1, map.Count);
			object val = SessionUtility.Deserialize((byte[])map["key"]);
			Assert.AreEqual("value", val);

			session.EndRequest(null);

			bool locked_1;
			TimeSpan lockAge_1;
			object lockId_1;
			SessionStateActions actions_1;
			SessionStateStoreData storeData_1 = session.GetItem(null, sessionId, out locked_1, out lockAge_1, out lockId_1, out actions_1);
			Assert.AreEqual(5, storeData_1.Timeout);

			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultReleaseItemExclusiveWithNullLockId()
		{
			ReleaseItemExclusiveWithNullLockId(false);
		}

		[TestMethod]
		public void UDFReleaseItemExclusiveWithNullLockId()
		{
			ReleaseItemExclusiveWithNullLockId(true);
		}

		private void ReleaseItemExclusiveWithNullLockId(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			session.ReleaseItemExclusive(null, sessionId, null);
			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultRemoveItemWithNullLockId()
		{
			RemoveItemWithNullLockId(false);
		}

		[TestMethod]
		public void UDFRemoveItemWithNullLockId()
		{
			RemoveItemWithNullLockId(true);
		}

		private void RemoveItemWithNullLockId(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			session.RemoveItem(null, sessionId, null, null);
			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultInitializeWithNullConfig()
		{
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();

			try
			{
				session.Initialize(null, null);
				Assert.Fail("Initialize should have failed");
			}
			catch (ArgumentNullException)
			{
			}
		}

		[TestMethod]
		public void DefaultCreateNewStoreDataWithEmptyStore()
		{
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			SessionStateStoreData sssd = new SessionStateStoreData(new SessionStateItemCollection(), null, 900);
			Assert.AreEqual(true, CompareSessionStateStoreData(session.CreateNewStoreData(null, 900), sssd));
		}

		[TestMethod]
		public void DefaultGetItemNullFromStore()
		{
			GetItemNullFromStore(false);
		}

		[TestMethod]
		public void UDFGetItemNullFromStore()
		{
			GetItemNullFromStore(true);
		}

		private void GetItemNullFromStore(bool useUDF)
		{
			CreateConfiguration(useUDF);
			string sessionId = "session-id";
			bool locked;
			TimeSpan lockAge;
			object lockId = null;
			SessionStateActions actions;

			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			SessionStateStoreData data = session.GetItem(null, sessionId, out locked, out lockAge, out lockId, out actions);
			Assert.IsNull(lockId);

			session.ReleaseItemExclusive(null, sessionId, lockId);

			Assert.IsNull(data);
			Assert.IsFalse(locked);
			Assert.AreEqual(TimeSpan.Zero, lockAge);
			Assert.IsNull(lockId);
		}

		[TestMethod]
		public void DefaultGetItemRecordLocked()
		{
			GetItemRecordLocked(false);
		}

		[TestMethod]
		public void UDFGetItemRecordLocked()
		{
			GetItemRecordLocked(true);
		}

		private void GetItemRecordLocked(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			session.CreateUninitializedItem(null, sessionId, 10);

			bool locked;
			TimeSpan lockAge;
			object lockId;
			SessionStateActions actions;
			SessionStateStoreData storeData = session.GetItemExclusive(null, sessionId, out locked, out lockAge, out lockId, out actions);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
			AerospikeClient client = cache.Client;
			Key key = cache.GetKey(sessionId);
			Record record = client.Get(null, key);
			long lid = record.GetLong("LockId");
			Assert.AreEqual((long)lockId, lid);

			storeData = session.GetItemExclusive(null, sessionId, out locked, out lockAge, out lockId, out actions);

			Assert.IsNull(storeData);
			Assert.IsTrue(locked);
			Assert.AreEqual(1, (long)lockId);

			session.ReleaseItemExclusive(null, sessionId, lockId);
			session.EndRequest(null);
			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultRemoveItem()
		{
			RemoveItem(false);
		}

		[TestMethod]
		public void UDFRemoveItem()
		{
			RemoveItem(true);
		}

		private void RemoveItem(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			session.CreateUninitializedItem(null, sessionId, 10);

			bool locked;
			TimeSpan lockAge;
			object lockId;
			SessionStateActions actions;
			SessionStateStoreData storeData = session.GetItemExclusive(null, sessionId, out locked, out lockAge, out lockId, out actions);
			session.RemoveItem(null, sessionId, lockId, storeData);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
			AerospikeClient client = cache.Client;
			Key key = cache.GetKey(sessionId);
			Record record = client.Get(null, key);
			Assert.IsNull(record);

			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultSetAndReleaseItemExclusiveNewItemNullItems()
		{
			SetAndReleaseItemExclusiveNewItemNullItems(false);
		}

		[TestMethod]
		public void UDFSetAndReleaseItemExclusiveNewItemNullItems()
		{
			SetAndReleaseItemExclusiveNewItemNullItems(true);
		}

		private void SetAndReleaseItemExclusiveNewItemNullItems(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			SessionStateStoreData data = new SessionStateStoreData(null, null, 15);

			session.SetAndReleaseItemExclusive(null, sessionId, data, null, true);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
			AerospikeClient client = cache.Client;
			Key key = cache.GetKey(sessionId);
			Record record = client.Get(null, key);
			Assert.IsNotNull(record);
			long lid = record.GetLong("LockId");
			Assert.AreEqual(0, lid);

			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultSetAndReleaseItemExclusiveNewValidItems()
		{
			SetAndReleaseItemExclusiveNewValidItems(false);
		}

		[TestMethod]
		public void UDFSetAndReleaseItemExclusiveNewValidItems()
		{
			SetAndReleaseItemExclusiveNewValidItems(true);
		}

		private void SetAndReleaseItemExclusiveNewValidItems(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();

			SessionStateItemCollection items = new SessionStateItemCollection();
			items["session-key"] = "session-value";
			SessionStateStoreData data = new SessionStateStoreData(items, null, 15);

			session.SetAndReleaseItemExclusive(null, sessionId, data, null, true);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
			AerospikeClient client = cache.Client;
			Key key = cache.GetKey(sessionId);
			Record record = client.Get(null, key);
			Assert.IsNotNull(record);
			long lid = record.GetLong("LockId");
			Assert.AreEqual(0, lid);

			IDictionary map = (IDictionary)record.GetValue("SessionItems");
			Assert.AreEqual(1, map.Count);
			object val = SessionUtility.Deserialize((byte[])map["session-key"]);
			Assert.AreEqual("session-value", val);

			AerospikeSessionStateProvider.CloseCache();
		}

		[TestMethod]
		public void DefaultSetAndReleaseItemExclusiveNullItems()
		{
			SetAndReleaseItemExclusiveNullItems(false);
		}

		[TestMethod]
		public void UDFSetAndReleaseItemExclusiveNullItems()
		{
			SetAndReleaseItemExclusiveNullItems(true);
		}

		private void SetAndReleaseItemExclusiveNullItems(bool useUDF)
		{
			string sessionId = CreateConfiguration(useUDF);
			AerospikeSessionStateProvider session = new AerospikeSessionStateProvider();
			SessionStateStoreData data = new SessionStateStoreData(null, null, 15);

			session.SetAndReleaseItemExclusive(null, sessionId, data, 7L, false);

			ProviderConfiguration config = AerospikeSessionStateProvider.Configuration;
			AerospikeCache cache = AerospikeSessionStateProvider.Cache;
		}

		private string CreateConfiguration(bool useUDF)
		{
			System.Collections.Specialized.NameValueCollection config = new System.Collections.Specialized.NameValueCollection();
			config.Add("host", args.host);
			config.Add("namespace", args.ns);
			config.Add("set", args.set);
			config.Add("user", args.user);
			config.Add("password", args.password);
			config.Add("useUDF", useUDF? "true" : "false");

			AerospikeSessionStateProvider.SetConfiguration(config);
			return Guid.NewGuid().ToString();
		}

		private static bool CompareSessionStateStoreData(SessionStateStoreData obj1, SessionStateStoreData obj2)
		{
			if ((obj1 == null && obj2 != null) || (obj1 != null && obj2 == null))
			{
				return false;
			}
			else if (obj1 != null && obj2 != null)
			{
				if (obj1.Timeout != obj2.Timeout)
				{
					return false;
				}

				System.Collections.Specialized.NameObjectCollectionBase.KeysCollection keys1 = obj1.Items.Keys;
				System.Collections.Specialized.NameObjectCollectionBase.KeysCollection keys2 = obj2.Items.Keys;

				if ((keys1 != null && keys2 == null) || (keys1 == null && keys2 != null))
				{
					return false;
				}
				else if (keys1 != null && keys2 != null)
				{
					foreach (string key in keys1)
					{
						if (obj2.Items[key] == null)
						{
							return false;
						}
					}
				}

			}
			return true;
		}
	}
}

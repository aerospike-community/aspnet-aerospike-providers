//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See LICENSE.md in the project root for license information.
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.Web;
using System.Web.SessionState;
using Aerospike.Client;

namespace Aerospike.Web
{
	public class AerospikeSessionStateProvider : SessionStateStoreProviderBase
	{
		private static ProviderConfiguration config;
		private static AerospikeClientCache cache;
		private static object configLock = new object();
		private static object cacheLock = new object();
		private static object lastException = new object();
		private const int FROM_MIN_TO_SEC = 60;

		public override void Initialize(string name, System.Collections.Specialized.NameValueCollection nameValuePairs)
		{
			if (nameValuePairs == null)
			{
				throw new ArgumentNullException("nameValues");
			}

			if (name == null || name.Length == 0)
			{
				name = "MyCacheStore";
			}

			if (String.IsNullOrEmpty(nameValuePairs["description"]))
			{
				nameValuePairs.Remove("description");
				nameValuePairs.Add("description", "Aerospike as a session data store");
			}

			base.Initialize(name, nameValuePairs);
			SetConfiguration(nameValuePairs);
		}

		/// <summary>
		/// Create one session state configuration that is shared across all instances.
		/// </summary>
		public static void SetConfiguration(System.Collections.Specialized.NameValueCollection nameValues)
		{
			if (config == null)
			{
				lock (configLock)
				{
					if (config == null)
					{
						config = new ProviderConfiguration(nameValues);
					}
				}
			}
		}

		/// <summary>
		/// Get configuration. Useful for testing.
		/// </summary>
		internal static ProviderConfiguration Configuration
		{
			get {return config;}
		}

		/// <summary>
		/// Get cache. Useful for testing.
		/// </summary>
		internal static AerospikeClientCache Cache
		{
			get { return cache; }
		}

		/// <summary>
		/// Create one Aerospike client that is shared across all instances.
		/// </summary>
		private static void OpenCache()
		{
			if (cache == null)
			{
				lock (cacheLock)
				{
					if (cache == null)
					{
						cache = new AerospikeClientCache(config);
					}
				}
			}
		}

		/// <summary>
		/// Close Aerospike client.  Useful for testing.
		/// It does not look like ASP.NET notifies session store providers that the application will
		/// exit and database server connections need to close.  Is there a clean way to shutdown? 
		/// </summary>
		public static void CloseCache()
		{
			if (cache != null)
			{
				cache.Close();
				cache = null;
			}
			config = null;
		}

		public override void InitializeRequest(HttpContext context)
		{
		}

		public override void EndRequest(HttpContext context)
		{
		}

		public override void Dispose()
		{
		}

		public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			LogUtility.LogInfo("GetItem => Session Id: {0}, Session provider object: {1}.", id, this.GetHashCode());

			try
			{
				OpenCache();
				return cache.ReadSessionData(id, config.RequestTimeout, out locked, out lockAge, out lockId, out actions);
			}
			catch (Exception e)
			{
				LogUtility.LogError("GetItemFromSessionStore => {0}", e.ToString());
				locked = false;
				lockId = null;
				lockAge = TimeSpan.Zero;
				actions = 0;
				LastException = e;
				if (config == null || config.ThrowOnError)
				{
					throw;
				}
				return null;
			}
		}

		public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			LogUtility.LogInfo("GetItemExclusive => Session Id: {0}, Session provider object: {1}.", id, this.GetHashCode());

			try
			{
				OpenCache();
				return cache.ReadSessionDataExclusive(id, config.RequestTimeout, out locked, out lockAge, out lockId, out actions);
			}
			catch (Exception e)
			{
				LogUtility.LogError("GetItemFromSessionStore => {0}", e.ToString());
				locked = false;
				lockId = null;
				lockAge = TimeSpan.Zero;
				actions = 0;
				LastException = e;
				if (config == null || config.ThrowOnError)
				{
					throw;
				}
				return null;
			}
		}

		public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
		{
			try
			{
				if (LastException == null)
				{
					if (newItem)
					{
						SessionStateItems sessionItems = null;
						if (item != null && item.Items != null)
						{
							sessionItems = (SessionStateItems)item.Items;
						}
						else
						{
							sessionItems = new SessionStateItems();
						}

						if (sessionItems["SessionStateActions"] != null)
						{
							sessionItems.Remove("SessionStateActions");
						}

						OpenCache();
						cache.WriteSessionData(id, sessionItems, item.Timeout * FROM_MIN_TO_SEC);
						LogUtility.LogInfo("SetAndReleaseItemExclusive => Session Id: {0}, Session provider object: {1} => created new item in session.", id, this.GetHashCode());
					}
					else
					{
						if (item != null && item.Items != null)
						{
							if (item.Items["SessionStateActions"] != null)
							{
								item.Items.Remove("SessionStateActions");
							}
							OpenCache();
							cache.UpdateSessionData(id, (long)lockId, (SessionStateItems)item.Items, item.Timeout * FROM_MIN_TO_SEC);
							LogUtility.LogInfo("SetAndReleaseItemExclusive => Session Id: {0}, Session provider object: {1} => updated item in session.", id, this.GetHashCode());
						}
					}
				}
			}
			catch (Exception e)
			{
				LogUtility.LogError("SetAndReleaseItemExclusive => {0}", e.ToString());
				LastException = e;
				if (config == null || config.ThrowOnError)
				{
					throw;
				}
			}
		}

		public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
		{
			try
			{
				// This check is required for unit tests to work
				int sessionTimeout;
				if (context != null && context.Session != null)
				{
					sessionTimeout = context.Session.Timeout * FROM_MIN_TO_SEC;
				}
				else
				{
					sessionTimeout = config.SessionTimeout;
				}

				if (LastException == null && lockId != null)
				{
					LogUtility.LogInfo("ReleaseItemExclusive => Session Id: {0}, Session provider object: {1} => For lockId: {2}.", id, this.GetHashCode(), lockId);
					OpenCache();
					cache.ReleaseItemExclusive(id, (long)lockId, sessionTimeout);
				}
			}
			catch (Exception e)
			{
				LogUtility.LogError("ReleaseItemExclusive => {0}", e.ToString());
				LastException = e;
				if (config == null || config.ThrowOnError)
				{
					throw;
				}
			}
		}

		public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
		{
			try
			{
				if (LastException == null && lockId != null)
				{
					LogUtility.LogInfo("RemoveItem => Session Id: {0}, Session provider object: {1}.", id, this.GetHashCode());
					OpenCache();
					cache.RemoveItem(id, (long)lockId);
				}
			}
			catch (Exception e)
			{
				LogUtility.LogError("RemoveItem => {0}", e.ToString());
				LastException = e;
				if (config == null || config.ThrowOnError)
				{
					throw;
				}
			}
		}

		public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
		{
			try
			{
				if (LastException == null)
				{
					LogUtility.LogInfo("CreateUninitializedItem => Session Id: {0}, Session provider object: {1}.", id, this.GetHashCode());
					SessionStateItems items = new SessionStateItems();
					items["SessionStateActions"] = SessionStateActions.InitializeItem;
					OpenCache();
					cache.WriteSessionData(id, items, timeout * FROM_MIN_TO_SEC);
				}
			}
			catch (Exception e)
			{
				LogUtility.LogError("CreateUninitializedItem => {0}", e.ToString());
				LastException = e;
				if (config == null || config.ThrowOnError)
				{
					throw;
				}
			}
		}

		public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
		{
			LogUtility.LogInfo("CreateNewStoreData => Session provider object: {0}.", this.GetHashCode());
			return new SessionStateStoreData(new SessionStateItems(), new HttpStaticObjectsCollection(), timeout);
		}

		public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
		{
			return false;
		}

		public override void ResetItemTimeout(HttpContext context, string id)
		{
			try
			{
				if (LastException == null)
				{
					LogUtility.LogInfo("ResetItemTimeout => Session Id: {0}, Session provider object: {1}.", id, this.GetHashCode());
					OpenCache();
					cache.ResetItemTimeout(id, config.SessionTimeout);
				}
			}
			catch (Exception e)
			{
				LogUtility.LogError("ResetItemTimeout => {0}", e.ToString());
				LastException = e;
				if (config == null || config.ThrowOnError)
				{
					throw;
				}
			}
		}

		/// <summary>
		/// Throwing an exception from a session state provider will break customer applications.
		/// If an exception occurs, store in HttpContext and return null.
		/// The user can later check LastException when received null from a session operation.
		/// </summary>
		public static Exception LastException
		{
			get
			{
				return (HttpContext.Current != null)? (Exception)HttpContext.Current.Items[lastException] : null;
			}

			set
			{
				if (HttpContext.Current != null)
				{
					HttpContext.Current.Items[lastException] = value;
				}
			}
		}
	}
}

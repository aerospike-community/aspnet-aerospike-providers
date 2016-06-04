//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See LICENSE.md in the project root for license information.
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Web;
using System.Web.SessionState;
using Aerospike.Client;

namespace Aerospike.Web
{
	public class AerospikeSessionStateProvider : SessionStateStoreProviderBase
	{
		private static ProviderConfiguration config;
		private static AerospikeCache cache;
		private static object configLock = new object();
		private static object cacheLock = new object();

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
		internal static AerospikeCache Cache
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
						if (config.UseUDF)
						{
							cache = new AerospikeCacheUDF(config);
						}
						else
						{
							cache = new AerospikeCacheDefault(config);
						}
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
			try
			{
				OpenCache();
				return cache.ReadSessionData(false, context, id, config.RequestTimeout, out locked, out lockAge, out lockId, out actions);
			}
			catch (Exception e)
			{
				if (LogUtility.Enabled)
				{
					LogUtility.LogError("GetItem: {0}", e.ToString());
				}
				throw;
			}
		}

		public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
		{
			try
			{
				OpenCache();
				return cache.ReadSessionData(true, context, id, config.RequestTimeout, out locked, out lockAge, out lockId, out actions);
			}
			catch (Exception e)
			{
				if (LogUtility.Enabled)
				{
					LogUtility.LogError("GetItemExclusive: {0}", e.ToString());
				}
				throw;
			}
		}

		public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
		{
			try
			{
				int sessionTimeout = item.Timeout * 60;

				OpenCache();

				if (newItem)
				{
					cache.WriteSessionData(id, sessionTimeout, item.Items);
				}
				else
				{
					cache.UpdateSessionData(id, (long)lockId, sessionTimeout, item.Items);
				}
			}
			catch (Exception e)
			{
				if (LogUtility.Enabled)
				{
					LogUtility.LogError("SetAndReleaseItemExclusive: {0}", e.ToString());
				}
				throw;
			}
		}

		public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
		{
			if (lockId == null)
			{
				return;
			}

			try
			{
				// This check is required for unit tests to work
				int sessionTimeout;
				if (context != null && context.Session != null)
				{
					sessionTimeout = context.Session.Timeout * 60;
				}
				else
				{
					sessionTimeout = config.SessionTimeout;
				}

				OpenCache();
				cache.ReleaseItemExclusive(id, (long)lockId, sessionTimeout);
			}
			catch (Exception e)
			{
				if (LogUtility.Enabled)
				{
					LogUtility.LogError("ReleaseItemExclusive: {0}", e.ToString());
				}
				throw;
			}
		}

		public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
		{
			if (lockId == null)
			{
				return;
			}

			try
			{
				OpenCache();
				cache.RemoveItem(id, (long)lockId);
			}
			catch (Exception e)
			{
				if (LogUtility.Enabled)
				{
					LogUtility.LogError("RemoveItem: {0}", e.ToString());
				}
				throw;
			}
		}

		public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
		{
			try
			{
				OpenCache();
				cache.CreateSessionData(id, timeout * 60);
			}
			catch (Exception e)
			{
				if (LogUtility.Enabled)
				{
					LogUtility.LogError("CreateUninitializedItem: {0}", e.ToString());
				}
				throw;
			}
		}

		public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
		{
			ISessionStateItemCollection items;

			if (config != null && config.UseUDF)
			{
				items = new SessionStateItems();
			}
			else
			{
				items = new SessionStateItemCollection();
			}
			return SessionUtility.CreateStoreData(context, items, timeout);
		}

		public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
		{
			return false;
		}

		public override void ResetItemTimeout(HttpContext context, string id)
		{
			try
			{
				OpenCache();
				cache.ResetItemTimeout(id, config.SessionTimeout);
			}
			catch (Exception e)
			{
				if (LogUtility.Enabled)
				{
					LogUtility.LogError("ResetItemTimeout: {0}", e.ToString());
				}
				throw;
			}
		}
	}
}

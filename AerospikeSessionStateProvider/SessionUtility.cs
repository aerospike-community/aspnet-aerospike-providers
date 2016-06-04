//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See LICENSE.md in the project root for license information.
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Web;
using System.Web.SessionState;
using Aerospike.Client;

namespace Aerospike.Web
{
	internal static class SessionUtility
	{
		internal static SessionStateStoreData CreateStoreData(HttpContext context, ISessionStateItemCollection items, int sessionTimeoutMinutes)
		{
			// Context will be null in unit tests.
			HttpStaticObjectsCollection staticObjects = (context != null) ? SessionStateUtility.GetSessionStaticObjects(context) : new HttpStaticObjectsCollection();
			return new SessionStateStoreData(items, staticObjects, sessionTimeoutMinutes);
		}

		internal static Dictionary<string, Value> Serialize(ISessionStateItemCollection items)
		{
			if (items == null)
			{
				return new Dictionary<string, Value>();
			}

			Dictionary<string, Value> map = new Dictionary<string, Value>(items.Keys.Count);

			using (MemoryStream memoryStream = new MemoryStream())
			{
				foreach (string key in items.Keys)
				{
					map[key] = Value.Get(Serialize(memoryStream, items[key]));
				}
			}
			return map;
		}

		internal static byte[] Serialize(MemoryStream memoryStream, object data)
		{
			if (data == null)
			{
				return null;
			}

			BinaryFormatter binaryFormatter = new BinaryFormatter();
			memoryStream.SetLength(0);
			binaryFormatter.Serialize(memoryStream, data);
			return memoryStream.ToArray();
		}

		internal static object Deserialize(byte[] bytes)
		{
			if (bytes == null)
			{
				return null;
			}

			BinaryFormatter binaryFormatter = new BinaryFormatter();

			using (MemoryStream memoryStream = new MemoryStream(bytes, 0, bytes.Length))
			{
				return (object)binaryFormatter.Deserialize(memoryStream);
			}
		}
	}
}

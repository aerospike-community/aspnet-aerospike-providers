//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See LICENSE.md in the project root for license information.
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Aerospike.Client;

namespace Aerospike.Web
{
	internal static class SessionUtility
	{
		internal static void AppendRemoveItemsInList(SessionStateItems items, List<string> list)
		{
			if (items.GetDeletedKeys() != null && items.GetDeletedKeys().Count != 0)
			{
				foreach (string delKey in items.GetDeletedKeys())
				{
					list.Add(delKey);
				}
			}
		}

		internal static void AppendUpdatedOrNewItemsInList(SessionStateItems items, Dictionary<string,Value> map)
		{
			if (items.GetModifiedKeys() != null && items.GetModifiedKeys().Count != 0)
			{
				foreach (string key in items.GetModifiedKeys())
				{
					map[key] = Value.Get(GetBytesFromObject(items[key]));
				}
			}
		}

		internal static byte[] GetBytesFromObject(object data)
		{
			if (data == null)
			{
				return null;
			}

			BinaryFormatter binaryFormatter = new BinaryFormatter();
			using (MemoryStream memoryStream = new MemoryStream())
			{
				binaryFormatter.Serialize(memoryStream, data);
				return memoryStream.ToArray();
			}
		}

		internal static object GetObjectFromBytes(byte[] dataAsBytes)
		{
			if (dataAsBytes == null)
			{
				return null;
			}

			BinaryFormatter binaryFormatter = new BinaryFormatter();
			using (MemoryStream memoryStream = new MemoryStream(dataAsBytes, 0, dataAsBytes.Length))
			{
				memoryStream.Seek(0, System.IO.SeekOrigin.Begin);
				return (object)binaryFormatter.Deserialize(memoryStream);
			}
		}
	}
}

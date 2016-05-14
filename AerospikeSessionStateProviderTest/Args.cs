using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aerospike.Web.Test
{
	[TestClass]
	public class Args
	{
		public static Args Instance = new Args();

		public string host;
		public string ns;
		public string set;
		public string user;
		public string password;

		public Args()
		{
			host = Properties.Settings.Default.Host;
			ns = Properties.Settings.Default.Namespace;
			set = Properties.Settings.Default.Set;
			user = Properties.Settings.Default.User;
			password = Properties.Settings.Default.Password;
		}
	}
}

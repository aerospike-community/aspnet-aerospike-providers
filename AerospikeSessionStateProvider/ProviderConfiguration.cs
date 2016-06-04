//
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License. See LICENSE.md in the project root for license information.
//
using System;
using System.Collections.Specialized;
using System.Configuration;
using System.IO;
using System.Reflection;
using System.Web.Configuration;
using System.Web.Hosting;

namespace Aerospike.Web
{
	internal class ProviderConfiguration
	{
		public string Host { get; set; }
		public String User { get; set; }
		public String Password { get; set; }
		public String Namespace { get; set; }
		public String Set { get; set; }
		public int ConnectionTimeout { get; set; }
		public int OperationTimeout { get; set; }
		public int MaxRetries { get; set; }
		public int SleepBetweenRetries { get; set; }
		public int MaxConnsPerNode { get; set; }
		public int MaxSocketIdle { get; set; }
		public int TendInterval { get; set; }
		public int RequestTimeout { get; set; }
		public int SessionTimeout { get; set; }
		public string ApplicationName { get; set; }
		public bool UseUDF { get; set; }

		internal ProviderConfiguration(NameValueCollection config)
		{
			EnableLoggingIfParametersAvailable(config);

			Host = GetStringSettings(config, "host", "127.0.0.1:3000");
			User = GetStringSettings(config, "user", null);
			Password = GetStringSettings(config, "password", null);
			Namespace = GetStringSettings(config, "namespace", "test");
			Set = GetStringSettings(config, "set", "test");
			ConnectionTimeout = GetIntSettings(config, "connectionTimeoutInMilliseconds", 1000);
			OperationTimeout = GetIntSettings(config, "operationTimeoutInMilliseconds", 100);
			MaxRetries = GetIntSettings(config, "maxRetries", 1);
			SleepBetweenRetries = GetIntSettings(config, "sleepBetweenRetriesInMilliseconds", 10);
			MaxConnsPerNode = GetIntSettings(config, "maxConnsPerNode", 300);
			MaxSocketIdle = GetIntSettings(config, "maxSocketIdleInSeconds", 55);
			TendInterval = GetIntSettings(config, "tendIntervalInMilliseconds", 1000);
			UseUDF = GetBoolSettings(config, "useUDF", false);

			HttpRuntimeSection httpRuntimeSection = ConfigurationManager.GetSection("system.web/httpRuntime") as HttpRuntimeSection;
			RequestTimeout = (int)httpRuntimeSection.ExecutionTimeout.TotalSeconds;

			SessionStateSection sessionStateSection = (SessionStateSection)WebConfigurationManager.GetSection("system.web/sessionState");
			SessionTimeout = (int)sessionStateSection.Timeout.TotalSeconds;

			ApplicationName = GetStringSettings(config, "applicationName", null);
			
			if (ApplicationName == null)
			{
				try
				{
					ApplicationName = HostingEnvironment.ApplicationVirtualPath;

					if (String.IsNullOrEmpty(ApplicationName))
					{
						ApplicationName = System.Diagnostics.Process.GetCurrentProcess().MainModule.ModuleName;

						int indexOfDot = ApplicationName.IndexOf('.');
						if (indexOfDot != -1)
						{
							ApplicationName = ApplicationName.Remove(indexOfDot);
						}
					}

					if (String.IsNullOrEmpty(ApplicationName))
					{
						ApplicationName = "/";
					}

				}
				catch (Exception e)
				{
					ApplicationName = "/";
					LogUtility.LogInfo(e.Message);
				}
			}

			LogUtility.LogInfo("Host: {0}, User: {1}, ConnectionTimeout: {2}, OperationTimeout: {3}, ApplicationName: {4}",
					Host, User, ConnectionTimeout, OperationTimeout, ApplicationName);
		}

		// 1) Use key available inside AppSettings
		// 2) Use literal value as given in config
		// 3) Both are null than use default value.
		private static string GetStringSettings(NameValueCollection config, string attrName, string defaultVal)
		{
			string literalValue = GetFromConfig(config, attrName);
			if (string.IsNullOrEmpty(literalValue))
			{
				return defaultVal;
			}

			string appSettingsValue = GetFromAppSetting(literalValue);
			if (!string.IsNullOrEmpty(appSettingsValue))
			{
				return appSettingsValue;
			}
			return literalValue;
		}

		// 1) Check if literal value is valid integer than use it as it is
		// 2) Use app setting value corrosponding to this string
		// 3) Both are null than use default value.
		private static int GetIntSettings(NameValueCollection config, string attrName, int defaultVal)
		{
			string literalValue = null;
			try
			{
				literalValue = GetFromConfig(config, attrName);
				if (literalValue == null)
				{
					return defaultVal;
				}
				return int.Parse(literalValue);
			}
			catch (FormatException)
			{ }

			string appSettingsValue = GetFromAppSetting(literalValue);
			if (appSettingsValue == null)
			{
				// This will blow up but gives right message to customer
				return int.Parse(literalValue);
			}
			return int.Parse(appSettingsValue);
		}

		// 1) Check if literal value is valid bool than use it as it is
		// 2) Use app setting value corrosponding to this string
		// 3) Both are null than use default value.
		private static bool GetBoolSettings(NameValueCollection config, string attrName, bool defaultVal)
		{
			string literalValue = null;
			try
			{
				literalValue = GetFromConfig(config, attrName);
				if (literalValue == null)
				{
					return defaultVal;
				}
				return bool.Parse(literalValue);
			}
			catch (FormatException)
			{ }

			string appSettingsValue = GetFromAppSetting(literalValue);
			if (appSettingsValue == null)
			{
				// This will blow up but gives right message to customer
				return bool.Parse(literalValue);
			}
			return bool.Parse(appSettingsValue);
		}

		// Reads value from app settings (mostly azure app settings)
		private static string GetFromAppSetting(string attrName)
		{
			if (!string.IsNullOrEmpty(attrName))
			{
				string paramFromAppSetting = ConfigurationManager.AppSettings[attrName];
				if (!string.IsNullOrEmpty(paramFromAppSetting))
				{
					return paramFromAppSetting;
				}
			}
			return null;
		}

		// Reads string value from web.config session state section
		private static string GetFromConfig(NameValueCollection config, string attrName)
		{
			string[] attrValues = config.GetValues(attrName);
			if (attrValues != null && attrValues.Length > 0 && !string.IsNullOrEmpty(attrValues[0]))
			{
				return attrValues[0];
			}
			return null;
		}

		internal static void EnableLoggingIfParametersAvailable(NameValueCollection config)
		{
			string LoggingClassName = GetStringSettings(config, "loggingClassName", null);
			string LoggingMethodName = GetStringSettings(config, "loggingMethodName", null);

			if (!string.IsNullOrEmpty(LoggingClassName) && !string.IsNullOrEmpty(LoggingMethodName))
			{
				// Find 'Type' that is same as fully qualified class name if not found than also don't throw error and ignore case while searching
				Type LoggingClass = Type.GetType(LoggingClassName, throwOnError: false, ignoreCase: true);

				if (LoggingClass == null)
				{
					// If class name is not assembly qualified name than look for class in all assemblies one by one
					LoggingClass = GetClassFromAssemblies(LoggingClassName);
				}

				if (LoggingClass == null)
				{
					// All ways of loading assembly are failed so throw
					throw new TypeLoadException(string.Format(Aerospike.Web.Properties.Resources.ClassNotFound, LoggingClassName));
				}

				MethodInfo LoggingMethod = LoggingClass.GetMethod(LoggingMethodName, new Type[] { });
				if (LoggingMethod == null)
				{
					throw new MissingMethodException(string.Format(Aerospike.Web.Properties.Resources.MethodNotFound, LoggingMethodName, LoggingClassName));
				}
				if ((LoggingMethod.Attributes & MethodAttributes.Static) == 0)
				{
					throw new MissingMethodException(string.Format(Aerospike.Web.Properties.Resources.MethodNotStatic, LoggingMethodName, LoggingClassName));
				}
				if (!(typeof(System.IO.TextWriter)).IsAssignableFrom(LoggingMethod.ReturnType))
				{
					throw new MissingMethodException(string.Format(Aerospike.Web.Properties.Resources.MethodWrongReturnType, LoggingMethodName, LoggingClassName, "System.IO.TextWriter"));
				}
				LogUtility.logger = (TextWriter)LoggingMethod.Invoke(null, new object[] { });
			}
		}

		private static Type GetClassFromAssemblies(string ClassName)
		{
			foreach (Assembly a in AppDomain.CurrentDomain.GetAssemblies())
			{
				// If class name is not assembly qualified name than look for class name in all assemblies one by one
				Type ClassType = a.GetType(ClassName, throwOnError: false, ignoreCase: true);
				if (ClassType == null)
				{
					// If class name is not assembly qualified name and it also doesn't contain namespace (it is just class name) than
					// try to use assembly name as namespace and try to load class from all assemblies one by one 
					ClassType = a.GetType(a.GetName().Name + "." + ClassName, throwOnError: false, ignoreCase: true);
				}
				if (ClassType != null)
				{
					return ClassType;
				}
			}
			return null;
		}
	}
}

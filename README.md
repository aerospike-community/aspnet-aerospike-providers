Aerospike ASP.NET Session State Provider
========================================

Aerospike implementation of the ASP.NET Session State Provider.  Aerospike Session State Provider is used to store ASP.NET application session state in an Aerospike distributed database cluster.

## Installation

Aerospike Session State Provider can be installed via [Nuget](https://www.nuget.org/packages/Aerospike.SessionStateProvider) or built/installed via this repository.

## Configuration

Configure `web.config` to use Aerospike's session state provider.

```xml
<system.web>
  <sessionState mode="Custom" customProvider="AerospikeSessionStateProvider">
    <providers>
      <add name="AerospikeSessionStateProvider" 
           type="Aerospike.Web.AerospikeSessionStateProvider" 
           host="host1:3000,host2:3000"
		   namespace="test"
		   set="test"
		   />
    </providers>
  </sessionState>
</system.web>
```

The full set of Aerospike configuration arguments are:

Name | Default | Description
---- | ------- | -----------
host      | localhost:3000 | HostName/Port combinations separated by commas.
user      |                | User name for servers configured with authentication.
password  |                | Password for servers configured with authentication.
namespace | test           | Namespace to store session data.
set       | test           | Set name to store session data.
connectionTimeoutInMilliseconds   | 1000 | Max milliseconds allowed to make socket connection to an Aerospike server.
operationTimeoutInMilliseconds    | 100  | Max milliseconds allowed to read or write session data.
maxRetries                        | 1    | Max number of retries if read or write fails.
sleepBetweenRetriesInMilliseconds | 10   | Milliseconds to sleep before attempting a retry.
maxConnsPerNode                   | 300  | Max number of connections allowed per Aerospike server node.
maxSocketIdleInSeconds            | 55   | Max seconds sockets are allowed to stay unused in connection pool.
tendIntervalInMilliseconds        | 1000 | Milliseconds between cluster tend requests to determine cluster state.
throwOnError                      | true | Throw exception on error.  If false, the error can later be accessed by AerospikeSessionStateProvider.LastException

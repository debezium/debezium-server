# Debezium Server

Debezium Server is a standalone Java application built on Qurkus framework.
The application itself contains the `core` module and a set of modules responsible for communication with different target systems.

The per-module integration tests depend on the availability of the external services.
It is thus recommended to execute integration tests per-module and set-up necessary pre-requisities beforehand.

Note: running these tests against external infrastructure may incur cost with your cloud provider.
We're not going to pay your AWS/GCP/Azure bill.

## Table of Contents

* [Amazon Kinesis](#amazon-kinesis)
* [Google Cloud Pub/Sub](#google-cloud-pubsub)
* [Azure Event Hubs](#azure-event-hubs)
* [Pravega](#pravega)

## Amazon Kinesis

* Execute `aws configure` as described in AWS CLI [getting started](https://github.com/aws/aws-cli#getting-started) guide and setup the account.
* Create Kinesis stream `aws kinesis create-stream --stream-name testc.inventory.customers --shard-count 1`
* Build the module and execute the tests `mvn clean install -DskipITs=false -am -pl debezium-server-kinesis`
* Remove the stream `aws kinesis delete-stream --stream-name testc.inventory.customers`

## Google Cloud Pub/Sub

* Login into your Google Cloud account using `gcloud auth application-default login` as described in the [documentation](https://cloud.google.com/sdk/gcloud/reference/auth/application-default).
* Build the module and execute the tests `mvn clean install -DskipITs=false -am -pl debezium-server-pubsub`

## Azure Event Hubs

Login into your Azure account and create a resource group, e.g. on the CLI:

```shell
az login
az group create --name eventhubstest --location westeurope
```

### Create an Event Hubs namespace

Create an [Event Hubs namespace](https://docs.microsoft.com/azure/event-hubs/event-hubs-features#namespace). Check the documentation for options on how do this using the [Azure Portal](https://docs.microsoft.com/azure/event-hubs/event-hubs-create#create-an-event-hubs-namespace), [Azure CLI](https://docs.microsoft.com/azure/event-hubs/event-hubs-quickstart-cli#create-an-event-hubs-namespace) etc., e.g. on the CLI:

```shell
az eventhubs namespace create --name debezium-test --resource-group eventhubstest -l westeurope
```

### Create an Event Hub

Create an Event Hub (equivalent to a topic) with `one` partition. Check the documentation for options on how do this using the [Azure Portal](https://docs.microsoft.com/azure/event-hubs/event-hubs-create#create-an-event-hub), [Azure CLI](https://docs.microsoft.com/azure/event-hubs/event-hubs-quickstart-cli#create-an-event-hub) etc. , e.g. on the CLI:

```shell
az eventhubs eventhub create --name debezium-test-hub --resource-group eventhubstest --namespace-name debezium-test
```

### Build the module

[Get the Connection string](https://docs.microsoft.com/azure/event-hubs/event-hubs-get-connection-string) required to communicate with Event Hubs. The format is: `Endpoint=sb://<NAMESPACE>/;SharedAccessKeyName=<ACCESS_KEY_NAME>;SharedAccessKey=<ACCESS_KEY_VALUE>`.
E.g. on the CLI:

```shell
az eventhubs namespace authorization-rule keys list --resource-group eventhubstest --namespace-name debezium-test --name RootManageSharedAccessKey
```

Set environment variables required for tests:

```shell
export EVENTHUBS_CONNECTION_STRING=<Event Hubs connection string>
export EVENTHUBS_NAME=<name of the Event hub created in previous step>
```

Execute the tests:

```shell
mvn clean install -DskipITs=false -Deventhubs.connection.string=$EVENTHUBS_CONNECTION_STRING -Deventhubs.hub.name=$EVENTHUBS_NAME -am -pl :debezium-server-eventhubs
```

### Examine Events in the Event Hub

E.g. using kafkacat. Create _kafkacat.conf_:

```shell
metadata.broker.list=debezium-test.servicebus.windows.net:9093
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=$ConnectionString
sasl.password=Endpoint=sb://debezium-test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<access key>
```

Start consuming events:

export KAFKACAT_CONFIG=<path to kafkacat.conf>
kafkacat -b debezium-test.servicebus.windows.net:9093 -t debezium-test-hub

### Clean up

Delete the Event Hubs namespace and log out, e.g. on the CLI:

```shell
az group delete -n eventhubstest
az logout
```

## Pravega

[Pravega](https://pravega.io/) is a cloud-native storage system for event streams and data streams. This sink offers two modes: non-transactional and transactional. The non-transactional mode individually writes each event in a Debezium batch to Pravega. The transactional mode writes the Debezium batch to a Pravega transaction that commits when the batch is completed.

The Pravega sink expects destination scope and streams to already be created.

The Pravega sink uses Pravega 0.13.0 Client API and supports Pravega versions 0.11.0 and above.

### `conf/application.properties` Configuration

|Property|Default|Description|
|--------|-------|-----------|
|`debezium.sink.type`||Must be set to `pravega`.|
|`debezium.sink.pravega.controller.uri`|`tcp://localhost:9090`|The connection string to a Controller in the Pravega cluster.|
|`debezium.sink.pravega.scope`||The name of the scope in which to find the destination streams.|
|`debezium.sink.pravega.transaction`|`false`|Set to `true` to have the sink use Pravega transactions for each Debezium batch.|

### CDI Injection Points

Pravega sink behavior can be modified by custom logic providing alternative implementations for specific functionalities. When the alternative implementations are not available then the default ones are used.

|Interface|Description|
|---------|-----------|
|`io.debezium.server.StreamNameMapper`|A custom implementation maps the planned destination stream name into a physical Pravega stream name. By default the same name is used.|

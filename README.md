PHP Native Apache Kafka Client — 0.8.x
=======================================

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lisachenko/kafka-client/ci.yml?branch=0.8.x)
[![Code Coverage](https://img.shields.io/codecov/c/github/lisachenko/kafka-client/0.8.x)](https://app.codecov.io/gh/lisachenko/kafka-client)
[![Minimum PHP Version](http://img.shields.io/badge/php-%3E%3D%208.4-8892BF.svg)](https://www.php.net/supported-versions.php)
[![License](https://img.shields.io/packagist/l/lisachenko/kafka-client.svg)](https://packagist.org/packages/lisachenko/kafka-client)

`lisachenko/kafka-client` is a native, pure-PHP implementation of the Apache Kafka wire
protocol — no `ext-rdkafka` required. It ships a Producer, a Consumer and a low-level Admin
client, designed to stay close in spirit to the official Java client's API while feeling
natural in PHP.

**This branch speaks the Apache Kafka 0.8.2.2 wire protocol** — the last release of the 0.8
line — and nothing else. The API of the classes is the one of the `main` branch wherever
0.8 has the same concept, so code written against `main` mostly compiles here; what the 0.8
protocol cannot do is simply absent, and [what that is](#supported-kafka-protocol-versions)
is listed below. The grammar this branch implements is written down, byte for byte, in
[docs/protocol/0.8.2.md](docs/protocol/0.8.2.md).

Installation
------------

```bash
composer require lisachenko/kafka-client:^0.8
```

Producer API
------------

The Producer API sends streams of records to topics in the Kafka cluster.

```php
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;
use Protocol\Kafka\Producer\RecordMetadata;

require __DIR__ . '/vendor/autoload.php';

$producer = new KafkaProducer([
    ProducerConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092'],
    ProducerConfig::ACKS              => 1,
]);

$producer->send('test', new Record('foo'))->then(
    function (RecordMetadata $metadata): void {
        echo "Written to partition {$metadata->partition} at offset {$metadata->offset}\n";
    }
);
$producer->flush();
```

`send(string $topic, Record $record, ?int $partition = null): Promise` buffers the record and
returns a promise that is resolved with a `RecordMetadata` once `flush()` has sent the batch
and the broker has acknowledged it. The only required option is
`ProducerConfig::BOOTSTRAP_SERVERS`; for every other option see the constants documented on
`Protocol\Kafka\Producer\ProducerConfig` and the [producer configuration] reference.

`ProducerConfig::ACKS` selects the durability of a write: `0` sends fire-and-forget (the
0.8 broker sends **no response at all** for such a request, so the promise resolves with the
offset `-1`), `1` waits for the leader's log and `-1` for all in-sync replicas. Records are
collected until they fill `ProducerConfig::BATCH_SIZE` bytes or `ProducerConfig::LINGER_MS`
has passed, and a batch that fails with a retriable error is sent again `ProducerConfig::RETRIES`
times — that option is the whole retry budget of a batch and defaults to no retry at all, like
the Java producer. Compression is set with `ProducerConfig::COMPRESSION_TYPE` and applies to a
whole batch; 0.8.2.2 supports `gzip` and `snappy` (`lz4` exists in the broker but is not
implemented by this client).

Without a key a record is spread over the partitions that have a leader, with a key it goes to
the partition that the murmur2 hash of the key selects, exactly as with the official Java
client (`Producer\DefaultPartitioner`); an explicit partition can be passed to `send()`.

A runnable version of this is [examples/producer.php](examples/producer.php).

Consumer API
------------

The Consumer API reads streams of records from topics in the Kafka cluster.

```php
use Protocol\Kafka\Common\TopicPartition;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;

$consumer = new KafkaConsumer([
    ConsumerConfig::BOOTSTRAP_SERVERS   => ['tcp://127.0.0.1:9092'],
    ConsumerConfig::GROUP_ID            => 'kafka-daemon',
    ConsumerConfig::FETCH_MAX_WAIT_MS   => 5000,
    ConsumerConfig::AUTO_OFFSET_RESET   => OffsetResetStrategy::EARLIEST,
    ConsumerConfig::METADATA_CACHE_FILE => '/tmp/metadata.php',
]);

// Kafka 0.8 has no broker-side group membership, so the partitions are assigned by the client
$consumer->assign([new TopicPartition('test', 0), new TopicPartition('test', 1)]);

for ($i = 0; $i < 100; $i++) {
    foreach ($consumer->poll(1000) as $record) {
        echo json_encode($record), PHP_EOL;
    }
    $consumer->commitSync();
}
```

`assign()` picks the partitions to read, `poll($timeoutMs)` fetches the next records from them,
`commitSync()` stores the current position of the group on the broker, and
`seek()`/`seekToBeginning()`/`seekToEnd()` move the position. The offsets a group committed
survive the process, so the next `poll()` continues where the last `commitSync()` left off.

**Group membership is a client-side concern on this branch.** The JoinGroup, SyncGroup,
Heartbeat and LeaveGroup requests only arrived with Kafka 0.9: a 0.8 broker stores the
*offsets* of a group but never assigns partitions to its members, so two consumers of the same
group that assign the same partition will both read it. The Kafka 0.8 answer to that is
ZooKeeper-based coordination, which this client, as a pure broker client, does not do.

See the [consumer configuration] reference for the full set of options and
[examples/consumer.php](examples/consumer.php) for a runnable version.

Admin API
---------

The Admin API exposes the low-level cluster operations a 0.8.2.2 broker can serve:

```php
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

$configuration = [ClientConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092']];
$admin         = new AdminClient(Cluster::bootstrap($configuration), $configuration);

$brokers  = $admin->findAllBrokers();                       // Node[], indexed by the node id
$topics   = $admin->listTopics();                           // string[]
$metadata = $admin->describeTopics(['test']);               // TopicMetadata[], indexed by the topic
$offsets  = $admin->listOffsets(['test' => [0, 1, 2]]);     // topic => partition => [offset]
$earliest = $admin->listOffsets(['test' => [0]], OffsetsRequest::EARLIEST);

$coordinator = $admin->findCoordinator('kafka-daemon');     // Node that holds the group offsets
$committed   = $admin->listGroupOffsets('kafka-daemon', ['test' => [0, 1, 2]]);
```

| Method                                       | Wire API                | Notes                                                                |
|----------------------------------------------|-------------------------|----------------------------------------------------------------------|
| `findAllBrokers()`                           | Metadata v0             | An empty result means "the cluster is not ready yet", see below      |
| `listTopics()` / `describeTopics()`          | Metadata v0             | **Creates** an unknown topic when `auto.create.topics.enable` is on   |
| `listOffsets()`                              | Offsets v0              | Earliest, latest or by segment timestamp; sent to the partition leader |
| `findCoordinator()`                          | GroupCoordinator v0     | Retries the codes 15 and 14 while the coordinator warms up            |
| `listGroupOffsets()`                         | OffsetFetch v0/v1       | The partitions are explicit: 0.8 has no "all topics" request          |
| `controlledShutdown()`                       | ControlledShutdown v0   | Moves every partition leader off a broker — it really does stop it    |

Three methods of `main`'s `AdminClient` are **not** on this branch, because the api keys do
not exist in Kafka 0.8.2.2 at all — a broker closes the connection when it receives them:
`describeGroup()` (DescribeGroups, key 15, Kafka 0.9), `listGroups()`/`listAllGroups()`
(ListGroups, key 16, Kafka 0.9) and `getApiVersions()` (ApiVersions, key 18, Kafka 0.10).
Consumer groups are managed through ZooKeeper in 0.8, so a broker has nothing to say about
their membership — the committed offsets that `listGroupOffsets()` reads are the only group
state it knows.

There is no CreateTopics api either (that is Kafka 0.10.1). A topic is created by writing to
ZooKeeper — `kafka-topics.sh --create` — or implicitly by asking for the metadata of a topic
that does not exist while the broker runs with `auto.create.topics.enable=true`. That first
Metadata answer carries the topic error code 5 (`LeaderNotAvailable`) and an empty partition
list until the controller has elected the leaders, so a client has to ask again.

[examples/admin.php](examples/admin.php) runs all of it against the broker of
`docker-compose.yml`.

Network client
--------------

One connection per broker is opened on demand and kept open for the requests that follow, the way a Kafka connection
is meant to be used: it is an ordered request/response channel, and every request carries a correlation id that the
broker echoes back. The client generates that id, checks it on every answer and drops a connection whose answer does
not match — its stream position would be unknown from then on. `Protocol\Kafka\Common\Node::closeConnections()`
closes every connection of the process, which a long-running worker can call when it goes idle.

Three options steer this:

- `connections.max.idle.ms` — a cached connection that was unused for longer is re-opened instead of handed out, because
  the broker closes idle connections on its side and a half-closed socket would only surface mid-request.
- `metadata.max.age.ms` — how long the cluster metadata (and a `metadata.cache.file`, if configured) stays valid before
  it is fetched again.
- `retries` and `retry.backoff.ms` — how often a request that failed with something a metadata refresh can cure is
  refreshed and sent again: the error codes 3 (UnknownTopicOrPartition, e.g. a topic that was only just auto-created),
  5 (LeaderNotAvailable, an election is in progress) and 6 (NotLeaderForPartition, the cached leader moved), plus a
  dropped connection. Every other error is final and reaches the caller straight away.

A request that fans out over several partition leaders can fail for some partitions and succeed for others. That is
reported as a `Common\Errors\TopicPartitionRequestException`, which carries both halves: `getPartialResult()` holds
the topic-partitions that did work and `getExceptions()` the exception of each one that did not, indexed by topic and
partition.

The Admin API uses the same connections and the same correlation id checks; it does not retry, but every request that
any broker can answer — Metadata, ControlledShutdown and the ZooKeeper-backed OffsetFetch v0 — is tried on the brokers
of the cluster in turn until one of them answers.

PHP-specific configuration
---------------------------

A few configuration options exist purely to make the client work well under PHP's
process-per-request model:

- `metadata.cache.file` — file used to cache cluster metadata; effectively cached by opcache in production.
- `stream.async.connect` — whether to connect to brokers asynchronously.
- `stream.persistent.connection` — whether to keep a persistent connection to the cluster.

For publishing from web requests, enabling persistent connections together with a metadata
cache file keeps producing as fast as possible.

One more option matters on this branch: `offsets.storage` selects where the offsets of a
consumer group live. `kafka` (the default) commits and fetches them with version 1 of the
OffsetCommit/OffsetFetch apis, which Kafka 0.8.2 introduced and which stores them in the
`__consumer_offsets` topic; `zookeeper` uses version 0 of the same apis, which stores them in
ZooKeeper the way Kafka 0.8.1 did. Nothing else in this client differs between the two.

There is **no transport security at all** in Kafka 0.8: SSL and SASL arrived with 0.9, so this
branch has no `security.protocol`, no `ssl.*` options and no authentication mechanism. Keep a
0.8 cluster on a trusted network.

Supported Kafka protocol versions
----------------------------------

This branch tracks the **Kafka 0.8.2.2** wire protocol. `main` tracks Kafka 0.11; the frozen
protocol snapshots of the older lines live on `0.10.x` (Kafka 0.10.0), `0.9.x` (Kafka 0.9.0)
and this branch, `0.8.x`.

| Api key | API                | Versions here | Client-facing | Implemented |
|---------|--------------------|---------------|---------------|-------------|
| 0       | Produce            | v0            | yes           | yes         |
| 1       | Fetch              | v0            | yes           | yes         |
| 2       | Offsets            | v0            | yes           | yes         |
| 3       | Metadata           | v0            | yes           | yes         |
| 4       | LeaderAndIsr       | v0            | broker→broker | no          |
| 5       | StopReplica        | v0            | broker→broker | no          |
| 6       | UpdateMetadata     | v0            | broker→broker | no          |
| 7       | ControlledShutdown | v0            | controller    | yes         |
| 8       | OffsetCommit       | v0, v1        | yes           | yes         |
| 9       | OffsetFetch        | v0, v1        | yes           | yes         |
| 10      | GroupCoordinator   | v0            | yes           | yes         |

Everything a later Kafka added is therefore missing here, by design:

| Feature                                        | Arrived in | On this branch                                   |
|------------------------------------------------|------------|--------------------------------------------------|
| Group membership (JoinGroup … LeaveGroup)      | 0.9        | no — assign the partitions yourself               |
| DescribeGroups / ListGroups                    | 0.9        | no                                                |
| SSL and SASL                                   | 0.9        | no                                                |
| `ThrottleTime` in responses, quotas            | 0.9        | no                                                |
| Nullable topic array of OffsetFetch/Metadata   | 0.9 / 0.10 | no — partitions and topics are always explicit    |
| Message format v1 with a timestamp, LZ4        | 0.10       | no — message format v0, gzip and snappy only      |
| ApiVersions, CreateTopics/DeleteTopics         | 0.10       | no — topics are created through ZooKeeper         |
| Offsets by timestamp (Offsets v1)              | 0.10.1     | no — the segment-based v0 only                    |
| Record batches v2, idempotence, transactions   | 0.11       | no                                                |
| Error codes above 20                           | 0.9+       | no — 0.8.2.2 defines -1 … 20                      |

Two properties of a 0.8.2.2 broker regularly surprise clients, and this implementation deals
with both explicitly:

* **A broker that has just started answers Metadata with an empty broker array.** The broker
  list comes from a metadata cache that stays empty until the controller pushes an
  `UpdateMetadata` to it, and on a cluster without a single topic that never happens on its own.
  An empty broker array is "not ready, retry", never "the cluster has no brokers" — a TCP
  health check on port 9092 does not tell them apart. Asking for a topic (which auto-creates
  it) unblocks the cache. See `tests/Fixture/ClusterReadinessProbe.php`.
* **The coordinator of a group is not available right away.** The first GroupCoordinator
  request for any group makes the broker create the internal `__consumer_offsets` topic and is
  answered with the error code 15 while that happens; code 14 means the coordinator is still
  reading the offsets of the group out of it. Both are retried with `retry.backoff.ms` until
  `metadata.fetch.timeout.ms` by `Common\CoordinatorLookup`, which
  `AdminClient::findCoordinator()` and `Client::getGroupCoordinator()` use.

A third one only shows up in the Admin API: `controlledShutdown()` for a broker id the
controller does not know is answered with the error code **-1 (Unknown)** instead of 8
(BrokerNotAvailable), because the broker maps the *cause* of an exception that has none. The
real reason is in the broker log. The protocol document has the details.

Testing & Contributing
-----------------------

```bash
composer install
composer check   # coding standards + static analysis + PHPUnit
```

The suite is split in three:

```bash
vendor/bin/phpunit --testsuite unit          # pure unit tests, no broker
vendor/bin/phpunit --testsuite compliance    # replays the documented wire vectors

docker compose up -d                         # Kafka 0.8.2.2, broker on 127.0.0.1:9092
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 vendor/bin/phpunit --testsuite integration
```

The integration suite is skipped unless `KAFKA_BOOTSTRAP_SERVERS` points at a running broker.
The compliance suite replays every wire vector of
[docs/protocol/vectors](docs/protocol/vectors) — frames that a real Kafka 0.8.2.2 broker sent
or accepted — through the request and response classes and checks that the annotated dumps of
[docs/protocol/0.8.2.md](docs/protocol/0.8.2.md) still hold the same bytes, so the document and
the code cannot drift apart.

Issues and pull requests are welcome.

License
-------

Released under the [MIT license](LICENSE).

[producer configuration]: https://kafka.apache.org/documentation/#producerconfigs
[consumer configuration]: https://kafka.apache.org/documentation/#newconsumerconfigs

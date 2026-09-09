PHP Native Apache Kafka Client — 0.10.x
========================================

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lisachenko/kafka-client/ci.yml?branch=0.10.x)
[![Code Coverage](https://img.shields.io/codecov/c/github/lisachenko/kafka-client/0.10.x)](https://app.codecov.io/gh/lisachenko/kafka-client)
[![Minimum PHP Version](http://img.shields.io/badge/php-%3E%3D%208.4-8892BF.svg)](https://www.php.net/supported-versions.php)
[![License](https://img.shields.io/packagist/l/lisachenko/kafka-client.svg)](https://packagist.org/packages/lisachenko/kafka-client)

`lisachenko/kafka-client` is a native, pure-PHP implementation of the Apache Kafka wire
protocol — no `ext-rdkafka` required. It ships a Producer, a Consumer and a low-level Admin
client, designed to stay close in spirit to the official Java client's API while feeling
natural in PHP.

**This branch speaks the Apache Kafka 0.10.2.2 wire protocol** — the last release of the 0.10
line, so it covers everything 0.10.0, 0.10.1 and 0.10.2 added — and nothing else. The API of the
classes is the one of the `main` branch wherever 0.10 has the same concept, so code written
against `main` mostly compiles here; what the 0.10 protocol cannot do is simply absent, and
[what that is](#supported-kafka-protocol-versions) is listed below. The grammar this branch
implements is written down, byte for byte, in [docs/protocol/0.10.2.md](docs/protocol/0.10.2.md).

Installation
------------

```bash
composer require lisachenko/kafka-client:^0.10
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
        echo "The broker throttled the batch for {$metadata->throttleTimeMs} ms\n";
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
broker sends **no response at all** for such a request, so the promise resolves with the
offset `-1`), `1` waits for the leader's log and `-1` for all in-sync replicas. Records are
collected until they fill `ProducerConfig::BATCH_SIZE` bytes or `ProducerConfig::LINGER_MS`
has passed, and a batch that fails with a retriable error is sent again `ProducerConfig::RETRIES`
times — that option is the whole retry budget of a batch and defaults to no retry at all, like
the Java producer. Compression is set with `ProducerConfig::COMPRESSION_TYPE` and applies to a
whole batch; 0.9.0.1 supports `gzip` and `snappy` (`lz4` exists in the broker but is not
implemented by this client).

Without a key a record is spread over the partitions that have a leader, with a key it goes to
the partition that the murmur2 hash of the key selects, exactly as with the official Java
client (`Producer\DefaultPartitioner`); an explicit partition can be passed to `send()`.

`RecordMetadata::$throttleTimeMs` is the `ThrottleTime` that version 1 of the Produce API added
in Kafka 0.9: the number of milliseconds the broker delayed the answer of that batch because the
`client.id` exceeded its `producer_byte_rate` quota. Quotas never reject a write — the records
are appended and only the response is held back — so the field is informational, and it is `0`
on a broker without quotas as well as for a fire-and-forget batch (`ACKS => 0`), which is never
answered. The consumer side is the same: every `Common\FetchedPartition` of
`Client::fetchPartitions()` carries the `throttleTimeMs` of the Fetch v1 answer it came in.
Quotas are set per client id on a running broker, e.g.

```console
$ kafka-configs.sh --zookeeper localhost:2181 --alter \
    --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048' \
    --entity-type clients --entity-name my-application
```

A runnable version of this is [examples/producer.php](examples/producer.php).

Consumer API
------------

The Consumer API reads streams of records from topics in the Kafka cluster. Kafka 0.9 moved the
coordination of a consumer group into the broker, so a consumer can simply **subscribe** to topics
and let the group hand out the partitions:

```php
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Consumer\ConsumerConfig;
use Protocol\Kafka\Consumer\KafkaConsumer;
use Protocol\Kafka\Consumer\OffsetResetStrategy;

$consumer = new KafkaConsumer([
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092'],
    // A JoinGroup is answered only once the whole rebalance is over, so this has to exceed
    // session.timeout.ms; the consumer defaults are 40000 and 30000, as in the Java client
    ClientConfig::REQUEST_TIMEOUT_MS => 40000,

    ConsumerConfig::GROUP_ID                      => 'kafka-daemon',
    ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => 'range', // or 'roundrobin', or your own class
    ConsumerConfig::SESSION_TIMEOUT_MS            => 30000,
    ConsumerConfig::HEARTBEAT_INTERVAL_MS         => 3000,
    ConsumerConfig::AUTO_OFFSET_RESET             => OffsetResetStrategy::EARLIEST,
]);

// Nothing is sent yet: the group is joined by the first poll(), which brings the assignment
$consumer->subscribe(['test']);

while (true) {
    // [topic][partition] => records, in offset order
    foreach ($consumer->poll(1000) as $topic => $partitions) {
        foreach ($partitions as $partition => $records) {
            foreach ($records as $record) {
                echo $topic, ':', $partition, '@', $record->offset, ' ', $record->value, PHP_EOL;
            }
        }
    }
    $consumer->commitSync();
}

$consumer->close(); // commits once more and leaves the group with a LeaveGroup request
```

`subscribe()` names the topics and `assignment()` reports the partitions the group gave this
member; `assign()` still picks partitions by hand and joins no group at all, and the two are
mutually exclusive, exactly as in the Java client. `commitSync()` stores the position of the
group — carrying the member id and the generation of this consumer, so a coordinator refuses a
commit of a generation that is over — and `seek()`/`seekToBeginning()`/`seekToEnd()` move the
position. `unsubscribe()` leaves the group without committing, `close()` commits first.

**The heartbeat is sent from `poll()`, because PHP has no background thread.** A consumer that
does not poll for longer than `session.timeout.ms` is dropped by the coordinator and its
partitions are given to the other members; the next `poll()` sees that in the error code of its
heartbeat and joins the group again. Keep the processing of a batch well below the session
timeout, or raise `session.timeout.ms` — within the `group.min.session.timeout.ms` and
`group.max.session.timeout.ms` of the broker. There is no `max.poll.interval.ms` on this line,
that option arrived with Kafka 0.10.1.

The partitions are distributed by the member the coordinator elected as the leader of the
generation: `partition.assignment.strategy` selects `range` (the default) or `roundrobin` —
both with the ordering rules of the Java client of 0.9.0.1, so a PHP member can lead a group of
Java members and the other way round — or names a class that implements
`Consumer\PartitionAssignorInterface`. Every member of a group has to offer the same one, a
coordinator that finds no common protocol refuses the join with the error 23. A
`Consumer\ConsumerRebalanceListener` passed to `subscribe()` is called with the partitions that
each rebalance takes away and hands over, which is where a consumer with `enable.auto.commit`
off commits what it has consumed.

[examples/consumer-group.php](examples/consumer-group.php) is a runnable version of this —
start it twice and watch the two members split the partitions — and
[examples/consumer.php](examples/consumer.php) is the same thing with `assign()`.

See the [consumer configuration] reference for the full set of options.

Admin API
---------

The Admin API exposes the low-level cluster operations a 0.10.2.2 broker can serve:

```php
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

$configuration = [ClientConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092']];
$admin         = new AdminClient(Cluster::bootstrap($configuration), $configuration);

$brokers  = $admin->findAllBrokers();                       // Node[], indexed by the node id
$apis     = $admin->getApiVersions($brokers[0]);            // api key => ApiVersionsResponseMetadata
$topics   = $admin->listTopics();                           // string[]
$metadata = $admin->describeTopics(['test']);               // TopicMetadata[], indexed by the topic
$offsets  = $admin->listOffsets(['test' => [0, 1, 2]]);     // topic => partition => [offset]
$earliest = $admin->listOffsets(['test' => [0]], OffsetsRequest::EARLIEST);

$coordinator = $admin->findCoordinator('kafka-daemon');     // Node that holds the group offsets
$committed   = $admin->listGroupOffsets('kafka-daemon', ['test' => [0, 1, 2]]);

$groups = $admin->listAllGroups();                          // group id => ListGroupResponseProtocol
$groups = $admin->listGroups($coordinator);                 // only the groups of that one broker

$group = $admin->describeGroup('kafka-daemon');             // DescribeGroupResponseMetadata
echo $group->state;                                         // Stable, AwaitingSync, PreparingRebalance, Empty or Dead
echo $group->protocol;                                      // the assignor, only while the group is stable
foreach ($group->members as $memberId => $member) {
    echo $memberId, ' ', $member->clientId, ' ', $member->clientHost, PHP_EOL;
    // $member->memberMetadata and $member->memberAssignment are the opaque bytes of the protocol type
}
```

| Method                                       | Wire API                | Notes                                                                |
|----------------------------------------------|-------------------------|----------------------------------------------------------------------|
| `getApiVersions()`                           | ApiVersions v0          | The version range of every api of **one** broker, indexed by api key  |
| `findAllBrokers()`                           | Metadata v0             | An empty result means "the cluster is not ready yet", see below      |
| `listTopics()` / `describeTopics()`          | Metadata v0             | **Creates** an unknown topic when `auto.create.topics.enable` is on   |
| `listOffsets()`                              | Offsets v0              | Earliest, latest or by segment timestamp; sent to the partition leader |
| `findCoordinator()`                          | GroupCoordinator v0     | Retries the codes 15 and 14 while the coordinator warms up            |
| `listGroupOffsets()`                         | OffsetFetch v0/v1       | The partitions are explicit: 0.9 has no "all topics" request          |
| `listGroups()` / `listAllGroups()`           | ListGroups v0           | A broker only knows its own groups; `listAllGroups()` merges them all  |
| `describeGroup()` / `describeGroups()`       | DescribeGroups v0       | Sent to the coordinator of the group; an unknown group answers `Dead`, one whose last member left `Empty` |
| `controlledShutdown()`                       | ControlledShutdown v1   | Moves every partition leader off a broker — it really does stop it    |

`getApiVersions()` is what Kafka 0.10.0 added: it asks one broker for the version range of every
api it serves and returns them indexed by the api key, which is the only way to tell one release
of the protocol from another without guessing. Every broker answers for itself, so a rolling
upgrade shows up as brokers that report different ranges. `Client::apiVersions()` returns the
whole response, with `supports()` and `maxVersionOf()` on it.

The group apis are what Kafka 0.9 added when it moved the consumer groups out of ZooKeeper, and
Kafka 0.10.1 gave them one more state: a group exists on its coordinator from the first JoinGroup
until its committed offsets expire, so `listGroups()` shows it even after its last member has
left, and `describeGroup()` reports its state, the assignor its members agreed on and one entry
per member, with the `Subscription` and `MemberAssignment` of the consumer protocol as opaque
byte arrays. A group with no members left is `Empty`, not `Dead`; asking about a group that does
not exist is still not an error, the coordinator answers the state `Dead` with the error code 0.

CreateTopics and DeleteTopics (keys 19 and 20) arrived with Kafka 0.10.1 and are not implemented
on this branch yet. Until they are, a topic is created by writing to ZooKeeper —
`kafka-topics.sh --create` — or implicitly by asking for the metadata of a topic that does not
exist while the broker runs with `auto.create.topics.enable=true`. That first Metadata answer
carries the topic error code 5 (`LeaderNotAvailable`) and an empty partition list until the
controller has elected the leaders, so a client has to ask again.

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
consumer group live. `kafka` (the default) commits with OffsetCommit v2 and fetches with
OffsetFetch v1, both sent to the coordinator of the group and stored in the `__consumer_offsets`
topic — the v2 commit carries the member id and generation of a group member and the
`offset.retention.ms` of the consumer as its `RetentionTime` (`-1` keeps the retention of the
broker); `zookeeper` uses version 0 of both apis, which stores the offsets in ZooKeeper the way
Kafka 0.8.1 did and which any broker of the cluster answers. A consumer that joins a group
(`subscribe()`) should keep `kafka`: a v0 commit carries no membership, so the coordinator could
not refuse the commit of a member whose generation is over.

Security / SSL
---------------

Kafka 0.9 is the release that added **transport security**: a broker binds one listener per
security protocol (`listeners=PLAINTEXT://…,SSL://…`) and every listener answers the identical
request set, so encryption changes the transport and never a single byte of a request. Point
`bootstrap.servers` at the SSL listener and set `security.protocol`:

```php
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Producer\KafkaProducer;

$producer = new KafkaProducer([
    ClientConfig::BOOTSTRAP_SERVERS    => ['tcp://kafka-1.example.com:9093'],
    ClientConfig::SECURITY_PROTOCOL    => SecurityProtocol::SSL,
    ClientConfig::SSL_CA_CERT_LOCATION => '/etc/kafka/ca.pem',
]);
```

| Option | Default | Meaning |
|---|---|---|
| `security.protocol` | `PLAINTEXT` | `PLAINTEXT` or `SSL`; `SASL_PLAINTEXT`/`SASL_SSL` are rejected, see below |
| `ssl.protocol` | `TLS` | TLS version to offer: `TLS` (any), `TLSv1_1`, `TLSv1_2`, `SSL`, `SSLv2`, `SSLv3` |
| `ssl.enabled.protocols` | – | list of the values above; when set it wins over `ssl.protocol` |
| `ssl.ca.cert.location` | – | PEM file with the certificates the broker certificate is verified against (the `ssl.truststore.location` of the Java client); without it the certificate stores of the system are used |
| `ssl.client.cert.location` | – | PEM file with the client certificate, for a broker running `ssl.client.auth=required` |
| `ssl.key.location` | – | private key of that client certificate |
| `ssl.key.password` | – | passphrase of the private key |

The certificate of the broker is always verified, and its subject has to match the host the
connection was made to — a self-signed broker certificate therefore needs
`ssl.ca.cert.location` pointing at it. The handshake happens right after `connect()` and is
bounded by the connection timeout of the stream, not by `request.timeout.ms`.

**Metadata over SSL.** Version 0 of the Metadata api has room for exactly one host/port per
broker, and a 0.9 broker fills it with the endpoint of the listener the request arrived on. A
client that bootstraps over TLS therefore learns the TLS endpoints of the whole cluster and
keeps talking TLS to every broker it discovers; one that bootstraps in plaintext learns the
plaintext ones. The two never mix, and there is no way to ask one listener about another.

[examples/ssl.php](examples/ssl.php) produces and consumes over the SSL listener of `docker-compose.yml`, whose
self-signed certificate is checked in as `docker/kafka-0.9.0.1/ssl/broker.crt`.

**SASL is out of scope on this branch.** Kafka 0.9 does have SASL, but only GSSAPI (Kerberos)
and it is negotiated *outside* the Kafka protocol: the broker expects the raw token exchange on
a freshly opened connection, with no request to introduce it. The `SaslHandshake` request that
made the mechanism negotiable is api key 17 and arrived with Kafka 0.10.0, so
`security.protocol = SASL_PLAINTEXT` and `SASL_SSL` raise an `InvalidConfigurationException`
that says so.

Supported Kafka protocol versions
----------------------------------

This branch tracks the **Kafka 0.10.2.2** wire protocol — the last release of the 0.10 line, so
it covers what 0.10.0, 0.10.1 and 0.10.2 each added. `main` tracks Kafka 0.11; the frozen
protocol snapshots of the other lines live on `0.9.x` (Kafka 0.9.0.1) and `0.8.x` (Kafka 0.8.2.2).

Kafka 0.10.0 added the **ApiVersions** request (key 18), so this is the first line that can ask a
broker what it speaks instead of guessing. The table below is the literal answer of the container,
read with `Client::apiVersions()` and pinned by `tests/Integration/ApiVersionProbeTest.php`.

The "Implemented" column is the state of this branch while the 0.10 line is being built; a
version marked `planned (T<n>)` has its ticket open and is not merged yet.

| Api key | API                | Versions in 0.10.2.2 | Client-facing | Implemented                     |
|---------|--------------------|----------------------|---------------|---------------------------------|
| 0       | Produce            | v0, v1, v2           | yes           | v0, v1 — v2 planned (T4)        |
| 1       | Fetch              | v0 … v3              | yes           | v0, v1 — v2, v3 planned (T4)    |
| 2       | Offsets            | v0, v1               | yes           | v0 — v1 planned (T5)            |
| 3       | Metadata           | v0, v1, v2           | yes           | v0 — v1, v2 planned (T3)        |
| 4       | LeaderAndIsr       | v0                   | broker→broker | no                              |
| 5       | StopReplica        | v0                   | broker→broker | no                              |
| 6       | UpdateMetadata     | v0 … v3              | broker→broker | no                              |
| 7       | ControlledShutdown | v1 (v0 still parsed) | controller    | v0, v1                          |
| 8       | OffsetCommit       | v0, v1, v2           | yes           | v0, v1, v2                      |
| 9       | OffsetFetch        | v0, v1, v2           | yes           | v0, v1 — v2 planned (T6)        |
| 10      | GroupCoordinator   | v0                   | yes           | yes                             |
| 11      | JoinGroup          | v0, v1               | yes           | v0 — v1 planned (T6)            |
| 12      | Heartbeat          | v0                   | yes           | yes                             |
| 13      | LeaveGroup         | v0                   | yes           | yes                             |
| 14      | SyncGroup          | v0                   | yes           | yes                             |
| 15      | DescribeGroups     | v0                   | yes           | yes                             |
| 16      | ListGroups         | v0                   | yes           | yes                             |
| 17      | SaslHandshake      | v0                   | yes           | planned (T8)                    |
| 18      | ApiVersions        | v0                   | yes           | yes                             |
| 19      | CreateTopics       | v0, v1               | controller    | planned (T7)                    |
| 20      | DeleteTopics       | v0                   | controller    | planned (T7)                    |

What the 0.10 line adds beyond the api versions themselves:

| Feature                                              | Arrived in | On this branch          |
|------------------------------------------------------|------------|-------------------------|
| Message format v1 (timestamps, LZ4, relative offsets) | 0.10.0     | planned (T2)            |
| SASL/PLAIN over `SASL_PLAINTEXT` and `SASL_SSL`       | 0.10.0     | planned (T8)            |
| `controller_id`, broker `rack`, `is_internal`         | 0.10.0     | planned (T3)            |
| `cluster_id` of Metadata v2                           | 0.10.1     | planned (T3)            |
| Offsets by timestamp, `offsetsForTimes()`             | 0.10.1     | planned (T5)            |
| `max.poll.interval.ms` and the `rebalance_timeout`    | 0.10.1     | planned (T6)            |
| The group state `Empty`                               | 0.10.1     | yes                     |
| Error codes 32 … 44                                   | 0.10.0-2   | yes                     |

Everything a later Kafka added is missing here, by design:

| Feature                                          | Arrived in | On this branch                       |
|--------------------------------------------------|------------|--------------------------------------|
| Record batches v2, idempotence, transactions     | 0.11       | no                                   |
| `throttle_time_ms` in the group apis             | 0.11       | no — Produce v1+ and Fetch v1+ only  |
| `DeleteRecords`, ACL and config apis (keys 21+)  | 0.11       | no — `ApiKeys` stops at 20           |
| `isolation_level`, read-committed consumers      | 0.11       | no                                   |
| `SaslAuthenticate` (key 36)                      | 1.0        | no — the token exchange is unframed  |
| Error codes above 44                             | 0.11       | no — 0.10.2.2 defines -1 … 44        |

Three properties of a 0.10.2.2 broker regularly surprise clients, and this implementation deals
with all of them explicitly:

* **An api the broker does not serve costs the connection.** A request whose api key or version
  a 0.10.2.2 broker cannot parse — and a body that does not match the schema of a version it
  does serve — makes it **close the socket**: `Closing socket for … because of error` in the
  broker log, and the end of the stream for the client, reported as a `NetworkException`. A
  0.9.0.1 broker only dropped such a frame and kept the connection open, so code ported from
  the line below waits for a timeout that will never come. There are exactly two exceptions:
  **ApiVersions** answers an unknown version with the error code 35 and survives, and
  **ControlledShutdown** answers every version because it is the last api the broker parses with
  a Scala class. This client only ever sends the api versions of the table above.
* **A group whose last member leaves does not disappear.** Since Kafka 0.10.1 it stays in the
  state `Empty` with its committed offsets until `offsets.retention.minutes` expires them, is
  still listed by `AdminClient::listGroups()` and is described as `Empty`, not `Dead`. That is
  what lets a restarted consumer of the same group resume where the group committed — and it is
  a behaviour change against 0.9, where the coordinator dropped such a group at once.
* **The coordinator of a group is not available right away.** The first GroupCoordinator
  request for any group makes the broker create the internal `__consumer_offsets` topic and is
  answered with the error code 15 while that happens; code 14 means the coordinator is still
  reading the offsets of the group out of it. Both are retried with `retry.backoff.ms` until
  `metadata.fetch.timeout.ms` by `Common\CoordinatorLookup`, which
  `AdminClient::findCoordinator()` and `Client::getGroupCoordinator()` use.

One thing that a 0.10 broker no longer does: **a broker without a single topic answers Metadata
with its brokers**, where 0.8 and 0.9 answered an empty broker array until some topic existed. An
empty broker array is still "not ready, retry" and never "the cluster has no brokers", and
`tests/Fixture/ClusterReadinessProbe.php` still treats it that way, but it is no longer the
normal state of a fresh cluster.

Two changes against the 0.8.2.2 line show up in single apis, and both are worth knowing when
porting code between the branches. **OffsetFetch v1 no longer validates the partition**: asking
for a partition the cluster does not host answers offset `-1` with the error code `0` —
"nothing committed" — where 0.8.2.2 answered 3 (UnknownTopicOrPartition), so Metadata is the
only api that says whether a partition exists. And `controlledShutdown()` for a broker id the
controller does not know is now answered with **8 (BrokerNotAvailable)**, the code a 0.8.2.2
broker turned into -1 (Unknown) by mapping the *cause* of an exception that has none. The
protocol document has the details.

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

docker compose up -d                         # Kafka 0.10.2.2: PLAINTEXT 9092, SSL 9093,
                                             #                 SASL_PLAINTEXT 9094, SASL_SSL 9095
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 vendor/bin/phpunit --testsuite integration
```

The integration suite is skipped unless `KAFKA_BOOTSTRAP_SERVERS` points at a running broker; the
tests of the SSL transport use the listener of `KAFKA_SSL_BOOTSTRAP_SERVERS` (`127.0.0.1:9093` by
default) and the certificate the broker container was built with.
The compliance suite replays every wire vector of
[docs/protocol/vectors](docs/protocol/vectors) — frames that a real Kafka broker sent or
accepted — through the request and response classes and checks that the annotated dumps of
[docs/protocol/0.10.2.md](docs/protocol/0.10.2.md) still hold the same bytes, so the document and
the code cannot drift apart.

Issues and pull requests are welcome.

License
-------

Released under the [MIT license](LICENSE).

[producer configuration]: https://kafka.apache.org/documentation/#producerconfigs
[consumer configuration]: https://kafka.apache.org/documentation/#newconsumerconfigs

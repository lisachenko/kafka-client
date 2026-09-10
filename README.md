PHP Native Apache Kafka Client — 1.x (Kafka 1.1.1)
==================================================

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lisachenko/kafka-client/ci.yml?branch=main)
[![Code Coverage](https://img.shields.io/codecov/c/github/lisachenko/kafka-client/main)](https://app.codecov.io/gh/lisachenko/kafka-client)
[![Minimum PHP Version](http://img.shields.io/badge/php-%3E%3D%208.4-8892BF.svg)](https://www.php.net/supported-versions.php)
[![License](https://img.shields.io/packagist/l/lisachenko/kafka-client.svg)](https://packagist.org/packages/lisachenko/kafka-client)

`lisachenko/kafka-client` is a native, pure-PHP implementation of the Apache Kafka wire
protocol — no `ext-rdkafka` required. It ships a Producer, a Consumer and a low-level Admin
client, designed to stay close in spirit to the official Java client's API while feeling
natural in PHP.

**This branch speaks the Apache Kafka 1.1.1 wire protocol** — the last release of the 1.x line,
so it covers everything Kafka 1.0.0 and 1.1.0 added, and nothing later. `main` is the line in
development: the frozen protocol snapshots below it live on `0.11.x` (Kafka 0.11.0.3), `0.10.x`
(Kafka 0.10.2.2), `0.9.x` (Kafka 0.9.0.1) and `0.8.x` (Kafka 0.8.2.2), and every wire vector those
lines captured is replayed against the classes of this branch, because a 1.1.1 broker still speaks
all of it. What the 1.1 protocol cannot do is simply absent, and
[what that is](#supported-kafka-protocol-versions) is listed below. The grammar this branch
implements is written down, byte for byte, in [docs/protocol/1.1.md](docs/protocol/1.1.md).

Installation
------------

```bash
composer require lisachenko/kafka-client:dev-main
```

`main` is the line in development, so it is installed by branch name; the frozen lines below it
carry a numeric branch and are installed by constraint (`^0.11@dev` for `0.11.x`, `^0.10@dev` for
`0.10.x`, and so on).

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
        echo "The log holds the timestamp {$metadata->timestamp}\n";
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
whole batch: `gzip`, `snappy` and — new in Kafka 0.10.0 — `lz4`, in the frame format of the
Kafka producer including the KAFKA-3160 checksum quirk of a message format v0 frame.

**Message formats, timestamps and headers.** Kafka 0.10.0 gave every record a timestamp:
`Record::$timestamp` (milliseconds since the epoch) and `Record::$timestampType`
(`TimestampType::CREATE_TIME`, `LOG_APPEND_TIME` or `NO_TIMESTAMP_TYPE`), and Kafka 0.11 gave it
**headers** (`Record::withHeaders()`, `Common\Record\Header`), a list of key-value pairs of
metadata next to the key and the value. `send()` stamps the create time of every record that does
not carry one, and `ProducerConfig::MESSAGE_FORMAT_VERSION` (`message.format.version`, `0.11.0` by
default) selects the format a batch is written in — the record batch v2 by default, `0.10.x` for a
message set with timestamps and `0.9.0` for one without. The format decides the version of the
Produce request: only the message format v2 travels in a **Produce v5**, and only it has a place
for the headers, for the producer id of an idempotent producer and for a transaction; a message set
is sent as a Produce v2, and a 1.1.1 broker closes the connection on a Produce v3 or above that
carries one. `RecordMetadata::$timestamp` reports what the **log** holds: the create time of the
first record of the batch, or the `LogAppendTime` the broker answered with (Produce v2 and above)
when the topic is configured with `message.timestamp.type=LogAppendTime`. Version 5 (Kafka 1.0)
also reports the `logStartOffset` of every partition it answers — the first offset the log still
holds after a retention run or a `deleteRecords()` — on `ProduceResponsePartition`.

Without a key a record is spread over the partitions that have a leader, with a key it goes to
the partition that the murmur2 hash of the key selects, exactly as with the official Java
client (`Producer\DefaultPartitioner`); an explicit partition can be passed to `send()`.

`RecordMetadata::$throttleTimeMs` is the `ThrottleTime` that version 1 of the Produce API added
in Kafka 0.9: the number of milliseconds the broker delayed the answer of that batch because the
`client.id` exceeded its `producer_byte_rate` quota. Quotas never reject a write — the records
are appended and only the response is held back — so the field is informational, and it is `0`
on a broker without quotas as well as for a fire-and-forget batch (`ACKS => 0`), which is never
answered. The consumer side is the same: every `Common\FetchedPartition` of
`Client::fetchPartitions()` carries the `throttleTimeMs` of the Fetch answer it came in.
Quotas are set per client id on a running broker, e.g.

```console
$ kafka-configs.sh --zookeeper localhost:2181 --alter \
    --add-config 'producer_byte_rate=1024,consumer_byte_rate=2048' \
    --entity-type clients --entity-name my-application
```

A runnable version of this is [examples/producer.php](examples/producer.php).

### Idempotent producer

Kafka 0.11 added a delivery guarantee that no release before it had, and one option turns it on:

```php
$producer = new KafkaProducer([
    ProducerConfig::BOOTSTRAP_SERVERS  => ['tcp://127.0.0.1:9092'],
    ProducerConfig::ENABLE_IDEMPOTENCE => true,
]);
```

With `enable.idempotence` the producer asks a broker for a **producer id** before its first batch
(`InitProducerId`, key 22) and numbers the batch of every topic-partition with a gapless sequence
number. A batch that has to be sent again — a lost acknowledgement, a leader that moved — goes out
with the very same producer id, epoch and sequence numbers, and the broker recognises it as the
batch it already holds: it answers the offset of the **original** append and writes nothing. The
records of a partition therefore reach the log exactly once and in order, however often the client
had to retry, and nothing about the API changes: `send()` and `flush()` work as before.

The guarantee implies `acks = all` and a non-zero `retries`; both are set for you when you did not
set them (`retries` becomes 3, where the Java producer, which has a background sender, uses an
unbounded budget), and a configuration that contradicts them — `acks` of 0 or 1, or `retries` of 0
— is refused with an `InvalidConfigurationException`. The Java requirement of
`max.in.flight.requests.per.connection = 1` needs no option here: this client sends one produce
request at a time.

It holds **within one producer session**: a new `KafkaProducer` gets a new producer id and cannot
deduplicate against what the previous one wrote, and a record your application sends a second time
is a new batch, which the broker has no way of recognising. Deduplication across sessions is what a
`transactional.id` is for.

Two error codes of the broker say that the producer state itself is broken. `47`
(`ProducerFencedException`) means another producer took the producer id over: the producer is
finished and refuses every further send. `45` (`OutOfOrderSequenceException`) means the producer
and the broker no longer agree on what is in the log: the batch that hit it is reported to the
caller, and the producer starts over with a new producer id — everything written under the old one
loses its deduplication. Both are documented, with what a real 1.1.1 broker answers, in
[docs/protocol/1.1.md](docs/protocol/1.1.md), section "The idempotent producer".

### Transactions

A `transactional.id` turns the idempotent producer into a **transactional** one: the records of
several partitions — and the committed offsets of a consumer group — become one unit that a
`read_committed` consumer either sees whole or does not see at all, and the guarantee survives a
restart of the producer, because the id is what the broker remembers it by.

```php
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Record\Record;
use Protocol\Kafka\Producer\KafkaProducer;
use Protocol\Kafka\Producer\ProducerConfig;

$producer = new KafkaProducer([
    ProducerConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092'],
    ProducerConfig::TRANSACTIONAL_ID  => 'orders-etl-1',   // implies enable.idempotence
]);

$producer->initTransactions();          // once, before the first send

$producer->beginTransaction();
try {
    $producer->send('orders', Record::fromValue('one'));
    $producer->send('audit',  Record::fromValue('one accepted'));
    $producer->commitTransaction();     // flushes what is buffered, then EndTxn
} catch (KafkaException $error) {
    $producer->abortTransaction();      // the only way out of a failed transaction
}
```

`initTransactions()` asks the **transaction coordinator** of the id for a producer id and an epoch
one higher than the previous incarnation used, which fences that incarnation for good and rolls
back whatever transaction it left open — so a transactional id must be used by one producer at a
time, and a crashed producer never blocks a reader for longer than its `transaction.timeout.ms`.
A `send()` outside a transaction is refused, and so is one after an error that only an abort can
clean up; `commitTransaction()` flushes the buffer before it ends the transaction and
`abortTransaction()` throws it away.

The read side is one consumer option:

```php
$consumer = new KafkaConsumer([
    ConsumerConfig::BOOTSTRAP_SERVERS  => ['tcp://127.0.0.1:9092'],
    ConsumerConfig::GROUP_ID           => 'orders-readers',
    ConsumerConfig::ISOLATION_LEVEL    => ConsumerConfig::ISOLATION_LEVEL_READ_COMMITTED,
]);
```

With `read_committed` the broker answers only up to the **last stable offset** of a partition, so
nothing of a transaction that is still open is shown, `endOffsets()` reports the offset such a
reader can really reach, and the records of transactions the broker names as **aborted** are
dropped by the consumer before `poll()` returns — the broker sends them and only names them.
The COMMIT and ABORT control batches of a transaction never reach an application in either level.

The **consume-transform-produce** loop is what all of this exists for: the consumer hands its
offsets to the producer instead of committing them itself, so reading the input and writing the
output either both happen or neither does.

```php
$producer->beginTransaction();
foreach ($consumer->poll(1000)['input'][0] ?? [] as $record) {
    $producer->send('output', Record::fromValue(strtoupper((string) $record->value)));
}
$producer->flush();
$producer->sendOffsetsToTransaction(['input' => [0 => $consumer->position('input', 0)]], 'my-group');
$producer->commitTransaction();
```

The consumer of that loop runs with `enable.auto.commit = false` and `read_committed`. A runnable
version is [examples/transactional-producer.php](examples/transactional-producer.php); the wire
protocol behind it — the five apis 24 to 28, the control batches and the last stable offset — is in
[docs/protocol/1.1.md](docs/protocol/1.1.md), section "Transactions".

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
    // A JoinGroup is answered only once the whole rebalance is over, so this has to exceed both
    // session.timeout.ms and max.poll.interval.ms; the consumer defaults are those of the Java
    // consumer of 0.10.1: 305000, 10000 and 300000
    ClientConfig::REQUEST_TIMEOUT_MS => 305000,

    ConsumerConfig::GROUP_ID                      => 'kafka-daemon',
    ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY => 'range', // or 'roundrobin', or your own class
    ConsumerConfig::SESSION_TIMEOUT_MS            => 10000,
    ConsumerConfig::MAX_POLL_INTERVAL_MS          => 300000,
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
                // $record->timestamp and $record->timestampType come from message format v1
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
`group.max.session.timeout.ms` of the broker.

`max.poll.interval.ms` (Kafka 0.10.1, KIP-62) is the second half of that: it is sent as the
`rebalance_timeout` of the JoinGroup v1 request and tells the coordinator how long it should wait
for **this** member to rejoin a rebalance — the group waits that long instead of the session
timeout, which is what lets a member process a batch for minutes without holding up nothing but
its own rejoin. In a PHP consumer it does **not** evict anything the way it does in a Java one:
without a heartbeat thread, an application that stops polling stops heartbeating and is dropped
when its *session* timeout expires. `request.timeout.ms` has to exceed both timeouts, because a
JoinGroup blocks the connection for the whole rebalance, and `subscribe()` refuses a value that
does not.

**Record headers and the isolation level** are what Kafka 0.11 adds on this side. A record read out
of a record batch v2 carries the headers the producer wrote (`ConsumerRecord::$headers`, a list of
`Common\Record\Header`), next to the key, the value, the timestamp and its type; a topic whose
`message.format.version` is older simply has none. `ConsumerConfig::ISOLATION_LEVEL`
(`isolation.level`, `read_uncommitted` by default) is sent as the isolation level of the Fetch v7
request: with `read_committed` the broker answers only up to the **last stable offset** — the first
record of a transaction that has neither committed nor aborted — and names the aborted transactions
of the answer, whose records the consumer drops. The control batches of the transaction protocol are
never handed to an application in either level. The option travels in the **Offsets v2** request as
well, so `endOffsets()`, `position()` and `seekToEnd()` of a `read_committed` consumer answer the last
stable offset instead of the log end offset — a consumer that compares its position against the end of
a partition compares it against the offset it can really reach. `AdminClient::listOffsets()` stays at
`read_uncommitted` on purpose: an administrator asks what is in the log.

**Offsets by timestamp** (Kafka 0.10.1, KIP-79) are what the record timestamps buy on the consumer
side: `offsetsForTimes(['test' => [0 => $millis]])` answers the first record of each partition
whose timestamp is at or after the given one, as an `OffsetAndTimestamp` (or `null` when the
partition holds no such record), and `beginningOffsets()` / `endOffsets()` are the two special
timestamps `-2` and `-1`. All three are pure queries and need no assignment, exactly as in the
Java consumer.

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

The Admin API exposes the low-level cluster operations a 1.1.1 broker can serve:

```php
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Admin\RecordsToDelete;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Protocol\Request\OffsetsRequest;

$configuration = [ClientConfig::BOOTSTRAP_SERVERS => ['tcp://127.0.0.1:9092']];
$admin         = new AdminClient(Cluster::bootstrap($configuration), $configuration);

$brokers  = $admin->findAllBrokers();                       // Node[], indexed by the node id
$apis     = $admin->getApiVersions($brokers[0]);            // api key => ApiVersionsResponseMetadata
$topics   = $admin->listTopics();                           // string[]
$metadata = $admin->describeTopics(['test']);               // TopicMetadata[], indexed by the topic
$offsets  = $admin->listOffsets(['test' => [0, 1, 2]]);     // topic => partition => offset
$earliest = $admin->listOffsets(['test' => [0]], OffsetsRequest::EARLIEST);

$controller = $admin->findController();                     // Node, from the controller_id of Metadata v1
$created    = $admin->createTopics([new NewTopic('test-2', 3, 1)]);   // topic => ?KafkaException
$deleted    = $admin->deleteTopics(['test-2']);                      // topic => ?KafkaException

$purged = $admin->deleteRecords(['test' => [0 => 100]]);    // topic => partition => DeletedRecords (low watermark)
$purged = $admin->deleteRecords(['test' => [0 => RecordsToDelete::allRecords()]]);

$topicResource = ConfigResource::topic('test');
$configs       = $admin->describeConfigs([$topicResource]); // resource key => Config
echo $configs[$topicResource->key()]->value('retention.ms');
$altered = $admin->alterConfigs([                           // resource key => ?KafkaException
    $topicResource->key() => ['retention.ms' => '3600000'] + $configs[$topicResource->key()]->nonDefaultValues(),
]);

$coordinator = $admin->findCoordinator('kafka-daemon');     // Node that holds the group offsets
$committed   = $admin->listGroupOffsets('kafka-daemon');    // every topic the group committed (v2)

$groups = $admin->listAllGroups();                          // group id => ListGroupResponseProtocol
$groups = $admin->listGroups($coordinator);                 // only the groups of that one broker

$group = $admin->describeGroup('kafka-daemon');             // DescribeGroupResponseMetadata
echo $group->state;                                         // Stable, CompletingRebalance, PreparingRebalance, Empty or Dead
echo $group->protocol;                                      // the assignor, only while the group is stable
foreach ($group->members as $memberId => $member) {
    echo $memberId, ' ', $member->clientId, ' ', $member->clientHost, PHP_EOL;
    // $member->memberMetadata and $member->memberAssignment are the opaque bytes of the protocol type
}
```

| Method                                       | Wire API                | Notes                                                                |
|----------------------------------------------|-------------------------|----------------------------------------------------------------------|
| `getApiVersions()`                           | ApiVersions v1          | The version range of every api of **one** broker, indexed by api key; version 1 carries the throttle time |
| `findAllBrokers()`                           | Metadata v5             | An empty result means "the cluster is not ready yet", see below      |
| `listTopics()` / `describeTopics()`          | Metadata v5             | Asks with `allow_auto_topic_creation = false`, so an unknown topic is answered 3 and **not** created; `describeTopics([])` asks for every topic (the `null` array of v1); every partition reports its `offlineReplicas` (v5, KIP-112/113) |
| `findController()`                           | Metadata v5             | The `controller_id` of the answer; the two topic apis below need it   |
| `createTopics()`                             | CreateTopics v2         | `NewTopic` with partitions/factor or an explicit assignment, plus topic configs; `validateOnly` checks without creating |
| `deleteTopics()`                             | DeleteTopics v1         | Needs `delete.topic.enable=true` on the broker                        |
| `listOffsets()`                              | Offsets v2              | Earliest, latest or by message timestamp; **one** offset per partition, sent to the partition leader, with the isolation level `read_uncommitted` |
| `findCoordinator()`                          | GroupCoordinator v1     | Retries the codes 15 and 14 while the coordinator warms up; version 1 also looks a **transactional id** up (`coordinator_type = 1`) |
| `listGroupOffsets()`                         | OffsetFetch v3          | Without a partition list it asks for **every** topic the group committed (`null` topics of v2) |
| `listGroups()` / `listAllGroups()`           | ListGroups v1           | A broker only knows its own groups; `listAllGroups()` merges them all  |
| `describeGroup()` / `describeGroups()`       | DescribeGroups v1       | Sent to the coordinator of the group; an unknown group answers `Dead`, one whose last member left `Empty` |
| `controlledShutdown()`                       | ControlledShutdown v1   | Moves every partition leader off a broker — it really does stop it    |
| `deleteRecords()`                            | DeleteRecords v0        | Moves the **low watermark** of a partition forward (KIP-107); sent to the partition leader, answers a `DeletedRecords` per partition |
| `describeConfigs()`                          | DescribeConfigs v0      | The configuration of a topic or a broker (KIP-133); a broker resource is only answered by that broker, and a sensitive value comes back `null` |
| `alterConfigs()`                             | AlterConfigs v0         | **Replaces** the whole configuration of a topic; a 1.1 broker takes a broker resource too and refuses the options it cannot change at runtime with 42 (KIP-226, ticket T4) |

Both topic apis are served by the **controller** alone: `AdminClient` looks it up in the
`controller_id` of a Metadata answer, and repeats the request once against a freshly looked up
controller when a topic comes back with the error code 41 (`NotController`). Neither method throws
for a topic: the result has one entry per requested topic, in the order of the request, `null` when
it worked and the exception of its error code — with the `error_message` of CreateTopics v1 in the
context — when it did not, because one topic of a batch says nothing about the others.

`getApiVersions()` is what Kafka 0.10.0 added: it asks one broker for the version range of every
api it serves and returns them indexed by the api key, which is the only way to tell one release
of the protocol from another without guessing. Kafka 0.11 raised it to **version 1**, whose answer
carries a trailing `throttle_time_ms` — the one api of KIP-124 that appends the field instead of
prepending it, because an unknown version is still answered in the version 0 layout. Every broker answers for itself, so a rolling
upgrade shows up as brokers that report different ranges. `Client::apiVersions()` returns the
whole response, with `supports()` and `maxVersionOf()` on it.

The group apis are what Kafka 0.9 added when it moved the consumer groups out of ZooKeeper, and
Kafka 0.10.1 gave them one more state: a group exists on its coordinator from the first JoinGroup
until its committed offsets expire, so `listGroups()` shows it even after its last member has
left, and `describeGroup()` reports its state, the assignor its members agreed on and one entry
per member, with the `Subscription` and `MemberAssignment` of the consumer protocol as opaque
byte arrays. A group with no members left is `Empty`, not `Dead`; asking about a group that does
not exist is still not an error, the coordinator answers the state `Dead` with the error code 0.

**Creating a topic** no longer means writing to ZooKeeper: CreateTopics (key 19) and DeleteTopics
(key 20) arrived with Kafka 0.10.1, and `AdminClient::createTopics()` sends version 2 of the first
one, with `validate_only` and the per-topic `error_message`. The implicit creation by a Metadata
request of an unknown topic still works when the broker runs with `auto.create.topics.enable=true`,
and still answers the topic error code 5 (`LeaderNotAvailable`) with an empty partition list until
the controller has elected the leaders — but the admin client no longer triggers it: Metadata v4
(Kafka 0.11, KIP-4) added `allow_auto_topic_creation`, and every request of `AdminClient` sends it
as `false`, so describing a topic that does not exist is answered with the code 3 and creates
nothing. `createTopics()` is the explicit alternative that reports what went wrong. Metadata **v5**
(Kafka 1.0, KIP-112/113) is what this client sends today, so every partition it describes also
carries its `offlineReplicas` — the replicas whose broker is down or whose log directory failed.

Metadata v1 and v2 also gave the cluster an identity of its own: `Cluster::clusterId()` is the
`cluster_id` the broker generated (the `/cluster/id` znode), `Cluster::controller()` the node the
`controller_id` names, `Common\Node::$rack` the `broker.rack` of a broker, and
`Cluster::topics()` hides `__consumer_offsets` unless `exclude.internal.topics` is turned off.

**Records and configuration through the protocol** are what Kafka 0.11 added to the admin surface.
`deleteRecords()` (KIP-107) moves the **low watermark** of a partition forward — everything below the
offset becomes unreadable at once, the record at the offset stays — and answers the new watermark of
every partition as an `Admin\DeletedRecords`; the offset is a plain integer or an
`Admin\RecordsToDelete` (`beforeOffset()`, or `allRecords()` for the `-1` of the wire, i.e. up to the
high watermark). It is served by the **leader** of each partition, so the request is split per leader
and a partial failure is reported as a `TopicPartitionRequestException`. `describeConfigs()` and
`alterConfigs()` (KIP-133) read and write the configuration of a topic or of a broker without going
through ZooKeeper: a resource is an `Admin\ConfigResource` (`topic()` / `broker()`) and is addressed
in the result by its `key()`, because PHP cannot use an object as an array key. `alterConfigs()`
**replaces** the whole configuration of a topic — an option that is left out is reset to its default,
which is what `Config::nonDefaultValues()` exists for. A **broker** resource is where this line differs
from the one below it: KIP-226 made a 1.1 broker accept one and validate it **per option**, so an
option it cannot change at runtime comes back as the error code 42 with
`Cannot update these configs dynamically: Set(log.retention.hours)` while a dynamic one is applied,
where a 0.11 broker refused every broker resource outright. Reading such a resource changed too — the
`is_default` of an entry is derived from the KIP-226 config *source* and `is_read_only` means "not
dynamically updatable" — and the version 1 of the api that reports the source and its synonyms is
implemented by the ticket T4 of this line.

[examples/admin.php](examples/admin.php), [examples/create-topic.php](examples/create-topic.php) and
[examples/admin-configs.php](examples/admin-configs.php) run all of it against the broker of
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
consumer group live. `kafka` (the default) commits with OffsetCommit **v3** and fetches with
OffsetFetch **v3**, both sent to the coordinator of the group and stored in the
`__consumer_offsets` topic — the commit carries the member id and generation of a group member
and the `offset.retention.ms` of the consumer as its `RetentionTime` (`-1` keeps the retention of
the broker), the fetch is the one that can ask for *every* topic the group committed, and the two
versions Kafka 0.11 added carry nothing but the `throttle_time_ms` of KIP-124;
`zookeeper` uses version 0 of both apis, which stores the offsets in ZooKeeper the way Kafka 0.8.1
did and which any broker of the cluster answers. A consumer that joins a group (`subscribe()`)
should keep `kafka`: a v0 commit carries no membership, so the coordinator could not refuse the
commit of a member whose generation is over.

Configuration reference
------------------------

Every option is a plain array key of the configuration passed to `KafkaProducer`, `KafkaConsumer`
or `AdminClient`; the constants are documented one by one on `Common\ClientConfig`,
`Consumer\ConsumerConfig` and `Producer\ProducerConfig`. The options the 0.10 line adds are
marked **(0.10)**.

**Client** (`Common\ClientConfig`, shared by all three)

| Option | Default | Meaning |
|---|---|---|
| `bootstrap.servers` | – | list of `tcp://host:port` entries, the only required option |
| `client.id` | `PHP/Kafka` | name of the application; the broker uses it for quotas and it is the prefix of a group member id |
| `security.protocol` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, **(0.10)** `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl.mechanism` **(0.10)** | `PLAIN` | the only implemented mechanism; `GSSAPI` and the two SCRAM ones are refused with the reason |
| `sasl.username` / `sasl.password` **(0.10)** | – | credentials of the PLAIN token, required for a SASL transport |
| `ssl.protocol`, `ssl.enabled.protocols`, `ssl.ca.cert.location`, `ssl.client.cert.location`, `ssl.key.location`, `ssl.key.password` | see "Security" below | TLS transport |
| `request.timeout.ms` | 30000 (consumer: 305000) | read timeout of a single request |
| `metadata.fetch.timeout.ms` | 60000 | how long `Cluster::bootstrap()` and the coordinator lookup keep retrying |
| `metadata.max.age.ms` | 300000 | how long cluster metadata stays valid |
| `connections.max.idle.ms` | 540000 | a cached connection older than this is re-opened |
| `retries` / `retry.backoff.ms` | 2 / 100 | retry budget for the codes 3, 5, 6 and a dropped connection |
| `reconnect.backoff.ms` | 50 | pause before a reconnect |
| `receive.buffer.bytes` / `send.buffer.bytes` | 32768 / 131072 | socket buffers |
| `offsets.storage` | `kafka` | `kafka` (OffsetCommit v2 / OffsetFetch v2) or `zookeeper` (v0 of both) |
| `metadata.cache.file`, `stream.async.connect`, `stream.persistent.connection` | – / false / false | the PHP-specific options above |

**Consumer** (`Consumer\ConsumerConfig`)

| Option | Default | Meaning |
|---|---|---|
| `group.id` | `''` | group to join with `subscribe()`, and the group a commit belongs to |
| `partition.assignment.strategy` | `range` | `range`, `roundrobin` or a `PartitionAssignorInterface` class |
| `session.timeout.ms` | **10000** | how long the coordinator waits for a heartbeat; the Java 0.10.1 default |
| `max.poll.interval.ms` **(0.10)** | 300000 | the `rebalance_timeout` of JoinGroup v1: how long the group waits for this member to rejoin a rebalance |
| `heartbeat.interval.ms` | 3000 | how often `poll()` sends a heartbeat |
| `request.timeout.ms` | **305000** | has to exceed both timeouts above, because a JoinGroup blocks |
| `fetch.min.bytes` / `fetch.max.wait.ms` | 1 / 500 | when the broker answers a fetch |
| `fetch.max.bytes` **(0.10)** | 52428800 | request-level `max_bytes` of Fetch v3, the bound of a whole answer |
| `isolation.level` **(0.11)** | `read_uncommitted` | `read_uncommitted` or `read_committed`: what a Fetch v4 and above and an Offsets v2 make of transactional records |
| `max.partition.fetch.bytes` | 65536 | per-partition bound; from Fetch v3 on the first partition is served whole even if it exceeds both |
| `auto.offset.reset` | `latest` | `latest` or `earliest`, used when a partition has no committed offset |
| `enable.auto.commit` / `auto.commit.interval.ms` | true / 0 | commit from `poll()`; 0 means "after every poll" |
| `offset.retention.ms` | -1 | `RetentionTime` of an OffsetCommit v2; -1 keeps the retention of the broker |
| `exclude.internal.topics` | true | hides `__consumer_offsets` from `Cluster::topics()` |
| `check.crcs` | true | verify the CRC of every message |
| `key.deserializer` / `value.deserializer` | – | class names; a poll then returns `ConsumerRecord`s |

**Producer** (`Producer\ProducerConfig`)

| Option | Default | Meaning |
|---|---|---|
| `acks` | 1 | `0` fire-and-forget, `1` the leader's log, `-1` all in-sync replicas |
| `timeout.ms` | 2000 | how long the broker waits for the replicas of a batch |
| `batch.size` / `linger.ms` | 0 / 0 | when a batch is sent |
| `compression.type` | `none` | `none`, `gzip`, `snappy`, **(0.10)** `lz4` |
| `message.format.version` **(0.10)** | `0.11.0` | format a batch is written in, and with it the Produce version: `0.9.0` and below format v0, `0.10.x` format v1 with timestamps (both a Produce v2), `0.11.0` the record batch v2 with headers (a Produce v5) |
| `max.request.size` | 1048576 | biggest record this client will buffer |
| `retries` / `retry.backoff.ms` | 0 / 100 | retry budget of a batch; **3** when `enable.idempotence` is on and it was not set |
| `enable.idempotence` **(0.11)** | false | exactly once and in order per partition; implies `acks = all` and a non-zero `retries` |
| `transactional.id` **(0.11)** | – | turns the producer into a transactional one and implies `enable.idempotence` |
| `transaction.timeout.ms` **(0.11)** | 60000 | how long the coordinator lets a transaction of this producer stay open |
| `partitioner.class` | `DefaultPartitioner` | murmur2 of the key, round robin without one |

Security / SSL
---------------

Kafka 0.9 is the release that added **transport security** and Kafka 0.10.0 the release that made
authentication part of the protocol: a broker binds one listener per security protocol
(`listeners=PLAINTEXT://…,SSL://…,SASL_PLAINTEXT://…,SASL_SSL://…`) and every listener answers the
identical request set, so the transport changes and never a single byte of a request. Point
`bootstrap.servers` at the listener and set `security.protocol`:

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
| `security.protocol` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL` — all four work on this branch |
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

**Metadata over a listener.** Every version of the Metadata api has room for exactly one host/port
per broker, and the broker fills it with the endpoint of the listener the request arrived on. A
client that bootstraps over TLS therefore learns the TLS endpoints of the whole cluster and keeps
talking TLS to every broker it discovers; one that bootstraps in plaintext learns the plaintext
ones, one that authenticates learns the SASL ones. They never mix, and there is no way to ask one
listener about another.

[examples/ssl.php](examples/ssl.php) produces and consumes over the SSL listener of `docker-compose.yml`, whose
self-signed certificate is checked in as `docker/kafka-1.1.1/ssl/broker.crt`.

**SASL/PLAIN works on this branch.** Kafka 0.9 did have SASL, but only GSSAPI (Kerberos) and
negotiated *outside* the Kafka protocol; Kafka 0.10.0 (KIP-43) added the `SaslHandshake` request
(api key 17) and the PLAIN mechanism, which is what makes authentication implementable in pure
PHP, and Kafka 1.0 (KIP-152) added the `SaslAuthenticate` request (api key 36), which is what makes
a refused password reportable:

```php
$producer = new KafkaProducer([
    ClientConfig::BOOTSTRAP_SERVERS => ['tcp://kafka-1.example.com:9094'],
    ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SASL_PLAINTEXT, // or SASL_SSL
    ClientConfig::SASL_MECHANISM    => SaslMechanism::PLAIN,
    ClientConfig::SASL_USERNAME     => 'kafkatest',
    ClientConfig::SASL_PASSWORD     => 'kafkatest-secret',
]);
```

The handshake and the token exchange happen inside `connect()`, before the first ordinary request:
one `SaslHandshake` frame naming the mechanism, then the PLAIN token `\0<username>\0<password>`,
answered with an empty token. **Kafka 1.0 (KIP-152) gave that token a request of its own** —
`SaslAuthenticate`, api key 36 — and version 1 of the handshake is how a client asks for it; this
client sends **v1**, so the token travels as an ordinary framed request and a refused credential
comes back as the error code **58** with the message of the broker
(`Authentication failed: Invalid username or password`) instead of a silently closed socket. The
raw, unframed exchange of a v0 handshake is still implemented and still served by a 1.1.1 broker —
it is what the four lines below speak. PLAIN sends the password in clear text, so use `SASL_SSL`
outside a trusted network: the very same exchange, inside the TLS channel. Either way a refusal is
a `SaslAuthenticationException` — carrying the code and the message when there is one — which
leaves every retry loop of the client, because nothing about the connection would be different next
time. `GSSAPI` and the SCRAM mechanisms of 0.10.2 are refused with an explanation before a socket is
opened. See [examples/sasl.php](examples/sasl.php), the "SASL/PLAIN" section of the protocol
document and its "SaslAuthenticate API (key 36, v0)" section.

Supported Kafka protocol versions
----------------------------------

This branch tracks the **Kafka 1.1.1** wire protocol — the last release of the 1.x line, so it
covers what Kafka 1.0.0 and 1.1.0 added; the bug-fix releases after them changed nothing on the
wire. The frozen protocol snapshots of the lines below live on `0.11.x` (Kafka 0.11.0.3), `0.10.x`
(Kafka 0.10.2.2), `0.9.x` (Kafka 0.9.0.1) and `0.8.x` (Kafka 0.8.2.2).

Kafka 0.10.0 added the **ApiVersions** request (key 18), so this line does not have to guess what
its broker speaks. The table below is the literal answer of the container, read with
`Client::apiVersions()` and pinned by `tests/Integration/ApiVersionProbeTest.php`.

The "this branch" column lists the versions this client has a class for; the one in **bold** is the
version it sends. A cell that names a ticket (`T<n>`) is a version of Kafka 1.x that the line is
still implementing.

| Api key | API                  | Versions in 1.1.1 | Client-facing | `0.10.x` | `0.11.x` | `main` (this branch)         |
|---------|----------------------|-------------------|---------------|----------|----------|------------------------------|
| 0       | Produce              | v0 … v5           | yes           | v0, v1, v2 | v0 … v2, **v3** | v0 … v4, **v5** (**v2** for `message.format.version` below 0.11.0) |
| 1       | Fetch                | v0 … v7           | yes           | v0 … v3  | v0 … v4, **v5** | v0 … v6, **v7** (session-less; the sessions in the consumer: T8) |
| 2       | Offsets              | v0 … v2           | yes           | v0, v1   | v0, v1, **v2** | v0, v1, **v2**              |
| 3       | Metadata             | v0 … v5           | yes           | v0, v1, v2 | v0 … v3, **v4** | v0 … v4, **v5**             |
| 4       | LeaderAndIsr         | v0, v1            | broker→broker | no       | no       | no                           |
| 5       | StopReplica          | v0                | broker→broker | no       | no       | no                           |
| 6       | UpdateMetadata       | v0 … v4           | broker→broker | no       | no       | no                           |
| 7       | ControlledShutdown   | v0, v1            | controller    | v0, v1   | v0, **v1** | v0, **v1**                 |
| 8       | OffsetCommit         | v0 … v3           | yes           | v0, v1, v2 | v0 … v2, **v3** | v0, v1, v2, **v3** (**v0** for `offsets.storage = zookeeper`) |
| 9       | OffsetFetch          | v0 … v3           | yes           | v0, v1, v2 | v0 … v2, **v3** | v0, v1, v2, **v3** (**v0** for `offsets.storage = zookeeper`) |
| 10      | GroupCoordinator     | v0, v1            | yes           | **v0**   | v0, **v1** | v0, **v1**                 |
| 11      | JoinGroup            | v0 … v2           | yes           | v0, v1   | v0, v1, **v2** | v0, v1, **v2**             |
| 12      | Heartbeat            | v0, v1            | yes           | **v0**   | v0, **v1** | v0, **v1**                 |
| 13      | LeaveGroup           | v0, v1            | yes           | **v0**   | v0, **v1** | v0, **v1**                 |
| 14      | SyncGroup            | v0, v1            | yes           | **v0**   | v0, **v1** | v0, **v1**                 |
| 15      | DescribeGroups       | v0, v1            | yes           | **v0**   | v0, **v1** | v0, **v1**                 |
| 16      | ListGroups           | v0, v1            | yes           | **v0**   | v0, **v1** | v0, **v1**                 |
| 17      | SaslHandshake        | v0, v1            | yes           | **v0**   | **v0**   | v0, **v1**                   |
| 18      | ApiVersions          | v0, v1            | yes           | **v0**   | v0, **v1** | v0, **v1**                 |
| 19      | CreateTopics         | v0 … v2           | controller    | v0, v1   | v0, v1, **v2** | v0, v1, **v2**             |
| 20      | DeleteTopics         | v0, v1            | controller    | **v0**   | v0, **v1** | v0, **v1**                 |
| 21      | DeleteRecords        | v0                | yes           | –        | **v0**   | **v0**                       |
| 22      | InitProducerId       | v0                | yes           | –        | **v0**   | **v0**                       |
| 23      | OffsetForLeaderEpoch | v0                | broker→broker | –        | **v0**   | **v0** (classes and vectors, no client method) |
| 24      | AddPartitionsToTxn   | v0                | yes           | –        | **v0**   | **v0**                       |
| 25      | AddOffsetsToTxn      | v0                | yes           | –        | **v0**   | **v0**                       |
| 26      | EndTxn               | v0                | yes           | –        | **v0**   | **v0**                       |
| 27      | WriteTxnMarkers      | v0                | broker→broker | –        | **v0**   | **v0** (classes and vectors, no client method) |
| 28      | TxnOffsetCommit      | v0                | yes           | –        | **v0**   | **v0**                       |
| 29      | DescribeAcls         | v0                | yes           | –        | no       | no, see below                |
| 30      | CreateAcls           | v0                | yes           | –        | no       | no, see below                |
| 31      | DeleteAcls           | v0                | yes           | –        | no       | no, see below                |
| 32      | DescribeConfigs      | v0, v1            | yes           | –        | **v0**   | **v0**; v1: T4               |
| 33      | AlterConfigs         | v0                | yes           | –        | **v0**   | **v0**                       |
| 34      | AlterReplicaLogDirs  | v0                | yes           | –        | –        | T5                           |
| 35      | DescribeLogDirs      | v0                | yes           | –        | –        | T5                           |
| 36      | SaslAuthenticate     | v0                | yes           | –        | –        | **v0**                       |
| 37      | CreatePartitions     | v0                | controller    | –        | –        | T4                           |
| 38      | CreateDelegationToken | v0               | yes           | –        | –        | **v0**                       |
| 39      | RenewDelegationToken | v0                | yes           | –        | –        | **v0**                       |
| 40      | ExpireDelegationToken | v0               | yes           | –        | –        | **v0**                       |
| 41      | DescribeDelegationToken | v0             | yes           | –        | –        | **v0**                       |
| 42      | DeleteGroups         | v0                | yes           | –        | –        | T4                           |

`offsets.storage = zookeeper` sends version 0 of OffsetCommit and OffsetFetch instead of the bold
ones, and the lower versions of every api are kept because their frames are what the wire vectors
of the lines below replay.

**The three ACL apis (29, 30, 31) are deliberately not implemented.** They do nothing on a broker
without an `authorizer.class.name` — a 1.1.1 broker answers all three with the error code 54,
`SecurityDisabled` — and every wire vector of this repository is captured from a real broker, so
they wait for a container that has an authorizer configured.

**The four delegation-token apis (38 to 41) are implemented, and a token cannot be used to
authenticate.** `AdminClient::createDelegationToken()`, `renewDelegationToken()`,
`expireDelegationToken()` and `describeDelegationToken()` speak them over an authenticated channel —
one of the SASL listeners, because KIP-48 derives the owner of a token from the principal of the
connection and answers the error code 64 on a PLAINTEXT or one-way-SSL one. What is missing is the
other half of KIP-48: *using* a token means a SASL/SCRAM login whose user name is the token id and
whose password is the base64 HMAC, and this client speaks SASL/PLAIN only. The four apis are
verified against a real 1.1.1 broker, the login with their result is not implemented.

What the five lines can do beyond the api versions themselves. A cell that names a ticket
(`T<n>`) is a capability of Kafka 1.x that the line is still implementing:

| Feature                                                | Arrived in | `0.8.x` | `0.9.x` | `0.10.x` | `0.11.x` | `main` |
|--------------------------------------------------------|------------|---------|---------|----------|----------|--------|
| Message format v0 (no timestamps)                      | 0.8        | yes     | yes     | yes      | yes      | yes    |
| Message format v1 (timestamps, relative inner offsets) | 0.10.0     | –       | –       | yes      | yes      | yes    |
| Record batch v2 (headers, varints, CRC-32C)            | 0.11       | –       | –       | –        | yes      | yes    |
| Compression `gzip`, `snappy`                           | 0.8        | yes     | yes     | yes      | yes      | yes    |
| Compression `lz4`                                      | 0.10.0     | –       | –       | yes      | yes      | yes    |
| Transport `PLAINTEXT`                                  | 0.8        | yes     | yes     | yes      | yes      | yes    |
| Transport `SSL`                                        | 0.9        | –       | yes     | yes      | yes      | yes    |
| Transport `SASL_PLAINTEXT` / `SASL_SSL` (PLAIN)        | 0.10.0     | –       | –       | yes      | yes      | yes    |
| Consumer groups (`subscribe()`, assignors)             | 0.9        | –       | yes     | yes      | yes      | yes    |
| The group state `Empty`                                | 0.10.1     | –       | –       | yes      | yes      | yes    |
| The group state `CompletingRebalance` (`AwaitingSync` below) | 1.0  | –       | –       | –        | –        | **yes** |
| Client quotas and their `throttle_time_ms`             | 0.9        | –       | yes     | yes      | yes      | yes    |
| `throttle_time_ms` in the group and admin apis         | 0.11       | –       | –       | –        | yes      | yes    |
| `controller_id`, broker `rack`, `is_internal`          | 0.10.0     | –       | –       | yes      | yes      | yes    |
| `cluster_id` of Metadata v2                            | 0.10.1     | –       | –       | yes      | yes      | yes    |
| Offsets by timestamp, `offsetsForTimes()`              | 0.10.1     | –       | –       | yes      | yes      | yes    |
| `fetch.max.bytes` of Fetch v3                          | 0.10.1     | –       | –       | yes      | yes      | yes    |
| `max.poll.interval.ms` and the `rebalance_timeout`     | 0.10.1     | –       | –       | yes      | yes      | yes    |
| Admin: create and delete topics through the protocol   | 0.10.1     | –       | –       | yes      | yes      | yes    |
| Admin: `getApiVersions()`                              | 0.10.0     | –       | –       | yes      | yes, v1  | yes, v1 |
| Admin: `DeleteRecords`, `DescribeConfigs`/`AlterConfigs` | 0.11     | –       | –       | –        | yes      | yes    |
| Record headers end to end (KIP-82)                     | 0.11       | –       | –       | –        | yes      | yes    |
| `OffsetForLeaderEpoch`, `allow_auto_topic_creation`    | 0.11       | –       | –       | –        | yes      | yes    |
| Idempotent producer (`enable.idempotence`)             | 0.11       | –       | –       | –        | yes      | yes    |
| Transactional producer, `isolation.level`              | 0.11       | –       | –       | –        | yes      | yes    |
| Framed SASL exchange (`SaslAuthenticate`, KIP-152)     | 1.0        | –       | –       | –        | –        | yes    |
| `log_start_offset` of a produce answer, `offline_replicas` | 1.0    | –       | –       | –        | –        | **yes** |
| Incremental fetch sessions (KIP-227)                   | 1.1        | –       | –       | –        | –        | the frame; in the consumer: T8 |
| Dynamic broker configuration, config sources and synonyms (KIP-226) | 1.1 | –  | –       | –        | –        | T4     |
| Admin: `createPartitions()`, `deleteConsumerGroups()`  | 1.0 / 1.1  | –       | –       | –        | –        | T4     |
| Admin: `describeLogDirs()`, `alterReplicaLogDirs()`    | 1.0        | –       | –       | –        | –        | T5     |
| Delegation tokens (KIP-48)                             | 1.1        | –       | –       | –        | –        | **issued, renewed, expired, described** |
| Error codes                                            | –          | -1 … 20 | -1 … 31 | -1 … 44  | -1 … 55  | **-1 … 71** |

Everything a later Kafka added is missing here, by design:

| Feature                                          | Arrived in | On this branch                        |
|--------------------------------------------------|------------|---------------------------------------|
| Api keys above 42 (`ElectPreferredLeaders`, `IncrementalAlterConfigs`, …) | 2.2+ | no — `ApiKeys` stops at 42 |
| Error codes above 71 (`LISTENER_NOT_FOUND`, …)   | 2.0        | no — 1.1.1 defines -1 … 71            |
| Produce v6, Fetch v8, Metadata v6 and later      | 2.0+       | no — the api-key table is the ceiling |
| Leader epochs in Fetch/Offsets/OffsetCommit (KIP-320) | 2.1   | no — `OffsetForLeaderEpoch` stays at v0 |
| Flexible versions and tagged fields              | 2.4        | no — the request header is the plain one |
| SASL/SCRAM and SASL/GSSAPI                       | 0.10.2 / 0.9 | no — PLAIN only, which is why a delegation token can be issued but not used |

Five properties of a 1.1.1 broker regularly surprise clients, and this implementation deals
with all of them explicitly:

* **An api the broker does not serve costs the connection.** A request whose api key or version
  a 1.1.1 broker cannot parse — and a body that does not match the schema of a version it
  does serve — makes it **close the socket**: `Closing socket for … because of error` in the
  broker log, and the end of the stream for the client, reported as a `NetworkException`. A
  0.9.0.1 broker only dropped such a frame and kept the connection open, so code ported from
  that line waits for a timeout that will never come. There is exactly one exception:
  **ApiVersions** answers an unknown version with the error code 35 and survives. Up to Kafka 0.11
  there was a second, **ControlledShutdown**, which the broker still parsed with a Scala class that
  never looked at the version; Kafka 1.0 moved that api to the schemas of the Java client, so it now
  serves v0 and v1 and hangs up on anything above. This client only ever sends the api versions of
  the table above.
* **A group whose last member leaves does not disappear.** Since Kafka 0.10.1 it stays in the
  state `Empty` with its committed offsets until `offsets.retention.minutes` expires them, is
  still listed by `AdminClient::listGroups()` and is described as `Empty`, not `Dead`. That is
  what lets a restarted consumer of the same group resume where the group committed — and it is
  a behaviour change against 0.9, where the coordinator dropped such a group at once.
* **A fetch answer may be empty although the partition has data — and larger than the limit it
  asked for.** Fetch v3 (Kafka 0.10.1) added a request-level `fetch.max.bytes`, and the broker
  spends it on the partitions **in the order of the request**: a partition behind an exhausted
  budget comes back empty with its high water mark above the fetch offset, and the next poll,
  which rotates the served partitions to the back, picks it up. The other half of the same rule is
  that the first non-empty partition is always served whole, even when the single message exceeds
  the limit — which is why a consumer of this branch can no longer get stuck on a record that is
  bigger than `max.partition.fetch.bytes`.
* **The coordinator of a group is not available right away.** The first GroupCoordinator
  request for any group makes the broker create the internal `__consumer_offsets` topic and is
  answered with the error code 15 while that happens; code 14 means the coordinator is still
  reading the offsets of the group out of it. Both are retried with `retry.backoff.ms` until
  `metadata.fetch.timeout.ms` by `Common\CoordinatorLookup`, which
  `AdminClient::findCoordinator()` and `Client::getGroupCoordinator()` use.
* **The broker remembers the last five batches of a producer, and it drops record headers when it
  converts a batch down.** Both are Kafka 1.0 changes against the 0.11 line and both are invisible
  in the frame: a duplicate of any of the last five batches of a producer id and partition is
  answered as the original append (0.11 remembered one batch and answered 45 for anything older),
  and a Fetch below v4 of a partition whose records carry headers now succeeds with the headers
  silently removed, where a 0.11 broker refused the whole partition with the error code -1.

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

The suite is split in three — 1487 unit tests, 236 compliance tests replaying the 229 documented
wire vectors, and 426 integration tests against a real broker over its four listeners:

```bash
vendor/bin/phpunit --testsuite unit          # pure unit tests, no broker
vendor/bin/phpunit --testsuite compliance    # replays the documented wire vectors

docker compose up -d                         # Kafka 1.1.1: PLAINTEXT 9092, SSL 9093,
                                             #                 SASL_PLAINTEXT 9094, SASL_SSL 9095
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 vendor/bin/phpunit --testsuite integration
```

The integration suite is skipped unless `KAFKA_BOOTSTRAP_SERVERS` points at a running broker, and
each of the other three listeners has an environment variable of its own — a test that needs one
is skipped when it is unset:

| Variable | Default | What it runs |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | – | the whole integration suite, over the PLAINTEXT listener |
| `KAFKA_SSL_BOOTSTRAP_SERVERS` | `127.0.0.1:9093` | `SslTransportTest`, against the certificate the container was built with (`docker/kafka-1.1.1/ssl/broker.crt`) |
| `KAFKA_SASL_BOOTSTRAP_SERVERS` | – | the SASL/PLAIN tests over `SASL_PLAINTEXT` (`127.0.0.1:9094`) |
| `KAFKA_SASL_SSL_BOOTSTRAP_SERVERS` | – | the same exchange inside TLS (`127.0.0.1:9095`) |
| `KAFKA_CONTAINER` | `kafka-0-11-0-3` | the container the quota tests run `kafka-configs.sh` in |

The compliance suite replays every wire vector of
[docs/protocol/vectors](docs/protocol/vectors) — frames that a real Kafka broker sent or
accepted — through the request and response classes and checks that the annotated dumps of
[docs/protocol/1.1.md](docs/protocol/1.1.md) still hold the same bytes, and that every
`@see docs/protocol/1.1.md, section "…"` of the sources names a heading that exists, so the
document and the code cannot drift apart.

Examples
--------

Every file in [examples/](examples) is runnable against the container of `docker-compose.yml`:

| Example | What it shows |
|---|---|
| [`producer.php`](examples/producer.php) | batching, compression, keys and partitions, the `RecordMetadata` of a batch |
| [`consumer.php`](examples/consumer.php) | `assign()`, `seek()`, deserializers, the timestamps of a record |
| [`consumer-group.php`](examples/consumer-group.php) | `subscribe()`, the rebalance listener, `max.poll.interval.ms` — start it twice |
| [`record-headers.php`](examples/record-headers.php) | the record headers of Kafka 0.11 (KIP-82), written and read back end to end |
| [`idempotent-producer.php`](examples/idempotent-producer.php) | `enable.idempotence`: the producer id, the sequence numbers and what a duplicate batch answers |
| [`transactional-producer.php`](examples/transactional-producer.php) | `transactional.id`, the consume-transform-produce loop and a `read_committed` consumer |
| [`admin.php`](examples/admin.php) | brokers, cluster id and controller, topics, offsets, groups |
| [`create-topic.php`](examples/create-topic.php) | `createTopics()` / `deleteTopics()` with `validateOnly` and the error of a topic |
| [`admin-configs.php`](examples/admin-configs.php) | `describeConfigs()`, `alterConfigs()` and `deleteRecords()` — the admin apis of Kafka 0.11 |
| [`offsets-for-times.php`](examples/offsets-for-times.php) | `offsetsForTimes()`, `beginningOffsets()`, `endOffsets()` |
| [`ssl.php`](examples/ssl.php) | the SSL listener, 9093 |
| [`sasl.php`](examples/sasl.php) | SASL/PLAIN over 9094, and over 9095 with `KAFKA_SASL_SSL_BOOTSTRAP_SERVERS` |

Issues and pull requests are welcome.

License
-------

Released under the [MIT license](LICENSE).

[producer configuration]: https://kafka.apache.org/documentation/#producerconfigs
[consumer configuration]: https://kafka.apache.org/documentation/#newconsumerconfigs

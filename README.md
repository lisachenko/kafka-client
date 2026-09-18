PHP Native Apache Kafka Client — 3.x (Kafka 3.9.2)
==================================================

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lisachenko/kafka-client/ci.yml?branch=main)
[![Code Coverage](https://img.shields.io/codecov/c/github/lisachenko/kafka-client/main)](https://app.codecov.io/gh/lisachenko/kafka-client)
[![Minimum PHP Version](http://img.shields.io/badge/php-%3E%3D%208.4-8892BF.svg)](https://www.php.net/supported-versions.php)
[![License](https://img.shields.io/packagist/l/lisachenko/kafka-client.svg)](https://packagist.org/packages/lisachenko/kafka-client)

`lisachenko/kafka-client` is a native, pure-PHP implementation of the Apache Kafka wire
protocol — no `ext-rdkafka` required. It ships a Producer, a Consumer and a low-level Admin
client, designed to stay close in spirit to the official Java client's API while feeling
natural in PHP.

**This branch is the 3.x line and is being built towards the Apache Kafka 3.9.2 wire protocol** — the last
release of the 3.x major, so everything Kafka 3.0 to 3.9 added — one Kafka minor at a time, on top of the
finished 2.x line. Today it speaks **Kafka 2.8.2** plus the constants of 3.9.2 (the api keys 65–87 and the
error codes 105–127); the milestone the line has reached is stated under
[Supported Kafka protocol versions](#supported-kafka-protocol-versions). `main` is the top of the cascade:
the frozen protocol snapshots below it live on `2.x` (Kafka 2.8.2), `1.x` (Kafka 1.1.1), `0.11.x`
(Kafka 0.11.0.3), `0.10.x` (Kafka 0.10.2.2), `0.9.x` (Kafka 0.9.0.1) and `0.8.x` (Kafka 0.8.2.2), and every
wire vector those lines captured is replayed against the classes of this branch. The grammar this branch
implements is written down, byte for byte, in [docs/protocol/3.9.md](docs/protocol/3.9.md), verified against
a real Kafka 3.9.2 node running in KRaft mode; the plan of the line (and, once it is complete, its release
record) is [docs/handoff/main.md](docs/handoff/main.md), the 2.x line's record is
[docs/handoff/2.x.md](docs/handoff/2.x.md) and the 1.x line's [docs/handoff/1.x.md](docs/handoff/1.x.md).

Installation
------------

```bash
composer require lisachenko/kafka-client:dev-main
```

**PHP 8.4 or newer, and nothing else** — `ext-openssl` is needed only for `SSL`/`SASL_SSL` and
`ext-zlib` (bundled with PHP) for `gzip` and `ext-zstd` for the `zstd` codec of Kafka 2.1; the
`snappy` and `lz4` codecs are implemented in PHP and use `ext-snappy` only when it happens to be
installed. `main` is the branch the top of the cascade lives on, so it is installed by branch name;
the frozen lines below it carry a numeric branch and are installed by constraint (`^2.8@dev` for
`2.x`, `^1.1@dev` for `1.x`, `^0.11@dev` for `0.11.x`, `^0.10@dev` for `0.10.x`, and so on). A line is
frozen as a numeric branch when the line above it starts, so code that must keep speaking Kafka 2.8.2
pins the branch rather than `dev-main`.

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
whole batch: `gzip`, `snappy`, `lz4` (Kafka 0.10.0) and `zstd` (Kafka 2.1, KIP-110, through
`ext-zstd`), in the frame format of the Kafka producer including the KAFKA-3160 checksum quirk of a
message format v0 frame.

**Message formats, timestamps and headers.** Kafka 0.10.0 gave every record a timestamp:
`Record::$timestamp` (milliseconds since the epoch) and `Record::$timestampType`
(`TimestampType::CREATE_TIME`, `LOG_APPEND_TIME` or `NO_TIMESTAMP_TYPE`), and Kafka 0.11 gave it
**headers** (`Record::withHeaders()`, `Common\Record\Header`), a list of key-value pairs of
metadata next to the key and the value. `send()` stamps the create time of every record that does
not carry one, and `ProducerConfig::MESSAGE_FORMAT_VERSION` (`message.format.version`, `0.11.0` by
default) selects the format a batch is written in — the record batch v2 by default, `0.10.x` for a
message set with timestamps and `0.9.0` for one without. The format decides the version of the
Produce request: only the message format v2 travels in a **Produce v11**, and only it has a place
for the headers, for the producer id of an idempotent producer and for a transaction; a message set
is sent as a Produce v2, and a 2.8.2 broker answers **87** `INVALID_RECORD` for every partition of
a Produce v3 or above that carries one. `RecordMetadata::$timestamp` reports what the **log** holds:
the create time of the first record of the batch, or the `LogAppendTime` the broker answered with
(Produce v2 and above)
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
$ kafka-configs.sh --bootstrap-server localhost:9092 --alter \
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

A **Kafka 1.x** broker widens the guarantee in two ways that need no option: it recognises a
duplicate of any of the **last five** batches of a producer and partition, not only of the very
last one, so a producer whose acknowledgements of several batches in a row were lost is still
answered with the original offsets instead of being thrown out of sequence; and it tells a client
when it has lost the state of a producer altogether, which a 0.11 broker could not.

Three error codes of the broker say something about the producer state itself. `47`
(`ProducerFencedException`) means another producer took the producer id over: the producer is
finished and refuses every further send. `45` (`OutOfOrderSequenceException`) means the producer
and the broker no longer agree on what is in the log: the batch that hit it is reported to the
caller, and the producer starts over with a new producer id — everything written under the old one
loses its deduplication. `59` (`UnknownProducerIdException`, Kafka 1.0, a subclass of the previous
one) means the broker has no state of this producer for that partition — because every record it
wrote there was deleted by `deleteRecords()` or by a retention run. That one the producer
**repairs by itself**: the `logStartOffset` that Produce v5 added to the answer shows that the
records fell below the start of the log, so the partition is numbered from the sequence 0 again
and the batch is sent once more, under the same producer id and without touching any other
partition. All three are documented, with what a real 1.1.1 broker answers, in
[docs/protocol/3.9.md](docs/protocol/3.9.md), section "The idempotent producer".

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
[docs/protocol/3.9.md](docs/protocol/3.9.md), section "Transactions".

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
(`isolation.level`, `read_uncommitted` by default) is sent as the isolation level of the Fetch v16
request: with `read_committed` the broker answers only up to the **last stable offset** — the first
record of a transaction that has neither committed nor aborted — and names the aborted transactions
of the answer, whose records the consumer drops. The control batches of the transaction protocol are
never handed to an application in either level. The option travels in the **Offsets v6** request as
well, so `endOffsets()`, `position()` and `seekToEnd()` of a `read_committed` consumer answer the last
stable offset instead of the log end offset — a consumer that compares its position against the end of
a partition compares it against the offset it can really reach. `AdminClient::listOffsets()` stays at
`read_uncommitted` on purpose: an administrator asks what is in the log.

**Incremental fetch sessions** (Kafka 1.1, KIP-227) are what the consumer adds on this side. Every
broker it reads from holds a *fetch session* for it: the first request states the whole assignment
and opens the session, every following one states only the partitions whose position moved and lets
the broker fill in the rest, a partition that leaves the assignment — a rebalance, `pause()`, a
topic that is gone — is dropped from the session with the `forgotten_topics_data` of the next
request, and the answer carries only the partitions that have news. A consumer of many partitions
therefore stops repeating its partition list in every fetch, and the broker stops answering
partitions that have nothing to say. None of it is visible in `poll()`: the error codes **70**
(`FetchSessionIdNotFound`, the broker no longer knows the session) and **71**
(`InvalidFetchSessionEpoch`, a request or an answer was lost) are answered with a full fetch by the
client itself, in the same call, and a broker that hands out no session at all — one below Kafka
1.1, or one whose cache of 1000 sessions is full — leaves the consumer on plain full fetches.
`Client::fetchPartitions()` keeps its session-less behaviour for callers that want one request and
one answer; the consumer fetches through `Client::fetchPartitionsWithSessions()`.

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

`group.protocol = consumer` switches the consumer to the **new consumer protocol of KIP-848**
(Kafka 3.5). The four apis of the classic membership protocol are then replaced by the single
**ConsumerGroupHeartbeat** (key 68), and three things change for an application: the
**coordinator** computes the assignment, so `partition.assignment.strategy` has nothing to say
and `group.remote.assignor` names a *server-side* assignor instead; the heartbeat interval is
dictated by the broker (`group.consumer.heartbeat.interval.ms`) rather than by
`heartbeat.interval.ms`; and a rebalance is **incremental** — a member gives up only the
partitions it really loses, so `onPartitionsRevoked()` sees exactly those and
`onPartitionsAssigned()` only the ones that were added, where the classic protocol hands the
whole assignment back and forth on every rebalance. The member epoch takes the place of the
generation and travels in the `OffsetCommit` v9 and `OffsetFetch` v9 of that member. A group is
of one protocol or the other: a heartbeat for a classic group is refused with the **69**, and
`AdminClient::describeConsumerGroups()` (key 69) describes the new groups where
`describeGroups()` (key 15) describes the classic ones.

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
| `getApiVersions()`                           | ApiVersions v4          | The version range of every api of **one** broker, indexed by api key; version 1 carries the throttle time, version 3 the features of KIP-584 and version 4 the ones whose minimum version is 0 (KAFKA-17011, Kafka 3.9) |
| `findAllBrokers()`                           | Metadata v12             | An empty result means "the cluster is not ready yet", see below      |
| `listTopics()` / `describeTopics()`          | Metadata v12             | Asks with `allow_auto_topic_creation = false`, so an unknown topic is answered 3 and **not** created; `describeTopics([])` asks for every topic (the `null` array of v1); every partition reports its `offlineReplicas` (v5, KIP-112/113) |
| `describeTopicsByIds()`                      | Metadata v12             | Names the topics by their **topic id** (KIP-516, Kafka 3.1), the `describeTopics(TopicCollection.ofTopicIds(...))` of the Java admin client; an id the cluster does not host is answered 100 `UnknownTopicId` with a `null` name |
| `findController()`                           | Metadata v12             | The `controller_id` of the answer; the two topic apis below need it   |
| `createTopics()`                             | CreateTopics v7         | `NewTopic` with partitions/factor or an explicit assignment, plus topic configs; `validateOnly` checks without creating |
| `deleteTopics()`                             | DeleteTopics v6         | Needs `delete.topic.enable=true` on the broker                        |
| `listOffsets()`                              | Offsets v9              | Earliest, latest, by message timestamp, `OffsetsRequest::MAX_TIMESTAMP`, `OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP` or `OffsetsRequest::LATEST_TIERED_TIMESTAMP`; **one** offset per partition, sent to the partition leader, with the isolation level `read_uncommitted` |
| `listMaxTimestampOffsets()`                  | Offsets v9              | The offset **and the timestamp** of the record with the largest timestamp of every partition (KIP-734, Kafka 3.0), `null` for an empty log — the end of the log only while a log's timestamps rise with its offsets |
| `listEarliestLocalOffsets()`                 | Offsets v9              | The **local log start offset** of every partition (the target time `-4` of KIP-405, Kafka 3.5): the first offset still on the broker's own disk once older segments moved to tiered storage; on a broker without remote storage it equals the earliest offset (the node answers `0` with the timestamp `-1` for an empty log) |
| `listLatestTieredOffsets()`                  | Offsets v9              | The **last tiered offset** of every partition (the target time `-5` of KIP-1005, Kafka 3.9): the last offset that has been moved to remote storage, the upper end of the range `listEarliestLocalOffsets()` names the lower end of; on a broker without remote storage it is **-1** with the error code 0 — "nothing of this partition is tiered", on a filled log as on an empty one |
| `findCoordinator()`                          | GroupCoordinator v6     | Retries the codes 15 and 14 while the coordinator warms up; version 1 also looks a **transactional id** up (`coordinator_type = 1`); one key travels as a one-element batch of the v4 of KIP-699 (Kafka 3.0), and `Client::getGroupCoordinators()` / `getTransactionCoordinators()` look several up at once; the v5 of KIP-890 (Kafka 3.8) and the v6 of KIP-932 (Kafka 3.9) added no field — the v6 only widens what `coordinator_type` may say, and the share type 2 it legalises is answered 15 by a node that has no share coordinator, so this client never sends it |
| `listGroupOffsets()`                         | OffsetFetch v9          | Without a partition list it asks for **every** topic the group committed (`null` topics of v2); one group travels as a one-element batch of v8 (Kafka 3.0), with the `member_id` / `member_epoch` of KIP-848 at their defaults (v9, Kafka 3.7) |
| `listConsumerGroupOffsets()`                 | OffsetFetch v9          | The committed offsets of **several** groups in one request per coordinator (Kafka 3.0), each group with its own topic array, its own member of KIP-848 (v9, Kafka 3.7) and its own error code; an empty batch is refused client-side, because a 3.9.2 node answers it with nothing at all |
| `listGroups()` / `listAllGroups()`           | ListGroups v5           | A broker only knows its own groups; `listAllGroups()` merges them all; an optional **state** filter (KIP-518) and **type** filter (KIP-848, Kafka 3.8) bound the answer |
| `describeGroup()` / `describeGroups()`       | DescribeGroups v5       | Sent to the coordinator of the group; an unknown group answers `Dead`, one whose last member left `Empty` — and so does a group of the **new** consumer protocol, which this api cannot describe |
| `describeConsumerGroup()` / `describeConsumerGroups()` | ConsumerGroupDescribe v0 | The KIP-848 half of the question (Kafka 3.7): the group epoch, the assignment epoch, the server-side assignor and per member its member epoch, its subscription as plain topic names and **both** assignments, as an `Admin\ConsumerGroupDescription`; a **classic** group is the **69** `GroupIdNotFound` here |
| `electLeaders()`                             | ElectLeaders v2         | Asks the **controller** to move partitions back to their preferred replica (KIP-183, Kafka 2.2); per-partition results, 84 for a partition that already has the right leader; `ElectionType::UNCLEAN` needs the v1 of KIP-460 |
| `deleteRecords()`                            | DeleteRecords v2        | Moves the **low watermark** of a partition forward (KIP-107); sent to the partition leader, answers a `DeletedRecords` per partition |
| `describeConfigs()`                          | DescribeConfigs v4      | The configuration of a topic or a broker (KIP-133); every entry says which `ConfigSource` its value comes from and, with `$includeSynonyms`, every place the broker looked (KIP-226). A broker resource is only answered by that broker, and a sensitive value comes back `null` |
| `alterConfigs()`                             | AlterConfigs v2         | **Replaces** the whole configuration of a resource (`Config::ownValues()` is the set to send back); a 1.1 broker takes a **broker** resource too — the dynamic options of KIP-226, per broker or cluster-wide with `ConfigResource::defaultBroker()` — and refuses the ones it cannot change at runtime with 42 |
| `incrementalAlterConfigs()`                  | IncrementalAlterConfigs v1 | Changes **single options** of a topic or a broker (KIP-339, Kafka 2.3) and leaves the ones it does not name alone — `AlterConfigOp::set()`, `delete()`, `append()` and `subtract()`; the api Kafka 2.3 put in place of `alterConfigs()` |
| `describeClientQuotas()`                     | DescribeClientQuotas v1 | The quotas of users, client ids and their defaults, filtered by entity (KIP-546, Kafka 2.6); `ClientQuotaFilter` and `ClientQuotaEntity` as in the Java admin client |
| `alterClientQuotas()`                        | AlterClientQuotas v1 | Sets or removes the producer, consumer and request quotas of an entity (KIP-546); one error per entity, `validateOnly` checks without writing |
| `listConsumerGroups()`                       | ListGroups v5 | The groups of the `consumer` protocol type of every broker, with their state (KIP-518, Kafka 2.6) and their **type** — `classic` or `consumer` — plus an optional state and type filter (KIP-848, Kafka 3.8) |
| `describeUserScramCredentials()`             | DescribeUserScramCredentials v0 | The SCRAM mechanisms and iteration counts of users (KIP-554, Kafka 2.7); the credentials themselves never travel |
| `alterUserScramCredentials()`                | AlterUserScramCredentials v0 | Upserts and deletes SCRAM credentials of users (KIP-554); the salted password is computed by the client, one error per user |
| `describeFeatures()` / `updateFeatures()`    | ApiVersions v4 / UpdateFeatures v1 | The finalized and supported feature versions of the cluster and their upgrade or downgrade on the **controller** (KIP-584, Kafka 2.7; the `UpgradeType` and the `validateOnly` dry run of KIP-778, Kafka 3.3) |
| `describeMetadataQuorum()`                   | DescribeQuorum v2 | The leader, the epoch, the high watermark and every voter and observer of the metadata quorum of a KRaft cluster (KIP-595, Kafka 2.7; the two replica timestamps of KIP-836, Kafka 3.3; the directory id of a replica, the two error messages and the `Admin\QuorumNode` endpoints of KIP-853, Kafka 3.9) |
| `describeCluster()`                          | DescribeCluster v1 | The brokers, the controller and the cluster id of a cluster, with the authorized operations of KIP-430 on request (KIP-700, Kafka 2.8), and the `endpoint_type` of KIP-919 (Kafka 3.7): `Admin\EndpointType::Broker` by default, `Controller` to ask a controller listener for the controllers — a broker listener refuses that with 114; `describeClusterFromMetadata()` asks Metadata instead, as every line below did |
| `describeTransactions()`                     | DescribeTransactions v0 | The state, producer id and epoch, timeout, start time and partitions of transactional ids, each from its transaction coordinator (Kafka 3.0); an id the coordinator does not know is `TransactionalIdNotFoundException` (105) in its place |
| `listTransactions()`                         | ListTransactions v1 | The transactions of the cluster, asked of every broker and merged, filtered by state, by producer id and — since Kafka 3.8 (KIP-994) — by the **age** of the transaction in milliseconds; the state filters no coordinator knew come back through the third parameter |
| `describeTopicPartitions()`                  | DescribeTopicPartitions v0 | The topics of a cluster **page by page** (KIP-966, Kafka 3.8), with the eligible leader replicas and the last known ELR of every partition and the authorized operations of the topic; an empty topic list is every topic, the client walks the `next_cursor` until the listing is complete, and a topic the cluster does not host is `UnknownTopicOrPartitionException` (3) in its place |
| `describeProducers()`                        | DescribeProducers v0 | The active producers of partitions: producer id, epoch, last sequence and timestamp, and the start offset of an open transaction (KIP-664, Kafka 2.8) |
| `createTopicsWithResults()`                  | CreateTopics v7 | The same creation, answered with what the broker made of it (KIP-525, Kafka 2.4): `CreatedTopic` with the partition count, the replication factor and every configuration entry of the new topic; `NewTopic::withBrokerDefaults()` asks for `num.partitions` and `default.replication.factor` (KIP-464) |
| `alterPartitionReassignments()`              | AlterPartitionReassignments v0 | Moves the replicas of partitions to other brokers, or cancels a move with `null` (KIP-455, Kafka 2.4); sent to the **controller**, one error per partition |
| `listPartitionReassignments()`               | ListPartitionReassignments v0 | The reassignments in flight, with the target, adding and removing replica lists of each partition (KIP-455) |
| `removeMembersFromConsumerGroup()`           | LeaveGroup v5 | Removes members of a group by hand, a static one by its `group.instance.id` (KIP-345, Kafka 2.4); one error per member, `MemberToRemove::byInstanceId()`/`byMemberId()`; since v5 (Kafka 3.2) every entry names a `reason`, `member was removed by an admin` unless the caller gives one |
| `deleteConsumerGroupOffsets()`               | OffsetDelete v0 | Deletes the committed offsets of single partitions of a group (KIP-496, Kafka 2.4); an `Empty` group hands over everything, a live consumer group answers 86 for the topics it consumes, another protocol type 68 and an unknown group 69 |
| `describeLogDirs()`                          | DescribeLogDirs v4      | What each **log directory** of a broker holds (KIP-113); broker-local, so it takes a list of broker ids — a `null` selection asks for every replica, an empty one only for the directories; since v3 (Kafka 3.2) a refusal of the whole request is a **top-level error code** and is thrown (31 for a principal that may not describe the cluster); since v4 (Kafka 3.3) every directory reports the **total and usable bytes** of its volume (KIP-827) |
| `describeAcls()` / `createAcls()` / `deleteAcls()` | DescribeAcls v3 / CreateAcls v3 / DeleteAcls v3 | The acls of the cluster (Kafka 3.3, the first line of this package to speak them): a `Common\AclBinding` is a resource pattern (`LITERAL` or `PREFIXED`, the `USER` resource of KIP-373 included) and an access control entry; a describe or a delete names an `AclBindingFilter` whose fields may be wildcards, `MATCH` asks which acls apply to a resource; measured against the `StandardAuthorizer` of the node with the principal `acltest` |
| `alterReplicaLogDirs()`                      | AlterReplicaLogDirs v2  | Moves a replica to another log directory of the broker that hosts it (KIP-113); the answer only says the move was **accepted**, `describeLogDirs()` says when it is done |
| `createPartitions()`                         | CreatePartitions v3     | Raises the partition count of topics that exist (KIP-195); controller-only like `createTopics()`, and it can only ever grow a topic (37 otherwise) |
| `deleteConsumerGroups()`                     | DeleteGroups v2         | Makes the coordinator forget groups and their committed offsets (KIP-229); a group with a live member is 68, one the coordinator does not know 69 |
| `createDelegationToken()`                    | CreateDelegationToken v3   | Issues a token to the principal of the connection (KIP-48), or to another principal with the `$owner` of KIP-373 (Kafka 3.3; 65 without the `CREATE_TOKENS` acl); needs an **authenticated** channel, otherwise 64 |
| `renewDelegationToken()`                     | RenewDelegationToken v2    | Extends a token named by its raw HMAC; only its owner or one of its renewers may, otherwise 63 |
| `expireDelegationToken()`                    | ExpireDelegationToken v2   | Moves the expiry forward, or **removes** the token when the period is negative |
| `describeDelegationToken()`                  | DescribeDelegationToken v2 | The tokens of the given owners, `null` for every token the principal may see; the answer carries their HMACs |

The three topic apis — `createTopics()`, `deleteTopics()` and `createPartitions()` — are served by
the **controller** alone: `AdminClient` looks it up in the `controller_id` of a Metadata answer, and
repeats the request once against a freshly looked up controller when a topic comes back with the
error code 41 (`NotController`). None of them throws for a topic: the result has one entry per
requested topic, in the order of the request, `null` when it worked and the exception of its error
code — with the `error_message` the controller sent in the context — when it did not, because one
topic of a batch says nothing about the others. `deleteConsumerGroups()` reports its groups the same
way, and sends one request to the coordinator of each of them.

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
(key 20) arrived with Kafka 0.10.1, and `AdminClient::createTopics()` sends the version 7 of the first
one, with `validate_only`, the per-topic `error_message`, the shape and configuration of the new topic in the
answer (KIP-525) and the topic id of KIP-516 next to them. The implicit creation by a Metadata
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
which is what `Config::ownValues()` exists for (`nonDefaultValues()` is the 0.11 name and, since
KIP-226, also reports options that only the *broker* configuration sets). A **broker** resource is where this line differs
from the one below it: KIP-226 made a 1.1 broker accept one and validate it **per option**, so an
option it cannot change at runtime comes back as the error code 42 with
`Cannot update these configs dynamically: Set(log.retention.hours)` while a dynamic one is applied,
where a 0.11 broker refused every broker resource outright. Reading such a resource changed too — the
`is_default` of an entry is derived from the KIP-226 config *source* and `is_read_only` means "not
dynamically updatable". This client sends **DescribeConfigs v1**, which reports that source directly
and, with `$includeSynonyms`, every place the broker looked for the value; the version 0 frame is
kept for the vectors of the line below and derives the source back from the boolean, which is lossy.

**The disks of a broker and the tokens of a principal** are the two api families Kafka 1.x added on
top of that. `describeLogDirs()` and `alterReplicaLogDirs()` (KIP-113) say which `log.dirs` entry a
replica lives in and move it to another one — both broker-local, so they are addressed by broker id
and by an `Admin\TopicPartitionReplica` rather than by a partition leader. The four token apis of
KIP-48 issue, renew, expire and describe a delegation token over an authenticated connection; what
they cannot do is *use* one, because authenticating with a token is a SASL/SCRAM login.

[examples/admin.php](examples/admin.php), [examples/create-topic.php](examples/create-topic.php),
[examples/admin-configs.php](examples/admin-configs.php),
[examples/admin-log-dirs.php](examples/admin-log-dirs.php) and
[examples/delegation-tokens.php](examples/delegation-tokens.php) run all of it against the broker of
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
any broker can answer — Metadata and DescribeCluster — is tried on the brokers of the cluster in turn until one of
them answers. `controlledShutdown()` is gone from this line: ControlledShutdown (key 7) is served on the controller
listener of a KRaft node only, never on a client listener.

PHP-specific configuration
---------------------------

A few configuration options exist purely to make the client work well under PHP's
process-per-request model:

- `metadata.cache.file` — file used to cache cluster metadata; effectively cached by opcache in production.
- `stream.async.connect` — whether to connect to brokers asynchronously.
- `stream.persistent.connection` — whether to keep a persistent connection to the cluster.

For publishing from web requests, enabling persistent connections together with a metadata
cache file keeps producing as fast as possible.

The offsets of a consumer group always go to the coordinator of the group, with OffsetCommit **v9**
and OffsetFetch **v7**, and live in the `__consumer_offsets` topic: the commit carries the member id,
the generation and the `group.instance.id` of a group member and the leader epoch of every offset,
but **no** `RetentionTime` any more (KIP-211 took the field out at version 5, so
`offsets.retention.minutes` of the broker alone decides), and the fetch is the one that can ask for
*every* topic the group committed and the one that can insist on **stable** offsets (KIP-447). The
ZooKeeper storage of Kafka 0.8.1 (the version 0 of both apis, the `offsets.storage` option of the
lines below) is gone from this line: a KRaft node answers both v0 requests with 35.

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
| `metadata.cache.file`, `stream.async.connect`, `stream.persistent.connection` | – / false / false | the PHP-specific options above |

**Consumer** (`Consumer\ConsumerConfig`)

| Option | Default | Meaning |
|---|---|---|
| `group.id` | `''` | group to join with `subscribe()`, and the group a commit belongs to |
| `group.protocol` **(3.5)** | `classic` | `classic` (JoinGroup, SyncGroup, Heartbeat, LeaveGroup) or `consumer`, the **KIP-848** protocol: one ConsumerGroupHeartbeat, an assignment computed by the coordinator and an **incremental** rebalance |
| `group.remote.assignor` **(3.5)** | `null` | the server-side assignor of a `group.protocol=consumer` member — `uniform` or `range` on a 3.9.2 node — `null` lets the coordinator pick; a name the broker does not have is the **112** |
| `partition.assignment.strategy` | `range` | `range`, `roundrobin` or a `PartitionAssignorInterface` class; not used at all by `group.protocol=consumer`, where the **broker** assigns |
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
| `offset.retention.ms` | -1 | `RetentionTime` of an OffsetCommit up to v4; **KIP-211 removed the field in v5**, so the broker's `offsets.retention.minutes` alone decides and the version this client sends ignores the option |
| `exclude.internal.topics` | true | hides `__consumer_offsets` from `Cluster::topics()` |
| `check.crcs` | true | verify the CRC of every message |
| `key.deserializer` / `value.deserializer` | – | class names; a poll then returns `ConsumerRecord`s |

**Producer** (`Producer\ProducerConfig`)

| Option | Default | Meaning |
|---|---|---|
| `acks` | 1 | `0` fire-and-forget, `1` the leader's log, `-1` all in-sync replicas |
| `timeout.ms` | 2000 | how long the broker waits for the replicas of a batch |
| `batch.size` / `linger.ms` | 0 / 0 | when a batch is sent |
| `compression.type` | `none` | `none`, `gzip`, `snappy`, **(0.10)** `lz4`, **(2.1)** `zstd` (needs `ext-zstd`) |
| `message.format.version` **(0.10)** | `0.11.0` | format a batch is written in, and with it the Produce version: `0.9.0` and below format v0, `0.10.x` format v1 with timestamps (both a Produce v2), `0.11.0` the record batch v2 with headers (a Produce v11) |
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
self-signed certificate is checked in as `docker/kafka-3.9.2/ssl/broker.crt`.

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

This branch is the **3.x line** and tracks the **Kafka 3.9.2** wire protocol — the last release of the
3.x major, so everything Kafka 3.0 to 3.9 added — and it is built **one Kafka minor at a time**: each
minor is a gated milestone commit of the branch (the tag points are listed in
[docs/handoff/main.md](docs/handoff/main.md)), and until the line is complete the table below says
which versions the current milestone has reached. **Current milestone: none yet — the foundation** (the
Kafka 3.9.2 KRaft node, the api keys 65–87, the error codes 105–127 and the api table below; every api is
still at the version the 2.x line sends). The frozen protocol snapshots of the lines below live on `2.x`
(Kafka 2.8.2), `1.x` (Kafka 1.1.1), `0.11.x` (Kafka 0.11.0.3), `0.10.x` (Kafka 0.10.2.2), `0.9.x`
(Kafka 0.9.0.1) and `0.8.x` (Kafka 0.8.2.2).

Kafka 0.10.0 added the **ApiVersions** request (key 18), so this line does not have to guess what
its broker speaks. The table below is the literal answer of the 3.9.2 container — a **KRaft** node,
broker and controller in one process — on its client listeners: **61 apis**, the keys 0–3, 8–51, 55, 57,
60, 61, 64–66, 68, 69, 74, 75, 80 and 81. The ZooKeeper-only apis 4–7 and 56, the controller-only apis
52–54, 58, 59, 62, 63, 67, 70, 73 and 82, the client-metrics apis 71 and 72 (hidden while the node has no
telemetry plugin) and the early-access share-group apis 76–79 and 83–87 (hidden without
`unstable.api.versions.enable`) are not in it. The answer is read with `Client::apiVersions()` and pinned
by `tests/Integration/ApiVersionProbeTest.php`, which sends one real frame of every key at its maximum
version and one above it.

The "main" column lists the versions this client has a class for; the one in **bold** is the version
it sends, and a version the node serves that the current milestone has not reached yet is named as
**not yet implemented on this line** — with the KIP-848 consumer wave, the last one of the line, there is
none left: every key the node serves on a client listener has a class here. The `2.x` column is where the
line started.

| Api key | API | Versions in 3.9.2 | Client-facing | `2.x` | `main` (3.x, towards Kafka 3.9.2) |
|---|---|---|---|---|---|
| 0 | Produce | v0 … v11 | yes | v0 … v8, **v9** (**v2** for `message.format.version` below 0.11.0) | v0 … v10, **v11** (the promise to understand the **120** `TransactionAbortable` of KIP-890, Kafka 3.8, which the node answers a transactional partition the coordinator has not verified where a v10 is answered the 48; `ProduceRequestV10`/`ProduceResponseV10` keep the leader discovery of KIP-951, `ProduceRequestV9` the flexible v9 and `ProduceResponsePartitionV8` its partition entry; **v2** for `message.format.version` below 0.11.0) |
| 1 | Fetch | v0 … v17 | yes | v0 … v11, **v12** (session-less in `fetchPartitions()`, with an **incremental fetch session per broker** in the consumer) | v0 … v16 and **v17** (the tagged `replica_directory_id` of KIP-853, Kafka 3.9, in every partition entry — the log directory a **follower** fetches for, left off the wire by a consumer whose zero uuid is the default of the field; the tagged `node_endpoints` of the answer, KIP-951, Kafka 3.7, which the node writes for the 6 of a follower fetch; every topic named by its **topic id**, KIP-516, Kafka 3.1; session-less in `fetchPartitions()`, with an incremental fetch session per broker in the consumer; `FetchRequestV12` keeps the frame that names its topics, `FetchRequestV13` to `FetchRequestV16` the versions below) |
| 2 | Offsets (ListOffsets) | v0 … v9 | yes | v0 … v5, **v6** | v0 … v8 and **v9** (the last tiered offset `-5` of KIP-1005, Kafka 3.9, `AdminClient::listLatestTieredOffsets()`; the local log start offset `-4` of KIP-405, Kafka 3.5, `AdminClient::listEarliestLocalOffsets()`; `OffsetsRequestV8` keeps the version of the local log start offset and `OffsetsRequestV7` the one of the max timestamp `-3` of KIP-734, Kafka 3.0) |
| 3 | Metadata | v0 … v12 | yes | v0 … v10, **v11** (v10 with the topic ids of KIP-516) | v0 … v11, **v12** (a request **by topic id**, `MetadataRequest::byTopicIds()`, KIP-516, Kafka 3.1; `MetadataRequestV11` keeps the version below it) |
| 4 | LeaderAndIsr | not on the client listener of a KRaft node | broker→broker | no | no |
| 5 | StopReplica | not on the client listener of a KRaft node | broker→broker | no | no |
| 6 | UpdateMetadata | not on the client listener of a KRaft node | broker→broker | no | no |
| 7 | ControlledShutdown | not on the client listener of a KRaft node | controller | v0 … v2, **v3** | v0 … v2, **v3** — wire only: the classes and the vectors stay, `controlledShutdown()` is gone from the admin client |
| 8 | OffsetCommit | v0 … v9 | yes | v0 … v7, **v8** (**v0** for `offsets.storage = zookeeper`) | v0 … v8, **v9** (Kafka 3.6, the v8 frame that may carry the 69 of an unknown group and the 113 of a KIP-848 member epoch; `OffsetCommitRequestV8` keeps the flexible v8) |
| 9 | OffsetFetch | v0 … v9 | yes | v0 … v6, **v7** (**v0** for `offsets.storage = zookeeper`) | v0 … v7, **v8** (several groups in one request, Kafka 3.0), **v9** (the member id and member epoch of KIP-848 per group entry, Kafka 3.7 — the version this client sends; `OffsetFetchRequestV8` keeps the batch of Kafka 3.0) |
| 10 | GroupCoordinator (FindCoordinator) | v0 … v6 | yes | v0 … v2, **v3** | v0 … v3, **v4** (the `coordinator_keys` batch of KIP-699, Kafka 3.0; `GroupCoordinatorRequestV3` keeps the single key), **v5** (the same frame with the promise of the code 120 of KIP-890, Kafka 3.8), **v6** (the third coordinator type of KIP-932, Kafka 3.9, the version this client sends: `COORDINATOR_TYPE_SHARE` is a constant and a measurement — the node answers a share lookup the **15** of a coordinator it does not have, and the versions below it the **42**; `GroupCoordinatorRequestV5`/`ResponseV5` and the V4 pair keep the versions below) |
| 11 | JoinGroup | v0 … v9 | yes | v0 … v6, **v7** | v0 … v8, **v9** (the `reason` of KIP-800 at v8, the `skip_assignment` of KIP-814 at v9, Kafka 3.2; `JoinGroupRequestV7`/`V8` keep the versions below) |
| 12 | Heartbeat | v0 … v4 | yes | v0 … v3, **v4** | v0 … v3, **v4** |
| 13 | LeaveGroup | v0 … v5 | yes | v0 … v3, **v4** | v0 … v4, **v5** (the `reason` of KIP-800 per member, Kafka 3.2; `LeaveGroupRequestV4` keeps the version below) |
| 14 | SyncGroup | v0 … v5 | yes | v0 … v4, **v5** | v0 … v4, **v5** |
| 15 | DescribeGroups | v0 … v5 | yes | v0 … v4, **v5** | v0 … v4, **v5** |
| 16 | ListGroups | v0 … v5 | yes | v0 … v3, **v4** | v0 … v3, **v4** (the states filter and the group state of KIP-518), **v5** (the `types_filter` of the request and the `group_type` of every entry, KIP-848, Kafka 3.8, the version this client sends; `ListGroupsRequestV4`/`ResponseV4` and `ListGroupResponseProtocolV4` keep the version below) |
| 17 | SaslHandshake | v0, v1 | yes | v0, **v1** | v0, **v1** |
| 18 | ApiVersions | v0 … v4 | yes | v0 … v2, **v3** | v0 … v3, **v4** (Kafka 3.9, KAFKA-17011: the v3 frame, and the answer reports a supported feature whose `min_version` is 0 — `kraft.version` on a KRaft node; `ApiVersionsRequestV3`/`ResponseV3` keep the version below) |
| 19 | CreateTopics | v0 … v7 | controller | v0 … v6, **v7** | v0 … v6, **v7** |
| 20 | DeleteTopics | v0 … v6 | controller | v0 … v5, **v6** | v0 … v5, **v6** |
| 21 | DeleteRecords | v0, v1, v2 | yes | v0, v1, **v2** | v0, v1, **v2** |
| 22 | InitProducerId | v0 … v5 | yes | v0 … v3, **v4** | v0 … v4, **v5** (the code 120 of KIP-890, Kafka 3.8, which a 3.9.2 coordinator never answers here — a fenced producer is still the 90; `InitProducerIdRequestV4`/`ResponseV4` keep the frame of KIP-588) |
| 23 | OffsetForLeaderEpoch | v0 … v4 | broker→broker | v0 … v3, **v4** (classes, vectors and the consumer's truncation detection) | v0 … v3, **v4** (classes, vectors and the consumer's truncation detection) |
| 24 | AddPartitionsToTxn | v0 … v5 | yes up to v3, **broker→broker from v4** | v0 … v2, **v3** | v0 … v2, **v3** (the frame `Client::addPartitionsToTxn()` sends), **v4** and **v5** (classes and vectors only — Kafka 3.5/3.8, KIP-890 made it a broker api: the node authorizes every version from 4 on as `CLUSTER_ACTION` and answers a client the top-level 31 at both, and the 120 of a `verify_only` at both; `AddPartitionsToTxnRequestV4`/`ResponseV4` keep the version below the v5) |
| 25 | AddOffsetsToTxn | v0 … v4 | yes | v0 … v2, **v3** | v0 … v3, **v4** (the code 120 of KIP-890, Kafka 3.8, which this api never carries: it adds a partition instead of writing into one, and an unknown producer id is the 49; `AddOffsetsToTxnRequestV3`/`ResponseV3` keep the version below) |
| 26 | EndTxn | v0 … v4 | yes | v0 … v2, **v3** | v0 … v3, **v4** (the code 120 of KIP-890, Kafka 3.8; an abort of a committed transaction is still the 48, a fenced producer the 90; `EndTxnRequestV3`/`ResponseV3` keep the version below); **v5 (4.0) is beyond this line** |
| 27 | WriteTxnMarkers | v0, v1 | broker→broker | v0, **v1** (classes and vectors; a broker→broker api, probed only) | v0, **v1** (classes and vectors; a broker→broker api, probed only) |
| 28 | TxnOffsetCommit | v0 … v4 | yes | v0 … v2, **v3** | v0 … v3, **v4** — the one client api of the five that really gets the **120** on this node (KIP-890, Kafka 3.8): a commit whose `__consumer_offsets` partition the transaction does not hold is answered 120 where the v3 is answered 48; `TxnOffsetCommitRequestV3`/`ResponseV3` keep the version below |
| 29 | DescribeAcls | v0 … v3 | yes | no, see below | **v3** (Kafka 3.3, `AdminClient::describeAcls()`) |
| 30 | CreateAcls | v0 … v3 | yes | no, see below | **v3** (Kafka 3.3, `AdminClient::createAcls()`) |
| 31 | DeleteAcls | v0 … v3 | yes | no, see below | **v3** (Kafka 3.3, `AdminClient::deleteAcls()`) |
| 32 | DescribeConfigs | v0 … v4 | yes | v0 … v3, **v4** | v0 … v3, **v4** |
| 33 | AlterConfigs | v0, v1, v2 | yes | v0, v1, **v2** | v0, v1, **v2** |
| 34 | AlterReplicaLogDirs | v0, v1, v2 | yes | v0, v1, **v2** | v0, v1, **v2** |
| 35 | DescribeLogDirs | v0 … v4 | yes | v0, v1, **v2** | v0 … v3, **v4** (the top-level error code of Kafka 3.2, the volume sizes of KIP-827, Kafka 3.3) |
| 36 | SaslAuthenticate | v0, v1, v2 | yes | v0, v1, **v2** | v0, v1, **v2** |
| 37 | CreatePartitions | v0 … v3 | controller | v0 … v2, **v3** | v0 … v2, **v3** |
| 38 | CreateDelegationToken | v0 … v3 | yes | v0, v1, **v2** | v0, v1, v2, **v3** (a token for another principal, KIP-373, Kafka 3.3) |
| 39 | RenewDelegationToken | v0, v1, v2 | yes | v0, v1, **v2** | v0, v1, **v2** |
| 40 | ExpireDelegationToken | v0, v1, v2 | yes | v0, v1, **v2** | v0, v1, **v2** |
| 41 | DescribeDelegationToken | v0 … v3 | yes | v0, v1, **v2** | v0, v1, v2, **v3** (the token requester, KIP-373, Kafka 3.3) |
| 42 | DeleteGroups | v0, v1, v2 | yes | v0, v1, **v2** | v0, v1, **v2** |
| 43 | ElectLeaders | v0, v1, v2 | controller | v0, v1, **v2** | v0, v1, **v2** |
| 44 | IncrementalAlterConfigs | v0, v1 | yes | v0 (Kafka 2.3), **v1** | v0 (Kafka 2.3), **v1** |
| 45 | AlterPartitionReassignments | v0 | controller | **v0** (Kafka 2.4) | **v0** (Kafka 2.4) |
| 46 | ListPartitionReassignments | v0 | controller | **v0** (Kafka 2.4) | **v0** (Kafka 2.4) |
| 47 | OffsetDelete | v0 | yes | **v0** (Kafka 2.4) | **v0** (Kafka 2.4) |
| 48 | DescribeClientQuotas | v0, v1 | yes | v0, **v1** | v0, **v1** |
| 49 | AlterClientQuotas | v0, v1 | yes | v0, **v1** | v0, **v1** |
| 50 | DescribeUserScramCredentials | v0 | yes | **v0** (Kafka 2.7) | **v0** (Kafka 2.7) |
| 51 | AlterUserScramCredentials | v0 | yes | **v0** (Kafka 2.7) | **v0** (Kafka 2.7) |
| 55 | DescribeQuorum | v0, v1, v2 | broker (a raft api the client listener serves) | – | **v0, v1, v2** (`AdminClient::describeMetadataQuorum()`, Kafka 2.7 / 3.3 / 3.9; the nodes, the directory ids and the two error messages of KIP-853, `Admin\QuorumNode` and `Admin\RaftVoterEndpoint`; `DescribeQuorumRequestV1`/`ResponseV1` and the `V0` classes keep the versions below) |
| 56 | AlterIsr | not on the client listener of a KRaft node | broker→controller | no — broker→controller, probed only | no — broker→controller, probed only |
| 57 | UpdateFeatures | v0, v1 | controller | **v0** (Kafka 2.7) | **v0, v1** (Kafka 2.7 / 3.3, the `upgrade_type` and the `validate_only` of KIP-778) |
| 60 | DescribeCluster | v0, v1 | yes | **v0** (Kafka 2.8) | **v0, v1** (Kafka 2.8 / 3.7, the `endpoint_type` of KIP-919 and the codes 114 and 115; `DescribeClusterRequestV0` keeps the frame below) |
| 61 | DescribeProducers | v0 | yes | **v0** (Kafka 2.8) | **v0** (Kafka 2.8) |
| 64 | UnregisterBroker | v0 | controller | – | no — a controller api, probed only (Kafka 2.8) |
| 65 | DescribeTransactions | v0 | yes | – | **v0** (`AdminClient::describeTransactions()`, Kafka 3.0) |
| 66 | ListTransactions | v0, v1 | yes | – | **v0, v1** (`AdminClient::listTransactions()`, Kafka 3.0; the duration filter of KIP-994, Kafka 3.8; `ListTransactionsRequestV0` keeps the version below) |
| 68 | ConsumerGroupHeartbeat | v0 | yes | – | **v0** (Kafka 3.5, KIP-848) — the whole membership protocol of the new consumer in one api, in place of JoinGroup, SyncGroup, Heartbeat and LeaveGroup: a consumer with `group.protocol=consumer` sends it from `Consumer\Internals\ConsumerGroupHeartbeatCoordinator` through `Client::joinConsumerGroup()`, `::consumerGroupHeartbeat()` and `::leaveConsumerGroup()`, with `ConsumerGroupHeartbeatRequest::forJoin()`/`forHeartbeat()`/`forLeave()` for the delta encoding of the frame |
| 69 | ConsumerGroupDescribe | v0 | yes | – | **v0** (Kafka 3.7, KIP-848) — the DescribeGroups of the new protocol (`AdminClient::describeConsumerGroups()`, `::describeConsumerGroup()`): the group and assignment epochs, the server-side assignor and both assignments of every member. Key 15 answers a group of this type the state `Dead`, so an admin client routes by the `group_type` of a ListGroups v5 |
| 71 | GetTelemetrySubscriptions | v0 | yes (hidden without a telemetry plugin) | – | **v0, wire only** (Kafka 3.7, KIP-714) — classes and vectors, no client method |
| 72 | PushTelemetry | v0 | yes (hidden without a telemetry plugin) | – | **v0, wire only** (Kafka 3.7, KIP-714) |
| 74 | ListClientMetricsResources | v0 | yes | – | **v0, wire only** (Kafka 3.7, KIP-714) — classes and vectors, no client method |
| 75 | DescribeTopicPartitions | v0 | yes | – | **v0** (`AdminClient::describeTopicPartitions()`, the paging api of KIP-966, Kafka 3.8) |
| 80 | AddRaftVoter | v0 | yes (a raft api the client listener serves, forwarded to the controller) | – | no — probed only (Kafka 3.9, KIP-853): a foreign cluster id is the 104, an unreadable voter key the 42, and a well-formed one the **35**, because the node has finalized `kraft.version` at 0 |
| 81 | RemoveRaftVoter | v0 | yes (a raft api the client listener serves, forwarded to the controller) | – | no — probed only (Kafka 3.9, KIP-853), refused exactly like the key 80 |
| 82 | UpdateRaftVoter | v0 | controller | – | no — a controller api, probed only (Kafka 3.9); every version of it closes a client connection |
| 76–79, 83–87 | The share-group apis (KIP-932) | v0 | no — unstable, hidden without `unstable.api.versions.enable` | – | no — **out by decision** (Kafka 3.9): every frame of them closes the connection, and their error codes 121 to 124 stay declared |

The lower versions of every api are kept because their frames are what the wire vectors of the
lines below replay.

**The three ACL apis (29, 30, 31) are implemented on this line, at the version 3 of Kafka 3.3.** The lines below left them out because they do nothing on a broker without an `authorizer.class.name` (such a broker answers all three with the error code 54, `SecurityDisabled`) and every wire vector of this repository is captured from a real broker. The 3.9.2 node of this line runs the `StandardAuthorizer` of KRaft with `super.users=User:ANONYMOUS;User:admin;User:kafkatest`, so `AdminClient::describeAcls()`, `createAcls()` and `deleteAcls()` were measured against a real authorizer: an acl is a `Common\AclBinding` — a `ResourcePattern` (`LITERAL` or `PREFIXED`, with the `USER` resource type that Kafka 3.3 added for KIP-373) and an `AccessControlEntry` (a principal, a host, an `AclOperation` and `ALLOW`/`DENY`) — and a describe or a delete names an `AclBindingFilter`, in which every field may be a wildcard and the pattern type `MATCH` asks which acls apply to a resource. The client sends the version 3 and keeps no lower one: an api that starts on this line gets the versions the node was measured at.

**The four delegation-token apis (38 to 41) are implemented, and a token cannot be used to
authenticate.** `AdminClient::createDelegationToken()`, `renewDelegationToken()`,
`expireDelegationToken()` and `describeDelegationToken()` speak them over an authenticated channel —
one of the SASL listeners, because KIP-48 derives the owner of a token from the principal of the
connection and answers the error code 64 on a PLAINTEXT or one-way-SSL one. What is missing is the
other half of KIP-48: *using* a token means a SASL/SCRAM login whose user name is the token id and
whose password is the base64 HMAC, and this client speaks SASL/PLAIN only. The four apis are
verified against the real broker, the login with their result is not implemented.
[examples/delegation-tokens.php](examples/delegation-tokens.php) runs one token's whole life against
the SASL listener of `docker-compose.yml`.

What the lines can do beyond the api versions themselves (the `main` column is the state of the
current milestone):

| Feature                                                | Arrived in | `0.8.x` | `0.9.x` | `0.10.x` | `0.11.x` | `1.x` | `2.x` | `main` |
|--------------------------------------------------------|------------|---------|---------|----------|----------|-------|-------|--------|
| Message format v0 (no timestamps)                      | 0.8        | yes     | yes     | yes      | yes      | yes   | yes    | yes    |
| Message format v1 (timestamps, relative inner offsets) | 0.10.0     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| Record batch v2 (headers, varints, CRC-32C)            | 0.11       | –       | –       | –        | yes      | yes   | yes    | yes    |
| Compression `gzip`, `snappy`                           | 0.8        | yes     | yes     | yes      | yes      | yes   | yes    | yes    |
| Compression `lz4`                                      | 0.10.0     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| Transport `PLAINTEXT`                                  | 0.8        | yes     | yes     | yes      | yes      | yes   | yes    | yes    |
| Transport `SSL`                                        | 0.9        | –       | yes     | yes      | yes      | yes   | yes    | yes    |
| Transport `SASL_PLAINTEXT` / `SASL_SSL` (PLAIN)        | 0.10.0     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| Consumer groups (`subscribe()`, assignors)             | 0.9        | –       | yes     | yes      | yes      | yes   | yes    | yes    |
| The group state `Empty`                                | 0.10.1     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| The group state `CompletingRebalance` (`AwaitingSync` below) | 1.0  | –       | –       | –        | –        | yes   | yes    | yes    |
| Client quotas and their `throttle_time_ms`             | 0.9        | –       | yes     | yes      | yes      | yes   | yes    | yes    |
| `throttle_time_ms` in the group and admin apis         | 0.11       | –       | –       | –        | yes      | yes   | yes    | yes    |
| `controller_id`, broker `rack`, `is_internal`          | 0.10.0     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| `cluster_id` of Metadata v2                            | 0.10.1     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| Offsets by timestamp, `offsetsForTimes()`              | 0.10.1     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| `fetch.max.bytes` of Fetch v3                          | 0.10.1     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| `max.poll.interval.ms` and the `rebalance_timeout`     | 0.10.1     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| Admin: create and delete topics through the protocol   | 0.10.1     | –       | –       | yes      | yes      | yes   | yes    | yes    |
| Admin: `getApiVersions()`                              | 0.10.0     | –       | –       | yes      | yes, v1  | yes, v1 | **yes, v2** | **yes, v4** |
| Admin: `DeleteRecords`, `DescribeConfigs`/`AlterConfigs` | 0.11     | –       | –       | –        | yes      | yes   | yes    | yes    |
| Record headers end to end (KIP-82)                     | 0.11       | –       | –       | –        | yes      | yes   | yes    | yes    |
| `OffsetForLeaderEpoch`, `allow_auto_topic_creation`    | 0.11       | –       | –       | –        | yes      | yes   | yes    | yes    |
| Idempotent producer (`enable.idempotence`)             | 0.11       | –       | –       | –        | yes      | yes   | yes    | yes    |
| Transactional producer, `isolation.level`              | 0.11       | –       | –       | –        | yes      | yes   | yes    | yes    |
| Framed SASL exchange (`SaslAuthenticate`, KIP-152)     | 1.0        | –       | –       | –        | –        | yes   | yes    | yes    |
| `log_start_offset` of a produce answer, `offline_replicas` | 1.0    | –       | –       | –        | –        | yes   | yes    | yes    |
| The five-batch duplicate window of a producer id        | 1.0        | –       | –       | –        | –        | yes   | yes    | yes    |
| `UnknownProducerId` (59) repaired from the `log_start_offset` | 1.0  | –   | –       | –        | –        | yes   | yes — a 2.8.2 broker no longer sends 59 on the produce path | yes — a 2.8.2 broker no longer sends 59 on the produce path |
| Incremental fetch sessions (KIP-227)                   | 1.1        | –       | –       | –        | –        | yes   | yes, one session per broker in the consumer | yes, one session per broker in the consumer |
| Dynamic broker configuration, config sources and synonyms (KIP-226) | 1.1 | –  | –       | –        | –        | yes   | yes    | yes    |
| Admin: `createPartitions()`, `deleteConsumerGroups()`  | 1.0 / 1.1  | –       | –       | –        | –        | yes   | yes    | yes    |
| Admin: `describeLogDirs()`, `alterReplicaLogDirs()`    | 1.0        | –       | –       | –        | –        | yes   | yes    | yes    |
| Delegation tokens (KIP-48)                             | 1.1        | –       | –       | –        | –        | issued, renewed, expired, described | issued, renewed, expired, described | issued, renewed, expired, described |
| **KIP-219: the client waits out `throttle_time_ms`** (`throttle.wait`) | 2.0 | –  | –       | –        | –        | –     | **yes** | **yes** |
| **KIP-279: the `leader_epoch` of an OffsetForLeaderEpoch answer** | 2.0 | – | –       | –        | –        | –     | **yes** | **yes** |
| **KIP-283: `message.downconversion.enable`, measured** | 2.0        | –       | –       | –        | –        | –     | **yes** (35 per partition) | **yes** (35 per partition) |
| **KIP-320: leader epochs in Fetch, ListOffsets, Metadata, OffsetCommit/OffsetFetch, OffsetForLeaderEpoch; truncation detection in the consumer** | 2.1 | – | – | – | – | – | **yes** (`LogTruncationException` with `auto.offset.reset=none`) | **yes** (`LogTruncationException` with `auto.offset.reset=none`) |
| **KIP-110: the zstd codec** (`compression.type=zstd`)   | 2.1        | –       | –       | –        | –        | –     | **yes, through `ext-zstd`** (76 without it) | **yes, through `ext-zstd`** (76 without it) |
| **KIP-211: OffsetCommit v5 without a per-commit retention** | 2.1     | –       | –       | –        | –        | –     | **yes** | **yes** |
| **KIP-394: the second join** (79 on a first JoinGroup v4 without a member id) | 2.2 | – | – | – | – | – | **yes**, the consumer rejoins by itself | **yes**, the consumer rejoins by itself |
| **KIP-207: 78 `OffsetNotAvailable`** of ListOffsets v5   | 2.2        | –       | –       | –        | –        | –     | **yes** (documented from the sources: one broker never lags) | **yes** (documented from the sources: one broker never lags) |
| **KIP-368: the SASL session lifetime** of SaslAuthenticate v1 | 2.2   | –       | –       | –        | –        | –     | **reported** (re-authentication is 2.5's) | **reported** (re-authentication is 2.5's) |
| **KIP-183: `electLeaders()`** (ElectLeaders v0)         | 2.2        | –       | –       | –        | –        | –     | **yes** (preferred elections; unclean from v1) | **yes** (preferred elections; unclean from v1) |
| **KIP-380: the broker epoch** of ControlledShutdown v2  | 2.2        | –       | –       | –        | –        | –     | **yes** (`controlledShutdown()`) | **wire only** — the classes and the vectors stay, `controlledShutdown()` is gone: a KRaft node serves the api on its controller listener only |
| **KIP-345: static membership** (`group.instance.id`, 82 fences the older instance) | 2.3 | – | – | – | – | – | **yes** — a static consumer keeps its partitions across a restart and does not leave on `close()` | **yes** — a static consumer keeps its partitions across a restart and does not leave on `close()` |
| **KIP-430: authorized operations** of Metadata v8 and DescribeGroups v3 (`Common\AclOperation`) | 2.3 | – | – | – | – | – | **yes** (the supported operations on a broker without an authorizer) | **yes** (the supported operations on a broker without an authorizer) |
| **KIP-392: reading from a follower** (`client.rack`, `preferred_read_replica` of Fetch v11) | 2.3 | – | – | – | – | – | **wire only** — one broker never names another replica | **wire only** — one broker never names another replica |
| **KIP-339: `incrementalAlterConfigs()`** (IncrementalAlterConfigs v0) | 2.3 | – | – | – | – | – | **yes** (SET, DELETE, APPEND, SUBTRACT) | **yes** (SET, DELETE, APPEND, SUBTRACT) |
| **KIP-482: flexible versions and tagged fields** (compact strings, bytes and arrays, request header v2, response header v1) | 2.4 | – | – | – | – | – | **yes** — the 2.4 versions were the first (ApiVersions v3, Metadata v9, the ten group apis, CreateTopics v5, DeleteTopics v4, ElectLeaders v2, IncrementalAlterConfigs v1, ControlledShutdown v3, InitProducerId v2, CreateDelegationToken v2), and every flexible version Kafka 2.5 to 2.8 added is sent that way too — DeleteRecords v2 (2.6), Fetch v12 and the four transaction apis (2.7), Produce v9, ListOffsets v6, OffsetForLeaderEpoch v4 and Metadata v10/v11 (2.8) — so that **SaslHandshake v1 and OffsetDelete v0 are the only requests this client still sends in a plain frame** | **yes** — the 2.4 versions were the first (ApiVersions v3, Metadata v9, the ten group apis, CreateTopics v5, DeleteTopics v4, ElectLeaders v2, IncrementalAlterConfigs v1, ControlledShutdown v3, InitProducerId v2, CreateDelegationToken v2), and every flexible version Kafka 2.5 to 2.8 added is sent that way too — DeleteRecords v2 (2.6), Fetch v12 and the four transaction apis (2.7), Produce v9, ListOffsets v6, OffsetForLeaderEpoch v4 and Metadata v10/v11 (2.8) — so that **SaslHandshake v1 and OffsetDelete v0 are the only requests this client still sends in a plain frame** |
| **KIP-455: partition reassignments** (`alterPartitionReassignments()`, `listPartitionReassignments()`) | 2.4 | – | – | – | – | – | **yes** | **yes** |
| **KIP-496: `deleteConsumerGroupOffsets()`** (OffsetDelete v0) | 2.4 | – | – | – | – | – | **yes** | **yes** |
| **KIP-345: static members removed by hand** (`removeMembersFromConsumerGroup()`, LeaveGroup v3) and the `group_instance_id` of DescribeGroups v4 | 2.4 | – | – | – | – | – | **yes** | **yes** |
| **KIP-464 / KIP-525: topics created with the broker defaults** (`NewTopic::withBrokerDefaults()`) and answered with their configuration (`createTopicsWithResults()`) | 2.4 | – | – | – | – | – | **yes** | **yes** |
| **KIP-460: unclean leader election** (`ElectionType::UNCLEAN`, ElectLeaders v1) | 2.4 | – | – | – | – | – | **wire only** — a one-broker cluster has no partition whose leader is gone | **wire only** — a one-broker cluster has no partition whose leader is gone |
| **KIP-467: the record errors of a refused batch** (Produce v8, `InvalidRecordException` names the records) | 2.4 | – | – | – | – | – | **yes** | **yes** |
| **KIP-360: the epoch bump of a transactional producer** (InitProducerId v3 with the producer's own id and epoch; an abortable error no longer ends the producer) | 2.5 | – | – | – | – | – | **yes** | **yes** |
| **KIP-447: exactly-once with a consumer group** (`sendOffsetsToTransaction()` with `ConsumerGroupMetadata`, TxnOffsetCommit v3; `require_stable` of OffsetFetch v7 and the 88, read by a `read_committed` consumer) | 2.5 | – | – | – | – | – | **yes** | **yes** |
| **KIP-559: the protocol type and name of a generation** (JoinGroup v7, SyncGroup v5) | 2.5 | – | – | – | – | – | **yes** | **yes** |
| **KIP-546: client quotas over the wire** (`describeClientQuotas()`, `alterClientQuotas()`) | 2.6 | – | – | – | – | – | **yes** | **yes** |
| **KIP-518: the states of `listConsumerGroups()`** (ListGroups v4) | 2.6 | – | – | – | – | – | **yes** | **yes** |
| **KIP-569: the type and documentation of a configuration entry** (DescribeConfigs v3, `ConfigType`) | 2.6 | – | – | – | – | – | **yes** | **yes** |
| **KIP-599: throttled topic creation** (the 89 `THROTTLING_QUOTA_EXCEEDED` of CreateTopics v6, DeleteTopics v5 and CreatePartitions v3, retried after the throttle) | 2.7 | – | – | – | – | – | **yes** | **yes** |
| **KIP-588: a fenced producer is 90** (InitProducerId v4, `TransactionalProducerFencedException`) | 2.7 | – | – | – | – | – | **yes** | **yes** |
| **KIP-595: epoch validation in the fetch itself** (Fetch v12, `last_fetched_epoch` and the `diverging_epoch` of the answer) | 2.7 | – | – | – | – | – | **yes** | **yes** |
| **KIP-554: SCRAM credentials over the wire** (`describeUserScramCredentials()`, `alterUserScramCredentials()`) | 2.7 | – | – | – | – | – | **yes** — the credentials can be managed, the SCRAM login itself is still not spoken | **yes** — the credentials can be managed, the SCRAM login itself is still not spoken |
| **KIP-584: feature versions** (`describeFeatures()`, `updateFeatures()`) | 2.7 | – | – | – | – | – | **yes** | **yes** |
| **KIP-516: topic ids** (`Common\Uuid`, Metadata v10 and v11, `TopicMetadata::$topicId`, CreateTopics v7 and DeleteTopics v6 with `CreatedTopic::$topicId` and the 100 `UnknownTopicId`) | 2.8 | – | – | – | – | – | **yes** — a deleted and re-created topic of the same name gets a new id | **yes** — a deleted and re-created topic of the same name gets a new id |
| **KIP-482 on the last plain apis** (the flexible v3 of AddPartitionsToTxn, AddOffsetsToTxn and EndTxn, DescribeConfigs v4, AlterConfigs v2, AlterReplicaLogDirs v2, WriteTxnMarkers v1) | 2.8 | – | – | – | – | – | **yes** | **yes** |
| **KIP-700: the cluster-wide authorized operations leave Metadata** (gone from the request and the answer of Metadata v11, asked with `describeCluster()`) and **KIP-664: `describeProducers()`** | 2.8 | – | – | – | – | – | **yes** | **yes** |
| **KIP-664: `describeTransactions()` / `listTransactions()`** (DescribeTransactions v0, ListTransactions v0) | 3.0 | – | – | – | – | – | – | **yes** |
| **KIP-734: the max timestamp** (`OffsetsRequest::MAX_TIMESTAMP`, ListOffsets v7) | 3.0 | – | – | – | – | – | – | **yes** (`maxTimestampOffsets()`, `listMaxTimestampOffsets()`) |
| **KIP-699: several coordinators in one FindCoordinator** (v4) and **several groups in one OffsetFetch** (v8) | 3.0 | – | – | – | – | – | – | **yes** (`getGroupCoordinators()`, `listConsumerGroupOffsets()`) |
| **KIP-516, the request side: topics named by their id** (Fetch v13, Metadata v12; `Cluster::topicIdOf()`/`topicNameById()`, `describeTopicsByIds()`; the 106 of a fetch session that mixes ids and names) | 3.1 | – | – | – | – | – | – | **yes** |
| **KIP-800: the `reason` of a join and of a leave** (JoinGroup v8, LeaveGroup v5; `Client::joinGroup()`/`leaveGroup()`, `KafkaConsumer::unsubscribe()`, `removeMembersFromConsumerGroup()`) and **KIP-814: `skip_assignment`** (JoinGroup v9; a static leader that returns to a `Stable` group keeps its assignment) | 3.2 | – | – | – | – | – | – | **yes** |
| **DescribeLogDirs v3: the top-level error code** of a refused request (thrown by `describeLogDirs()`) | 3.2 | – | – | – | – | – | – | **yes** |
| **The ACL apis at v3** (DescribeAcls, CreateAcls, DeleteAcls; `describeAcls()`, `createAcls()`, `deleteAcls()`, `Common\AclBinding`) | 3.3 | – | – | – | – | – | – | **yes** — the first line of this package to speak them, against a real authorizer |
| **KIP-778: the upgrade type and the dry run of a feature update** (`Admin\UpgradeType`, `updateFeatures(..., validateOnly: true)`, UpdateFeatures v1) | 3.3 | – | – | – | – | – | – | **yes** — the safe and the unsafe downgrade are two frames, and a dry run writes nothing |
| **KIP-836: the lag of a voter** (`describeMetadataQuorum()`, the `LastFetchTimestamp` and `LastCaughtUpTimestamp` of DescribeQuorum v1) | 3.3 | – | – | – | – | – | – | **yes** — the one-node quorum reports the leader's own current time in both |
| **KIP-827: the volume sizes of a log directory** (DescribeLogDirs v4, `LogDirInfo::$totalBytes`/`$usableBytes`) and **KIP-373: a token for another principal** (CreateDelegationToken v3, DescribeDelegationToken v3, `createDelegationToken(..., $owner)`, `TokenInformation::$tokenRequester`) | 3.3 | – | – | – | – | – | – | **yes** |
| **KIP-405, the client side of tiered storage** (Fetch v14 and the error code 109, ListOffsets v8 and the target time `-4`; `OffsetsRequest::EARLIEST_LOCAL_TIMESTAMP`, `listEarliestLocalOffsets()`) | 3.5 | – | – | – | – | – | – | **yes** — the wire; the node has no remote storage, so `-4` is the earliest offset and the 109 stays declared |
| **KIP-903: the replica state of a follower fetch** (Fetch v15, the tagged `replica_state` in the place of the top-level `replica_id`; `Data\FetchRequestReplicaState`, `FetchRequest::$replicaEpoch`) | 3.5 | – | – | – | – | – | – | **yes** — a consumer writes nothing and its frame is four bytes shorter; a one-node cluster answers a follower 75 or 6 before the epoch is looked at |
| **KIP-890, part 1: AddPartitionsToTxn v4, the batched broker version** (`AddPartitionsToTxnRequest::forTransactions()`, `Data\AddPartitionsToTxnTransaction`, `Data\AddPartitionsToTxnResult`) | 3.5 | – | – | – | – | – | – | **wire only** — the node answers a client the 31 of `CLUSTER_ACTION`; `Client::addPartitionsToTxn()` keeps the v3 until the v5 of Kafka 3.8 |
| **KIP-848, the first version a classic client sends: OffsetCommit v9** (the v8 frame; the 69 `GroupIdNotFound` of an unknown group and the 113 `StaleMemberEpoch` of a member epoch; `OffsetCommitRequestV8`/`ResponseV8` keep the v8) | 3.6 | – | – | – | – | – | – | **yes** — the 113 observed with a hand-built KIP-848 member; the consumer protocol itself is the last wave of the line |
| **KIP-951: leader discovery** (Produce v10, Fetch v16; the tagged `current_leader` of a refused partition and the `node_endpoints` of the answer, `ProduceResponsePartition::$currentLeader`, `ProduceResponse::$nodeEndpoints`, `FetchResponse::$nodeEndpoints`, handed to the caller in the exception context as `currentLeaderId`/`Epoch`/`Host`/`Port`) | 3.7 | – | – | – | – | – | – | **yes** — the wire and the hint; the client still refreshes its metadata instead of following the endpoint |
| **KIP-919: the endpoint type of a DescribeCluster** (v1, `Admin\EndpointType`, `describeCluster(..., EndpointType::Controller)`, the codes 114 and 115) | 3.7 | – | – | – | – | – | – | **yes** — a broker listener answers the 114 for the controllers, the controller listener of the node is not exposed |
| **KIP-848, the fetch half: the member id and epoch of an OffsetFetch** (v9; `Client::fetchGroupOffsetsAsMember()`, `OffsetFetchRequest::forMember()`, `Data\OffsetFetchRequestGroup::$memberId`/`$memberEpoch`; the 113 and the 25 as group-level codes) | 3.7 | – | – | – | – | – | – | **yes** — measured with a hand-built KIP-848 member; the consumer protocol itself is the last wave of the line |
| **KIP-714: client metrics** (GetTelemetrySubscriptions 71, PushTelemetry 72, ListClientMetricsResources 74) | 3.7 | – | – | – | – | – | – | **wire only** — classes and vectors of a node without a receiver plugin, no client method, no emitter (owner decision) |
| **KIP-966: the eligible leader replicas over the wire** (DescribeTopicPartitions, key 75; `describeTopicPartitions()`, `Admin\TopicDescription`, `Admin\TopicPartitionInfo`, `Protocol\NullableStruct`) and **KIP-994: the duration filter of `listTransactions()`** (ListTransactions v1) | 3.8 | – | – | – | – | – | – | **yes** — the api pages, and the node answers the two ELR arrays empty |
| **KIP-890 (part 1): the abortable transaction error** (Produce v11; the **120** `TransactionAbortable` a transactional batch of an unverified partition is refused with, `TransactionAbortableException`, which makes the transaction abortable instead of fatal) | 3.8 | – | – | – | – | – | – | **yes** — the wire and the producer state machine; the transaction protocol v2 of part 2 is not in 3.9 (the node finalizes no `transaction.version`) |
| **KIP-848 (the group types of a listing)** (ListGroups v5, `group_type` + `types_filter`) and **KIP-890 (the code 120 reaches FindCoordinator)** (v5, no field) | 3.8 | – | – | – | – | – | – | **yes** (`listGroups($node, $states, $types)`, `ListGroupResponseProtocol::TYPE_*`); the 120 is produced at AddPartitionsToTxn and Produce, never at FindCoordinator |
| **KIP-848, the consumer protocol itself** (ConsumerGroupHeartbeat 68, ConsumerGroupDescribe 69, `group.protocol=consumer`, `group.remote.assignor`) | 3.5 / 3.7 | – | – | – | – | – | – | **yes** — one api in place of four, the assignment computed by the coordinator, the heartbeat interval dictated by the broker, an **incremental** rebalance, the member epoch on OffsetCommit v9 / OffsetFetch v9, the static leave of the epoch **-2**, and the codes 110, 111, 112 and the 69 of a classic group observed on the node |
| **KIP-890, part 2 (the wire half): the abortable transaction error** (InitProducerId v5, AddPartitionsToTxn v5, AddOffsetsToTxn v4, EndTxn v4, TxnOffsetCommit v4, and the error code 120 `TransactionAbortableException`) | 3.8 | – | – | – | – | – | – | **yes** — the four client-facing versions are sent; AddPartitionsToTxn v5 stays a broker version. The node finalizes no `transaction.version`, so only the partition verification of part 1 produces the 120: a TxnOffsetCommit v4 whose offsets partition the transaction does not hold, where the v3 is answered 48 |
| **KAFKA-17011: a supported feature with the minimum version 0** (ApiVersions v4, no field; `kraft.version` 0…1 appears in the answer of a v4 and in no answer below it) and **KIP-853: the reconfigurable quorum over the wire** (DescribeQuorum v2, the `Nodes` array, the `ReplicaDirectoryId` of a replica state and the two `ErrorMessage` fields; `Admin\QuorumNode`, `Admin\RaftVoterEndpoint`, `ReplicaState::$replicaDirectoryId`, and the `uint16` the engine gained for a listener port) | 3.9 | – | – | – | – | – | – | **yes** — the wire of both; the node runs a **static** voter set (`kraft.version` finalized at 0), so every directory id it reports is the zero uuid and the reconfiguration apis 80 and 81 refuse every frame with the 35 |
| **KIP-853: the directory id of a follower fetch** (Fetch v17, the tagged `replica_directory_id` of every partition entry; `Data\FetchRequestTopicPartition::$replicaDirectoryId`, `FetchRequest::getReplicaDirectoryId()`) | 3.9 | – | – | – | – | – | – | **yes** — the wire; a consumer writes nothing and its frame does not change, and a fetch of an ordinary topic never reads the field: only `KafkaRaftClient` does, for `__cluster_metadata` on the controller listener |
| **KIP-1005: the last tiered offset** (ListOffsets v9, the target time `-5`; `OffsetsRequest::LATEST_TIERED_TIMESTAMP`, `AdminClient::listLatestTieredOffsets()`) | 3.9 | – | – | – | – | – | – | **yes** — the wire; without remote storage the node answers the offset `-1` with the error code 0, on a filled log as on an empty one |
| **KIP-932 (the coordinator type of a share group)** (FindCoordinator v6, `COORDINATOR_TYPE_SHARE`) | 3.9 | – | – | – | – | – | – | **wire only** — the version and the constant; the type 2 is legal from v6 and answered the **15** `CoordinatorNotAvailable` by a 3.9.2 node, which has no share coordinator, and the **42** below it. Share groups themselves (76–79, 83–87) are out of this line |
| Error codes                                            | –          | -1 … 20 | -1 … 31 | -1 … 44  | -1 … 55  | -1 … 71 | **-1 … 104** (the constants of 2.8.2; 72 is 2.0's) | **-1 … 127** (the constants of 3.9.2, declared by the foundation; 105 is 3.0's) |

What this line leaves out **by design** (the owner's decisions for the 3.x line; everything else the 3.9.2
node serves is "not yet" until its milestone lands):

| Feature                                          | Arrived in | On this branch                        |
|--------------------------------------------------|------------|---------------------------------------|
| SASL/SCRAM, SASL/GSSAPI and SASL/OAUTHBEARER    | 0.10.2 / 0.9 / 2.0 | no — PLAIN only, which is why a delegation token can be issued but not used |
| ACL apis `DescribeAcls`/`CreateAcls`/`DeleteAcls` | 0.11       | **yes** — at the version 3 of Kafka 3.3, against the `StandardAuthorizer` of the 3.9.2 KRaft node (`AdminClient::describeAcls()`, `createAcls()`, `deleteAcls()`) |
| Replication apis `LeaderAndIsr`/`StopReplica`/`UpdateMetadata`/`AlterIsr` | 0.8 / 2.7 | no — only a ZooKeeper controller sends them, and a KRaft node does not even list them on its client listeners |
| The KRaft controller apis (52–54, 58, 59, 62–64, 67, 70, 73, 80–82) | 2.7 … 3.9 | no — probed only; of the ones a KRaft node lists on its client listeners, **64** answers the 102 of an unknown broker id and **80/81** the 35 of a quorum whose `kraft.version` is 0, and the rest live on the controller listener. **DescribeQuorum (55) is the exception and is implemented**, at v0 to v2 |
| Share groups (76–79 and their state apis 83–87, KIP-932) | 3.9 | no — early access in 3.9, hidden without `unstable.api.versions.enable`: a frame of any of the nine **closes the connection**, and the error codes 121 to 124 are declared and unreachable; the 4.x line implements them |
| Client metrics (71, 72, 74, KIP-714) | 3.7 | **the wire classes are implemented** — `GetTelemetrySubscriptionsRequest`/`Response`, `PushTelemetryRequest`/`Response`, `ListClientMetricsResourcesRequest`/`Response` and `Data\ClientMetricsResource`, with 26 vectors of what a node **without** a telemetry receiver plugin answers (the generated client instance id, the 300000 ms default interval, the 89 of asking twice, the 117 of a foreign subscription id, the 118 of an oversized blob); there is no client method and no telemetry emitter, so nothing of this package ever sends them |
| Tiered storage (KIP-405, KIP-1005) | 3.5, 3.9 | **the wire halves are implemented** — Fetch **v14** (the error code 109 `OffsetMovedToTieredStorage`, `Errors\OffsetMovedToTieredStorageException`), ListOffsets **v8** (the target time `-4`, `AdminClient::listEarliestLocalOffsets()`) and ListOffsets **v9** (the target time `-5`, `AdminClient::listLatestTieredOffsets()`); the container has no remote storage, so the 109 is declared, `-4` equals `-2` and `-5` answers the offset -1 with the error code 0 on it; the remote-storage RPCs of KIP-405 are broker-internal and have no client-facing api key, so nothing of the feature is left out |
| `offsets.storage = zookeeper` (OffsetCommit/OffsetFetch v0) | 0.8.1 | removed from this line — the option and its code path are gone; the classes stay for the wire vectors of the lines below, and a KRaft node answers both v0 requests with 35 `UnsupportedVersion` |
| `controlledShutdown()` (ControlledShutdown, key 7) | 0.8 | removed from this line — the method is gone from the admin client; the classes and the vectors stay, and a KRaft node serves the api on its controller listener only |

Five properties of a 3.9.2 node (and of every broker since 1.0) regularly surprise clients, and this
implementation deals with all of them explicitly:

* **An api the broker does not serve costs the connection.** A request whose api key or version
  a 3.9.2 node cannot parse — and a body that does not match the schema of a version it
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
only api that says whether a partition exists. And a ControlledShutdown for a broker id the
controller does not know was answered with **8 (BrokerNotAvailable)** from 0.9 on, the code a 0.8.2.2
broker turned into -1 (Unknown) by mapping the *cause* of an exception that has none — a measurement
of the lines below, since a KRaft node does not serve the api on a client listener. The protocol
document has the details.

Testing & Contributing
-----------------------

```bash
composer install
composer check   # coding standards + static analysis + PHPUnit
```

The suite is split in three — 1717 unit tests, 321 compliance tests replaying the 314 documented
wire vectors, and 540 integration tests against a real broker over its four listeners, without a single skip:

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
| `KAFKA_SSL_BOOTSTRAP_SERVERS` | `127.0.0.1:9093` | `SslTransportTest`, against the certificate the container was built with (`docker/kafka-3.9.2/ssl/broker.crt`) |
| `KAFKA_SASL_BOOTSTRAP_SERVERS` | – | the SASL/PLAIN tests over `SASL_PLAINTEXT` (`127.0.0.1:9094`) |
| `KAFKA_SASL_SSL_BOOTSTRAP_SERVERS` | – | the same exchange inside TLS (`127.0.0.1:9095`) |
| `KAFKA_CONTAINER` | `kafka-3-9-2` | the container the log dumps and the topic tools of a few tests run their scripts in (the quotas are set through the wire since this line) |

The compliance suite replays every wire vector of
[docs/protocol/vectors](docs/protocol/vectors) — frames that a real Kafka broker sent or
accepted — through the request and response classes and checks that the annotated dumps of
[docs/protocol/3.9.md](docs/protocol/3.9.md) still hold the same bytes, and that every
`@see docs/protocol/3.9.md, section "…"` of the sources names a heading that exists, so the
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
| [`admin.php`](examples/admin.php) | brokers, cluster id and controller, topics, offsets, groups, and `deleteConsumerGroups()` (KIP-229) |
| [`create-topic.php`](examples/create-topic.php) | `createTopics()` / `deleteTopics()` with `validateOnly`, and `createPartitions()` growing a topic (KIP-195) |
| [`admin-configs.php`](examples/admin-configs.php) | `describeConfigs()` with the config **sources and synonyms** of KIP-226, `alterConfigs()` on a topic and on a **broker** resource, and `deleteRecords()` |
| [`admin-log-dirs.php`](examples/admin-log-dirs.php) | `describeLogDirs()` and `alterReplicaLogDirs()` — the disks of a broker and a replica moved between them (KIP-113) |
| [`delegation-tokens.php`](examples/delegation-tokens.php) | the four token apis of KIP-48 over a SASL listener: create, describe, renew and expire |
| [`offsets-for-times.php`](examples/offsets-for-times.php) | `offsetsForTimes()`, `beginningOffsets()`, `endOffsets()` |
| [`ssl.php`](examples/ssl.php) | the SSL listener, 9093 |
| [`sasl.php`](examples/sasl.php) | SASL/PLAIN over 9094, and over 9095 with `KAFKA_SASL_SSL_BOOTSTRAP_SERVERS` |

Issues and pull requests are welcome.

License
-------

Released under the [MIT license](LICENSE).

[producer configuration]: https://kafka.apache.org/documentation/#producerconfigs
[consumer configuration]: https://kafka.apache.org/documentation/#newconsumerconfigs

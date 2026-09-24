<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Common\Errors\UnknownTopicIdException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ProduceRequestPartition;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopic;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopicV12;

/**
 * The produce API, version 13
 *
 * The produce API is used to send message sets to the server. For efficiency it allows sending message sets intended
 * for many topic partitions in a single request.
 *
 * The produce API uses the generic message set format, but since no offset has been assigned to the messages at the
 * time of the send the producer is free to fill in that field in any way it likes.
 *
 * <pre>
 *   ProduceRequest (Version: 8) => TransactionalId RequiredAcks Timeout [TopicName [Partition RecordSetSize
 *                                                                                   RecordSet]]
 *     TransactionalId => nullable string
 *     RequiredAcks    => int16
 *     Timeout         => int32
 * </pre>
 *
 * The body of this request did not change between the versions 0 and 2: `PRODUCE_REQUEST_V2` is
 * `PRODUCE_REQUEST_V1` is `PRODUCE_REQUEST_V0` in `ProduceRequest.schemaVersions()` @ 1.1.1, so up to version 2 a
 * version only selects the layout of the answer - version 1 (Kafka 0.9) appended `ThrottleTime` to it, version 2
 * (Kafka 0.10.0) added the `LogAppendTime` of every partition, see {@see ProduceResponse} - and what the broker
 * does with the record set it is given.
 *
 * **Version 3 (Kafka 0.11.0, KIP-98) is the first one that changed the request**: it prefixes the body with the
 * nullable `TransactionalId` of the producer, and the record set of every partition is a **record batch of the
 * message format v2** ({@see \Protocol\Kafka\Common\Record\RecordBatch}) instead of a message set. That batch is
 * what carries the record headers, the producer id, the producer epoch and the sequence numbers of an idempotent
 * or transactional producer, so none of them can travel below this version.
 *
 * **The versions 4 and 5 (Kafka 1.0) send the very same body again**: `PRODUCE_REQUEST_V5` is `PRODUCE_REQUEST_V4`
 * is `PRODUCE_REQUEST_V3` in `ProduceRequest.schemaVersions()` @ 1.1.1, and each of the two only states something
 * about the *client*:
 *
 * * **version 4** says that the client understands the error code **56** `KAFKA_STORAGE_ERROR`; a broker translates
 *   that condition to 6 `NOT_LEADER_FOR_PARTITION` for a request of version 3 or lower
 *   (`ProduceRequest.java` @ 1.1.1: "The KafkaStorageException will be translated to NotLeaderForPartitionException
 *   in the response if version <= 3");
 * * **version 5** says that the client understands the `LogStartOffset` that the answer gained, see
 *   {@see \Protocol\Kafka\Protocol\Data\ProduceResponsePartition::$logStartOffset}.
 *
 * **Version 6 (Kafka 2.0, KIP-219) sends the same body once more**: `PRODUCE_REQUEST_V6` is `PRODUCE_REQUEST_V5`
 * is `PRODUCE_REQUEST_V3` - `ProduceRequest.json` @ 2.8.2 has no field above version 3 - so a version 6 request is
 * a version 3 request with another number in its header. What the version states is a promise of the **client**:
 * that it honours the `ThrottleTime` of the answer itself, because a throttled request is answered **before** the
 * delay and the channel is muted for the reported time afterwards, instead of the answer being held back until the
 * throttle has passed. {@see \Protocol\Kafka\Client} does that: it sleeps the remaining throttle time of a
 * broker before its next request to it, unless {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT}
 * switches it off.
 *
 * **A 2.8.2 broker does not branch on the version here** - measured, and `KafkaApis.handleProduceRequest` @ 2.8.2
 * says so itself ("Send the response immediately. In case of throttling, the channel has already been muted"): a
 * version 5 request of the same burst is answered just as quickly and with the same `ThrottleTime`. The version is
 * what the client promises, not what the broker decides, and a client that sends a lower version simply stalls on
 * the muted channel instead.
 *
 * **Version 7 (Kafka 2.1, KIP-110) sends the same body a fourth time, and it is the version a zstd batch needs.**
 * `ProduceRequest.json` @ 2.8.2 still has no field above version 3, so the frame is the frame of version 3 with
 * another number in its header; what the version states is that the record sets of this request may carry the
 * compression type **4**, zstd. `ProduceRequest.validateRecords` @ 2.8.2 refuses a lower version that does:
 *
 * ```java
 * if (version < 7 && entry.compressionType() == CompressionType.ZSTD) {
 *     throw new UnsupportedCompressionTypeException("Produce requests with version " + version + " are not allowed to use ZStandard compression");
 * }
 * ```
 *
 * and the check runs on the **broker** as well, because it parses the request with the same class - measured on
 * the container, see the section of the document. This is why the client sends version 7 as soon as
 * {@see \Protocol\Kafka\Producer\ProducerConfig::COMPRESSION_TYPE} may be `zstd`
 * ({@see \Protocol\Kafka\Common\Record\CompressionCodec::ZSTD}); the answer is unchanged, see
 * {@see ProduceResponse}.
 *
 * **Version 8 (Kafka 2.4, KIP-467) sends the same body a fifth time**, and what it states is again about the
 * answer: that the client understands the `record_errors` and the `error_message` that a refused batch is
 * answered with, see {@see ProduceResponse}. `ProduceRequest.json` @ 2.8.2 has no field of it either.
 *
 * **Version 9 (Kafka 2.8) is the flexible version of KIP-482**, see {@see self::FLEXIBLE_VERSION}: the same body
 * once more, written with the request header **v2**, a compact `transactional_id` and topic name, compact arrays,
 * a **compact record set** and a tagged-field section at the end of the body, of every topic entry and of every
 * partition entry. {@see ProduceRequestV9} keeps that version and {@see ProduceRequestV8} the plain frame.
 *
 * **Version 10 (Kafka 3.7, KIP-951) sends the version 9 body a seventh time.** `ProduceRequest.json` @ 3.7.2
 * declares no field of it and its whole comment is "Version 10 is the same as version 9 (KIP-951)", so a version
 * 10 request is a version 9 request with another number in its header. What the version states is that the client
 * understands the **leader discovery** the answer gained: the tagged `current_leader` of a partition entry that
 * was refused **6** `NOT_LEADER_OR_FOLLOWER` and the tagged `node_endpoints` of the body that says where that
 * leader can be reached, see {@see ProduceResponse::$nodeEndpoints}. A producer that reads them re-sends the
 * batch to the new leader without asking Metadata first, which is the round trip the KIP removes.
 * {@see ProduceRequestV10} keeps that version.
 *
 * **Version 11 (Kafka 3.8, KIP-890) sends the very same body an eighth time** - `ProduceRequest.json` @ 3.8.1
 * declares no field of it and its whole comment is "Version 11 adds support for new error code
 * TRANSACTION_ABORTABLE (KIP-890)" - so a version 11 request is a version 10 request with another number in its
 * header. What it states is that the client understands the error code
 * **120** ({@see \Protocol\Kafka\Common\Errors\TransactionAbortableException}) in a partition of the answer: the
 * broker's way of saying "this transaction can not be committed any more, abort it and carry on with the same
 * transactional id" instead of the fatal-looking **48** `InvalidTxnState` the versions below are answered.
 *
 * **The version alone is what the broker decides that on.**
 * `KafkaApis.handleProduceRequest` @ 3.9.2 turns it into the `TransactionSupportedOperation` of the append,
 * `val transactionSupportedOperation = if (request.header.apiVersion > 10) genericError else defaultError`, and
 * `AddPartitionsToTxnManager` @ 3.9.2 maps a 120 of the verification back to the 48 for everything below
 * ("For backward compatibility with clients"). Measured on the node: a transactional batch for a partition the
 * coordinator has not verified is answered **120** at this version and **48** with the message "Partition was
 * not added to the transaction" at version 10, see the section of the document.
 *
 * **Version 12 (Kafka 4.0, KIP-890 part 2) sends the very same body a ninth time**; {@see ProduceRequestV12} keeps
 * it. `ProduceRequest.json` @ 4.0.0 declares no field of it - "Version 12 is the same as version 11 (KIP-890)"
 * - and what the number changes is the meaning of a **transactional** batch on a node that finalizes the feature
 * `transaction.version` 2 (the transaction protocol v2): "if transaction V2 (KIP_890 part 2) is enabled, the
 * produce request will also include the function for a AddPartitionsToTxn call. If V2 is disabled, the client
 * can't use produce request version higher than 11 within a transaction". A batch outside a transaction - no
 * transactional id - is appended exactly as at version 11, so the client sends version 12 there, and caps a
 * transaction at {@see ProduceRequestV11} unless its producer speaks the protocol v2, see
 * {@see \Protocol\Kafka\Client::produceVersion()}.
 *
 * **Kafka 4.0 also removed the versions 0 to 2 (KIP-896)**: "Versions 0-2 were removed in Apache Kafka 4.0, version
 * 3 is the new baseline". A 4.x node still lists Produce from version 0 in its ApiVersions answer - "due to a bug in
 * librdkafka, these versions have to be included in the api versions response (see KAFKA-18659), but are rejected
 * otherwise" - and closes the connection on a frame of them. The classes of those versions stay, for the wire
 * vectors of the lines below and for a peer of Kafka 3.x.
 *
 * **Version 13 (Kafka 4.1, KIP-516) names every topic by its id**, and this class is version 13:
 * `ProduceRequest.json` @ 4.1.0, "Version 13 replaces topic names with topic IDs (KIP-516). May return
 * UNKNOWN_TOPIC_ID error code" - the `Name` of a topic entry is `"versions": "0-12"`, the new `TopicId` `"13+"`. So
 * a client can not produce to a topic whose id it does not know; the `$topicIds` of the constructor are where it
 * states them ({@see \Protocol\Kafka\Common\Cluster::topicIdsOf()} is where it learns them), and a version 13
 * request without the id of one of its topics is refused before it is built, with
 * {@see UnknownTopicIdException}. The partitions and the record sets did not change.
 *
 * {@see ProduceRequestV12}, {@see ProduceRequestV11}, {@see ProduceRequestV10}, {@see ProduceRequestV9}, {@see ProduceRequestV8}, {@see ProduceRequestV7}, {@see ProduceRequestV6}, {@see ProduceRequestV5}, {@see ProduceRequestV4}, {@see ProduceRequestV3},
 * {@see ProduceRequestV2}, {@see ProduceRequestV1} and {@see ProduceRequestV0} keep the lower versions - and with
 * them the legacy message sets - available.
 *
 * The broker does **not** check the message format against the api version: it stores whatever it is given in the
 * `message.format.version` of the topic and converts the batch on append. What a version really states is what the
 * *client* understands, and the version of a Produce request only ever matters for the answer it selects; it is the
 * Fetch api that converts a log down for a client that asked with an older version.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v13)", "The abortable transaction error of
 *      KIP-890 (v11)", "The transaction protocol v2 of KIP-890 part 2 (v12)" and "The topic ids of the produce path
 *      (v13, KIP-516)"
 */
class ProduceRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::PRODUCE;

    /**
     * @inheritdoc
     */
    public const int VERSION = 13;

    /**
     * First version of this api whose frame is written with the compact types and the tagged fields of KIP-482
     *
     * `ProduceRequest.json` @ 2.8.2 declares `"flexibleVersions": "9+"` and comments "Version 9 enables flexible
     * versions": not a field was added, the encoding changed. The record set of a partition entry is then a
     * **compact** byte array - an unsigned varint of `length + 1` in front of the batches instead of an int32.
     */
    public const int FLEXIBLE_VERSION = 9;

    /**
     * Lowest version a node of Kafka 4.0 or later serves (KIP-896), and the first one that carries a record batch
     *
     * `ProduceRequest.json` @ 4.0.0: "Versions 0-2 were removed in Apache Kafka 4.0, version 3 is the new baseline".
     * Version 3 (Kafka 0.11.0, KIP-98) is also where the message format v2 and the transactional id came in, so a
     * message set of the formats v0 and v1 has no place in any version a 4.x node accepts.
     */
    public const int BASELINE_VERSION = 3;

    /**
     * Version the api gained in the release that raised its baseline to {@see self::BASELINE_VERSION}
     *
     * `ProduceRequest.json` @ 4.0.0 declares `"validVersions": "3-12"` in the commit that removed the versions 0 to 2
     * and added version 12, and the ApiVersions answer of a node of Kafka 4.0 or later still lists Produce from
     * version **0** (KAFKA-18659, `ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION` @ 4.0.0) - so it is the
     * **top** of the row that tells whether the node serves the versions below 3: a row that reaches this version
     * does not, whatever its minimum says.
     */
    public const int BASELINE_RAISED_WITH_VERSION = 12;

    /**
     * Value of RequiredAcks for which the broker sends no response at all
     */
    public const int ACKS_NONE = 0;

    /**
     * Record sets to append, indexed by the topic name - a list from version 13 on, whose entries carry no name
     *
     * @var array<array-key, ProduceRequestTopic>
     */
    public array $topicMessages = [];

    /**
     * Id of every topic this request names, as topic name => the 16 raw bytes of its uuid (KIP-516)
     *
     * Version 13 names every topic by its id and by nothing else; every version below it ignores the map.
     *
     * @var array<string, string>
     */
    protected readonly array $topicIds;

    /**
     * @param array<string, array<int, string|\Stringable>> $topicPartitionRecords Encoded record sets in the format
     *                              topic => [partition => record set]. A version 3 or higher request carries a
     *                              record batch of the message format v2 there, a lower one a message set of the
     *                              format v0 or v1.
     * @param int    $requiredAcks  This field indicates how many acknowledgements the servers should receive before
     *                              responding to the request.
     *                              If it is 0 the server will not send any response
     *                              (this is the only case where the server will not reply to a request).
     *                              If it is 1, the server will wait the data is written to the local log before
     *                              sending a response.
     *                              If it is -1 the server will block until the message is committed by all in sync
     *                              replicas before sending a response.
     * @param int    $timeout       This provides a maximum time in milliseconds the server can await the receipt of
     *                              the number of acknowledgements in RequiredAcks.
     * @param string $clientId      ApiKeys client identifier
     * @param int    $correlationId Correlation request ID (will be returned in the response)
     */
    public function __construct(
        array $topicPartitionRecords,
        protected readonly int $requiredAcks,
        protected readonly int $timeout,
        string $clientId = '',
        int $correlationId = 0,
        /**
         * Transactional id of the producer, `null` for everything that is not part of a transaction.
         *
         * The broker authorizes a transactional produce request on this id and refuses a batch whose
         * `transactional` attribute bit is set without one; a plain or a merely idempotent producer leaves it null.
         * The field exists since version 3 (Kafka 0.11.0, KIP-98).
         */
        protected readonly ?string $transactionalId = null,
        /**
         * Id of every topic named above, as name => the 16 raw bytes of its uuid; **version 13 needs one per topic**
         * (KIP-516) and throws {@see UnknownTopicIdException} without it, every lower version ignores the map.
         */
        array $topicIds = []
    ) {
        $this->topicIds = $topicIds;
        $topicClass     = static::topicClass();
        foreach ($topicPartitionRecords as $topic => $partitionRecordSets) {
            $partitions = [];
            foreach ($partitionRecordSets as $partition => $recordSet) {
                $partitions[$partition] = new ProduceRequestPartition($partition, $recordSet);
            }

            if (static::VERSION >= 13) {
                // A version 13 entry carries no name at all, so the list it travels in is the only honest shape
                $this->topicMessages[] = new $topicClass((string) $topic, $partitions, self::idOf($topicIds, (string) $topic));
            } else {
                $this->topicMessages[$topic] = new $topicClass((string) $topic, $partitions);
            }
        }

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [];
        if (static::VERSION >= 3) {
            $body['transactionalId'] = BinarySchema::TYPE_NULLABLE_STRING;
        }
        $body['requiredAcks']  = BinarySchema::TYPE_INT16;
        $body['timeout']       = BinarySchema::TYPE_INT32;
        // From version 13 the entries carry no name, so there is no field to index the array by
        $body['topicMessages'] = static::VERSION >= 13
            ? [static::topicClass()]
            : ['topic' => static::topicClass()];

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class sends
     *
     * @return class-string<ProduceRequestTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 13 ? ProduceRequestTopic::class : ProduceRequestTopicV12::class;
    }

    /**
     * Returns the id of a topic from the map of the caller
     *
     * A version below 13 names its topics by name and never looks at the map; a version 13 frame can not name a
     * topic at all without its id, and a client that does not know it refreshes its metadata instead of guessing.
     *
     * @param array<string, string> $topicIds Id of every topic, as name => the 16 raw bytes of its uuid
     *
     * @throws UnknownTopicIdException If a version 13 request names a topic whose id the caller did not state
     */
    private static function idOf(array $topicIds, string $topic): string
    {
        $topicId = $topicIds[$topic] ?? Uuid::ZERO;
        if (Uuid::isZero($topicId)) {
            throw new UnknownTopicIdException(
                [
                    'error' => 'A Produce request of version 13 names its topics by id (KIP-516), and this client'
                        . ' does not know the id of this one yet',
                    'topic' => $topic,
                ]
            );
        }

        return $topicId;
    }

    /**
     * Returns the id of every topic this request names, as topic name => the 16 raw bytes of its uuid
     *
     * @return array<string, string>
     */
    public function getTopicIds(): array
    {
        return $this->topicIds;
    }

    /**
     * Tells whether the broker will answer this request at all.
     *
     * `RequiredAcks = 0` is the only case in the whole protocol in which the broker sends no response: the client
     * must not wait for one, otherwise it would read the answer of the next request from that connection.
     */
    public function expectsResponse(): bool
    {
        return $this->requiredAcks !== self::ACKS_NONE;
    }

    /**
     * Returns the number of acknowledgements the broker was asked to wait for
     */
    public function getRequiredAcks(): int
    {
        return $this->requiredAcks;
    }

    /**
     * Returns the transactional id this batch was sent under, `null` outside of a transaction
     */
    public function getTransactionalId(): ?string
    {
        return $this->transactionalId;
    }
}

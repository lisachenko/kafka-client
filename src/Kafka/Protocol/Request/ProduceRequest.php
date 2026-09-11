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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ProduceRequestPartition;
use Protocol\Kafka\Protocol\Data\ProduceRequestTopic;

/**
 * The produce API, version 6
 *
 * The produce API is used to send message sets to the server. For efficiency it allows sending message sets intended
 * for many topic partitions in a single request.
 *
 * The produce API uses the generic message set format, but since no offset has been assigned to the messages at the
 * time of the send the producer is free to fill in that field in any way it likes.
 *
 * <pre>
 *   ProduceRequest (Version: 6) => TransactionalId RequiredAcks Timeout [TopicName [Partition RecordSetSize
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
 * {@see ProduceRequestV5}, {@see ProduceRequestV4}, {@see ProduceRequestV3}, {@see ProduceRequestV2},
 * {@see ProduceRequestV1} and {@see ProduceRequestV0} keep the lower versions - and with them the legacy message
 * sets - available.
 *
 * The broker does **not** check the message format against the api version: it stores whatever it is given in the
 * `message.format.version` of the topic and converts the batch on append. What a version really states is what the
 * *client* understands, and the version of a Produce request only ever matters for the answer it selects; it is the
 * Fetch api that converts a log down for a client that asked with an older version.
 *
 * @see docs/protocol/2.8.md, section "Produce API (key 0, v0 to v6)"
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
    public const int VERSION = 6;

    /**
     * Value of RequiredAcks for which the broker sends no response at all
     */
    public const int ACKS_NONE = 0;

    /**
     * Record sets to append, indexed by the topic name
     *
     * @var array<string, ProduceRequestTopic>
     */
    public array $topicMessages = [];

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
        protected readonly ?string $transactionalId = null
    ) {
        foreach ($topicPartitionRecords as $topic => $partitionRecordSets) {
            $partitions = [];
            foreach ($partitionRecordSets as $partition => $recordSet) {
                $partitions[$partition] = new ProduceRequestPartition($partition, $recordSet);
            }

            $this->topicMessages[$topic] = new ProduceRequestTopic((string) $topic, $partitions);
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
        $body['topicMessages'] = ['topic' => ProduceRequestTopic::class];

        return $header + $body;
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

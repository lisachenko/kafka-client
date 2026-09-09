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
 * The produce API, version 2
 *
 * The produce API is used to send message sets to the server. For efficiency it allows sending message sets intended
 * for many topic partitions in a single request.
 *
 * The produce API uses the generic message set format, but since no offset has been assigned to the messages at the
 * time of the send the producer is free to fill in that field in any way it likes.
 *
 * <pre>
 *   ProduceRequest (Version: 2) => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
 *     RequiredAcks => int16
 *     Timeout      => int32
 * </pre>
 *
 * The body of this request has not changed since version 0: `PRODUCE_REQUEST_V2` is `PRODUCE_REQUEST_V1` is
 * `PRODUCE_REQUEST_V0` in `Protocol.java` @ 0.10.2.2. What a version does select is the layout of the answer -
 * version 1 (Kafka 0.9) appended `ThrottleTime` to it and version 2 (Kafka 0.10.0) added the `LogAppendTime` of
 * every partition, see {@see ProduceResponse} - and, since Kafka 0.10.0, what the broker does with the message set
 * it is given: a version 2 request may carry message format v1, while a batch of a version 0 or 1 request is
 * expected to be message format v0. The broker converts either of them into the `message.format.version` of the
 * topic, so a magic 0 batch of a version 2 request is accepted and stored as v1 with the timestamp -1
 * (`Log.append` @ 0.10.2.2, verified against the broker).
 *
 * {@see ProduceRequestV1} and {@see ProduceRequestV0} keep the lower pairs available.
 *
 * The `TransactionalId` of the later protocol lines arrived with version 3 of this API (Kafka 0.11.0).
 *
 * @see docs/protocol/0.11.0.md, section "Produce API (key 0, v0, v1 and v2)"
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
    public const int VERSION = 2;

    /**
     * Value of RequiredAcks for which the broker sends no response at all
     */
    public const int ACKS_NONE = 0;

    /**
     * Message sets to append, indexed by the topic name
     *
     * @var array<string, ProduceRequestTopic>
     */
    public array $topicMessages = [];

    /**
     * @param array<string, array<int, string|\Stringable>> $topicMessages Encoded message sets in the format
     *                                                                     topic => [partition => message set]
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
        array $topicMessages = [],
        protected readonly int $requiredAcks = 1,
        protected readonly int $timeout = 0,
        string $clientId = '',
        int $correlationId = 0
    ) {
        foreach ($topicMessages as $topic => $partitionMessageSets) {
            $partitions = [];
            foreach ($partitionMessageSets as $partition => $messageSet) {
                $partitions[$partition] = new ProduceRequestPartition($partition, $messageSet);
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

        return $header + [
            'requiredAcks'  => BinarySchema::TYPE_INT16,
            'timeout'       => BinarySchema::TYPE_INT32,
            'topicMessages' => ['topic' => ProduceRequestTopic::class],
        ];
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
}

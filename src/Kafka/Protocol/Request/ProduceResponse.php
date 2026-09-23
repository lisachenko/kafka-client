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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ProduceResponseCurrentLeader;
use Protocol\Kafka\Protocol\Data\ProduceResponseNodeEndpoint;
use Protocol\Kafka\Protocol\Data\ProduceResponsePartition;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV0;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV2;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV5;
use Protocol\Kafka\Protocol\Data\ProduceResponseTopicV8;
use Protocol\Kafka\Protocol\TaggedField;

/**
 * Produce response object, version 11
 *
 * <pre>
 *   ProduceResponse (Version: 8) => [TopicName [Partition ErrorCode Offset LogAppendTime LogStartOffset
 *                                                [RecordErrors] ErrorMessage]]
 *                                   ThrottleTime
 *     RecordErrors => BatchIndex BatchIndexErrorMessage
 *       BatchIndex             => int32
 *       BatchIndexErrorMessage => nullable string
 *     ErrorMessage   => nullable string
 *     LogAppendTime  => int64
 *     LogStartOffset => int64
 *     ThrottleTime   => int32
 *     CurrentLeader  => tag 0 of a partition entry, [LeaderId int32 LeaderEpoch int32] -- since version 10
 *     NodeEndpoints  => tag 0 of the BODY, [NodeId int32 Host compact string Port int32
 *                       Rack compact nullable string] -- since version 10
 * </pre>
 *
 * Version 1 of the API added `ThrottleTime` **after** the topics array (`PRODUCE_RESPONSE_V1` in
 * `ProduceResponse.schemaVersions()` @ 1.1.1): the number of milliseconds the broker delayed this request because
 * the client exceeded its produce quota. A broker without quotas - the default, `quota.producer.default` is
 * unlimited - always answers 0.
 *
 * Version 2 (Kafka 0.10.0, message format v1) added `LogAppendTime` to every partition entry, in front of that
 * throttle time, see {@see ProduceResponsePartition::$logAppendTime}. **The versions 3 and 4 changed nothing at
 * all**: `PRODUCE_RESPONSE_V4` is `PRODUCE_RESPONSE_V3` is `PRODUCE_RESPONSE_V2`, so a broker really answers a
 * version 3 or a version 4 request with the version 2 frame, and {@see ProduceResponseV4},
 * {@see ProduceResponseV3} and {@see ProduceResponseV2} decode the very same bytes - they only differ in the
 * version of the request they belong to.
 *
 * **Version 5 (Kafka 1.0) is the next one that really changed the answer**: every partition entry gains
 * `LogStartOffset` behind its `LogAppendTime`, the earliest offset the log of that partition still holds
 * ({@see ProduceResponsePartition::$logStartOffset}). An idempotent producer needs it to tell a *spurious*
 * `OutOfOrderSequence` - its records fell below the log start offset and the broker forgot its producer state,
 * which arrives as the error code 59 `UNKNOWN_PRODUCER_ID` - from a real one. {@see ProduceResponseV1} and
 * {@see ProduceResponseV0} carry the two lower frames that really differ.
 *
 * **Version 6 (Kafka 2.0, KIP-219) changed the answer no more than 3 and 4 did**: `ProduceResponse.json` @ 2.8.2
 * carries no field of it, and {@see ProduceResponseV5} decodes the very same bytes. What version 6 states is that
 * the client understands **when** a throttled answer arrives: the broker sends it first, with the delay it is
 * about to impose in `ThrottleTime`, and mutes the channel for that long afterwards, so the client has to wait the
 * value out itself, see {@see \Protocol\Kafka\Common\ClientConfig::THROTTLE_WAIT}. A 2.8.2 broker answers every
 * version that way, the promise of the version notwithstanding.
 *
 * **Version 7 (Kafka 2.1, KIP-110) changes it no more**: `ProduceResponse.json` @ 2.8.2 has no field of version
 * 7 either, and {@see ProduceResponseV6} decodes the same bytes. What version 7 states lives entirely in the
 * request - that its record sets may be compressed with zstd, see {@see ProduceRequest}.
 *
 * **Version 8 (Kafka 2.4, KIP-467) names the records that a refused batch was refused for**: every partition
 * entry gains a `record_errors` array of `[batch_index, batch_index_error_message]` pairs and an
 * `error_message`, both behind the `log_start_offset`. A batch that fails validation is answered with one error
 * code for the **whole partition** - **87** `INVALID_RECORD` for a record without a key on a compacted topic, an
 * invalid timestamp, a broken checksum - and until this version that was all a producer learned; now it also
 * learns which records of the batch it was, by their position in the batch. See
 * {@see \Protocol\Kafka\Protocol\Data\ProduceResponseRecordError} and
 * {@see ProduceResponsePartition::$recordErrors}; {@see ProduceResponseV7} decodes the frame without the two
 * fields.
 *
 * **Version 10 (Kafka 3.7, KIP-951) is the first version of this api that declares a tagged field**, and it
 * declares two of them: the `current_leader` of a partition entry (tag 0,
 * {@see \Protocol\Kafka\Protocol\Data\ProduceResponseCurrentLeader}) and the top-level `node_endpoints` of the
 * body (tag 0 as well, {@see self::$nodeEndpoints}). The request of that version is the request of version 9 with
 * another number in its header, see {@see ProduceRequest}; {@see ProduceResponseV9} keeps the answer that has
 * neither, and {@see \Protocol\Kafka\Protocol\Data\ProduceResponsePartitionV8} its partition entry.
 *
 * **Version 11 (Kafka 3.8, KIP-890) adds no field either** - `ProduceResponse.json` @ 3.8.1 comments "Version 11
 * adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" and declares nothing - so this class decodes
 * the version 10 frame, and {@see ProduceResponseV10} the very same bytes for the version below. What changes is
 * the **error code** a partition of a transactional produce may carry: **120** `TransactionAbortable`
 * ({@see \Protocol\Kafka\Common\Errors\TransactionAbortableException}), which says that the transaction can not
 * be committed any more but the producer is intact, where version 10 is answered the **48** `InvalidTxnState`
 * with the message "Partition was not added to the transaction". A broker picks between the two on the api
 * version alone, see {@see ProduceRequest}.
 *
 * A request with `RequiredAcks = 0` is never answered at all, see {@see ProduceRequest::expectsResponse()}.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v11)", "The leader discovery of KIP-951 (v10)"
 *      and "The abortable transaction error of KIP-890 (v11)"
 */
class ProduceResponse extends AbstractResponse
{
    /**
     * Version of the Produce API that this class decodes the answer of
     */
    public const int VERSION = 11;

    /**
     * First version of this api whose frame is written with the compact types and the tagged fields of KIP-482
     *
     * `ProduceResponse.json` @ 2.8.2 declares `"flexibleVersions": "9+"`; not a field was added to the answer,
     * the encoding changed - the response header **v1**, compact strings and arrays and a tagged-field section
     * behind every structure.
     */
    public const int FLEXIBLE_VERSION = 9;

    /**
     * Result for each topic of the request, indexed by the topic name
     *
     * @var array<string, ProduceResponseTopic>
     */
    public array $topics = [];

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas.
     *
     * @since Version 1 of protocol
     */
    public int $throttleTime = 0;

    /**
     * Where the leaders this answer named can be reached, as node id => endpoint (KIP-951)
     *
     * The top-level **tagged** field (tag 0) that version 10 added, the other half of the
     * {@see ProduceResponseCurrentLeader} of a partition entry: the id, the host, the port and the rack of every
     * node that one of those entries points at, each named once.
     * `ProduceResponse.json` @ 3.7.2 says "Endpoints for all current-leaders enumerated in
     * PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER", and its default is the **empty array**, so
     * an answer that refused nothing carries the field not at all.
     *
     * A one-broker cluster can never fill it: the node is the leader of every partition it hosts, so no produce
     * of it is ever answered 6, which is the one condition `KafkaApis.handleProduceRequest` @ 3.9.2 writes the
     * hint for. The frames of this line therefore document the shape and not a capture, see the section of the
     * document.
     *
     * @since Version 10 of protocol (Kafka 3.7, KIP-951)
     *
     * @var array<int, ProduceResponseNodeEndpoint>
     */
    public array $nodeEndpoints = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();
        $body   = [
            'topics' => ['topic' => static::topicClass()],
        ];
        if (static::VERSION >= 1) {
            $body['throttleTime'] = BinarySchema::TYPE_INT32;
        }
        // The `node_endpoints` of version 10 is a TAGGED field (tag 0) and therefore travels at the end of the
        // body, behind the throttle time, and only when the broker really named a leader
        if (static::VERSION >= 10) {
            $body['nodeEndpoints'] = new TaggedField(0, ['nodeId' => ProduceResponseNodeEndpoint::class], []);
        }

        return $header + $body;
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<ProduceResponseTopic>
     */
    protected static function topicClass(): string
    {
        return match (true) {
            static::VERSION >= 10 => ProduceResponseTopic::class,
            static::VERSION >= 8 => ProduceResponseTopicV8::class,
            static::VERSION >= 5 => ProduceResponseTopicV5::class,
            static::VERSION >= 2 => ProduceResponseTopicV2::class,
            default              => ProduceResponseTopicV0::class,
        };
    }
}

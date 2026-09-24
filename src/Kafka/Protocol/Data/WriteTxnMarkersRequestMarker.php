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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\Request\EndTxnRequest;

/**
 * One marker of a WriteTxnMarkers request, i.e. one entry of the `transaction_markers` array
 *
 * <pre>
 *   WriteTxnMarkersRequestMarker => producer_id producer_epoch transaction_result [topics] coordinator_epoch
 *                                   transaction_version
 *     producer_id         => INT64
 *     producer_epoch      => INT16
 *     transaction_result  => BOOLEAN
 *     topics              => topic [partitions]
 *       topic      => STRING
 *       partitions => INT32
 *     coordinator_epoch   => INT32
 *     transaction_version => INT8 (version 2+)
 * </pre>
 *
 * `WRITE_TXN_MARKERS_ENTRY_V0` in `Protocol.java` @ 0.11.0.3. One entry writes the marker of **one producer** into
 * every partition it lists; a coordinator that completes several transactions at once puts one entry per producer
 * into the same request.
 *
 * `coordinator_epoch` is the epoch of the `__transaction_state` partition that the sending coordinator owns, not
 * the epoch of the producer: a leader that already saw a *higher* one refuses the marker with the error code
 * **52** (`TransactionCoordinatorFenced`), which is how a coordinator that lost its partition is stopped from
 * writing markers behind the back of the one that took it over.
 *
 * `transaction_version` is the field of the version 2 (Kafka 4.2, *"Version 2 adds TransactionVersion field to the
 * WritableTxnMarker (KIP-1228)"* in `WriteTxnMarkersRequest.json` @ 4.2.0): the transaction protocol the marker
 * ends a transaction of - 0 and 1 the legacy ones, 2 the protocol v2 of KIP-890. {@see WriteTxnMarkersRequestMarkerV1}
 * is the entry of the versions 0 and 1, without it.
 *
 * @see docs/protocol/4.3.md, section "WriteTxnMarkers API (key 27, v0 to v2)"
 */
class WriteTxnMarkersRequestMarker implements BinarySchemaInterface
{
    /**
     * Version of the WriteTxnMarkers API that this DTO encodes an entry of
     */
    public const int VERSION = 2;

    /**
     * Producer id whose transaction is being completed
     */
    public int $producerId;

    /**
     * Epoch of that producer id
     */
    public int $producerEpoch;

    /**
     * Result to write into every partition: false is an ABORT marker, true a COMMIT marker
     */
    public bool $transactionResult;

    /**
     * Partitions to write the marker into, indexed by the topic name
     *
     * @var array<string, PartitionsForTopic>
     */
    public array $topics;

    /**
     * Epoch of the `__transaction_state` partition the sending coordinator owns
     */
    public int $coordinatorEpoch;

    /**
     * Transaction version of the marker: 0 or 1 for the legacy protocols, 2 for the transaction protocol v2
     *
     * @since Version 2 of protocol (Kafka 4.2, KIP-1228)
     */
    public int $transactionVersion = 0;

    /**
     * @param array<string, list<int>|PartitionsForTopic> $topicPartitions    Partitions to write the marker into
     * @param int                                         $transactionVersion Transaction version of the marker
     *        (version 2), the `default` 0 of the field
     */
    public function __construct(
        int $producerId,
        int $producerEpoch,
        bool $transactionResult = EndTxnRequest::COMMIT,
        array $topicPartitions = [],
        int $coordinatorEpoch = 0,
        int $transactionVersion = 0
    ) {
        $packedTopics = [];
        foreach ($topicPartitions as $topic => $partitions) {
            $packedTopics[$topic] = $partitions instanceof PartitionsForTopic
                ? $partitions
                : new PartitionsForTopic((string) $topic, array_values(array_map(intval(...), $partitions)));
        }

        $this->producerId         = $producerId;
        $this->producerEpoch      = $producerEpoch;
        $this->transactionResult  = $transactionResult;
        $this->topics             = $packedTopics;
        $this->coordinatorEpoch   = $coordinatorEpoch;
        $this->transactionVersion = $transactionVersion;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            'producerId'        => BinarySchema::TYPE_INT64,
            'producerEpoch'     => BinarySchema::TYPE_INT16,
            'transactionResult' => BinarySchema::TYPE_BOOLEAN,
            'topics'            => ['topic' => PartitionsForTopic::class],
            'coordinatorEpoch'  => BinarySchema::TYPE_INT32,
        ];
        if (static::VERSION >= 2) {
            $scheme['transactionVersion'] = BinarySchema::TYPE_INT8;
        }

        return $scheme;
    }
}

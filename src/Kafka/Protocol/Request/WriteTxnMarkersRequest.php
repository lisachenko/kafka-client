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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarker;
use Protocol\Kafka\Protocol\Data\WriteTxnMarkersRequestMarkerV1;

/**
 * WriteTxnMarkers: writes the control batches of a finished transaction (key 27, Kafka 0.11, KIP-98; v2 KIP-1228)
 *
 * <pre>
 *   WriteTxnMarkers Request (Version: 2) => [transaction_markers] TAG_BUFFER
 *     transaction_markers => producer_id producer_epoch transaction_result [topics] coordinator_epoch
 *                            transaction_version TAG_BUFFER
 *       producer_id         => INT64
 *       producer_epoch      => INT16
 *       transaction_result  => BOOLEAN
 *       topics              => topic [partitions] TAG_BUFFER
 *         topic      => COMPACT_STRING
 *         partitions => COMPACT_ARRAY of INT32
 *       coordinator_epoch   => INT32
 *       transaction_version => INT8 (version 2+)
 * </pre>
 *
 * **This is a broker-to-broker request**, and the only one of the transaction protocol that a client never sends:
 * it is what the *transaction coordinator* sends to the leader of every partition of a transaction after an
 * {@see EndTxnRequest}, and it is what makes the leader append the **control batch** with the COMMIT or ABORT
 * marker that a `read_committed` consumer reads the outcome from. `TransactionMarkerChannelManager` @ 0.11.0.3
 * groups the pending markers by destination broker and repeats them until every partition has answered without a
 * retriable error; only then does the coordinator write the `CompleteCommit`/`CompleteAbort` entry into
 * `__transaction_state`.
 *
 * The classes exist here so that the api of the line is complete and so that its frame can be documented and
 * replayed, not because this client has anything to send them for. `KafkaApis.handleWriteTxnMarkersRequest`
 * requires `ClusterAction` on the cluster, which is only granted to the broker principal on a secured cluster; on
 * the unsecured container of this branch the request is served for anybody, which is what makes a wire vector of it
 * possible at all.
 *
 * **Kafka 4.2 added the version 2** (`"validVersions": "1-2"` @ 4.2.0, *"Version 2 adds TransactionVersion field
 * to the WritableTxnMarker (KIP-1228)"*): every marker carries the `transaction_version` of its transaction
 * ({@see WriteTxnMarkersRequestMarker::$transactionVersion}), and the answer is the frame of the version 1. The
 * markers given to the constructor are encoded as the entries of the version of the class, so a marker built with a
 * transaction version loses it in {@see WriteTxnMarkersRequestV1}, which keeps the version 1.
 *
 * @see docs/protocol/4.3.md, section "WriteTxnMarkers API (key 27, v0 to v2)"
 */
class WriteTxnMarkersRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::WRITE_TXN_MARKERS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * The version 1 of Kafka 2.8 is the first flexible one of this api (KIP-482)
     */
    public const int FLEXIBLE_VERSION = 1;

    /**
     * Markers to write, indexed by the producer id they belong to
     *
     * @var array<int, WriteTxnMarkersRequestMarker>
     */
    protected readonly array $transactionMarkers;

    /**
     * @param list<WriteTxnMarkersRequestMarker>|array<int, WriteTxnMarkersRequestMarker> $transactionMarkers Markers
     *        to write, one per producer whose transaction is being completed
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $transactionMarkers = [],
        string $clientId = '',
        int $correlationId = 0
    ) {
        $markerClass = static::markerClass();
        $markers     = [];
        foreach ($transactionMarkers as $marker) {
            $markers[$marker->producerId] = $marker::class === $markerClass
                ? $marker
                : new $markerClass(
                    $marker->producerId,
                    $marker->producerEpoch,
                    $marker->transactionResult,
                    $marker->topics,
                    $marker->coordinatorEpoch,
                    $marker->transactionVersion
                );
        }
        $this->transactionMarkers = $markers;

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'transactionMarkers' => ['producerId' => static::markerClass()],
        ];
    }

    /**
     * Returns the class of a marker entry for the version of the API that this class sends
     *
     * @return class-string<WriteTxnMarkersRequestMarker>
     */
    protected static function markerClass(): string
    {
        return static::VERSION >= 2 ? WriteTxnMarkersRequestMarker::class : WriteTxnMarkersRequestMarkerV1::class;
    }
}

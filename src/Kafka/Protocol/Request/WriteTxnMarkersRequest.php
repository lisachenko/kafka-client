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

/**
 * WriteTxnMarkers, version 0: writes the control batches of a finished transaction (key 27, Kafka 0.11, KIP-98)
 *
 * <pre>
 *   WriteTxnMarkers Request (Version: 0) => [transaction_markers]
 *     transaction_markers => producer_id producer_epoch transaction_result [topics] coordinator_epoch
 *       producer_id        => INT64
 *       producer_epoch     => INT16
 *       transaction_result => BOOLEAN
 *       topics             => topic [partitions]
 *         topic      => STRING
 *         partitions => INT32
 *       coordinator_epoch  => INT32
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
 * @see docs/protocol/0.11.0.md, section "WriteTxnMarkers API (key 27, v0)"
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
    public const int VERSION = 0;

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
        $markers = [];
        foreach ($transactionMarkers as $marker) {
            $markers[$marker->producerId] = $marker;
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
            'transactionMarkers' => ['producerId' => WriteTxnMarkersRequestMarker::class],
        ];
    }
}

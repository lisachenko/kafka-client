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

/**
 * Who a Fetch request comes from, the `ReplicaState` of KIP-903 (Kafka 3.5)
 *
 * The **tag 1** of a Fetch request from version 15 on, and the whole of what that version added: `FetchRequest.json`
 * @ 3.5.2 declares the top-level `ReplicaId` as `versions 0-14` and puts a `ReplicaState` of a replica id **and an
 * epoch** in its place, `"taggedVersions": "15+", "tag": 1`, both fields defaulting to `-1`.
 *
 * The epoch is what the KIP is about. A follower that is fenced - one whose broker epoch is stale because it was
 * restarted, or because the partition was reassigned away from it - used to be able to have itself added back to the
 * in-sync replica set by simply fetching: the leader had no way of telling the fetch of the current incarnation of
 * that broker from the fetch of an old one. With the epoch on the wire `Partition.maybeUpdateFollowerState`
 * @ 3.9.2 compares it against the broker epoch the controller published and ignores a fetch that carries an older
 * one, so an "AlterPartition" of a stale replica can no longer bring it back.
 *
 * **A consumer has nothing to say here**: its state is the pair `-1` / `-1`, which is the default of the
 * specification, and a tagged field whose value is its default is left off the wire altogether. A version 15 frame
 * of this client is therefore the version 14 frame **minus** the four bytes of the old `replica_id` field, see
 * {@see \Protocol\Kafka\Protocol\Request\FetchRequest::$replicaState}.
 *
 * @since Version 15 of the Fetch API (Kafka 3.5, KIP-903)
 *
 * @see docs/protocol/3.9.md, section "The replica state of KIP-903 (v15)"
 */
class FetchRequestReplicaState implements BinarySchemaInterface
{
    /**
     * Version of the Fetch API that this DTO belongs to
     */
    public const int VERSION = 15;

    /**
     * Value of both fields of the structure that a consumer means: "I am no replica at all"
     *
     * It is the `default` of both fields in `FetchRequest.json` @ 3.5.2, so a state of `-1` / `-1` is exactly the
     * state that is **not** written, {@see \Protocol\Kafka\Protocol\Request\FetchRequest::$replicaState}.
     */
    public const int UNKNOWN = -1;

    /**
     * Node id of the follower this fetch comes from, -1 for a consumer
     */
    public int $replicaId = self::UNKNOWN;

    /**
     * Broker epoch of that follower, -1 when it does not know it
     *
     * The epoch the controller gave the broker when it registered, which fences the fetch of a stale incarnation
     * of a replica; the Java `ReplicaFetcherThread` fills it from its own `BrokerEpochSupplier`.
     */
    public int $replicaEpoch = self::UNKNOWN;

    public function __construct(int $replicaId = self::UNKNOWN, int $replicaEpoch = self::UNKNOWN)
    {
        $this->replicaId    = $replicaId;
        $this->replicaEpoch = $replicaEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'replicaId'    => BinarySchema::TYPE_INT32,
            'replicaEpoch' => BinarySchema::TYPE_INT64,
        ];
    }
}

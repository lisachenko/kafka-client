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
 * The leader a refused partition really has, the `LeaderIdAndEpoch` of a Produce v10 answer (KIP-951)
 *
 * The **tag 0** of a partition entry of a Produce answer from version 10 on (Kafka 3.7), the very structure the
 * Fetch answer has carried in its own tag 1 since version 12 ({@see FetchResponseCurrentLeader}):
 * `ProduceResponse.json` @ 3.7.2 declares it `"versions": "10+", "taggedVersions": "10+", "tag": 0`, with a
 * `LeaderId` and a `LeaderEpoch` that both default to `-1`.
 *
 * A broker writes it for one condition only - a partition it refused with **6** `NOT_LEADER_OR_FOLLOWER` whose
 * leader it knows (`KafkaApis.handleProduceRequest` @ 3.9.2, the `getCurrentLeader` of the callback) - and names
 * the endpoint of that node in the top-level `node_endpoints` of the same answer,
 * {@see \Protocol\Kafka\Protocol\Request\ProduceResponse::$nodeEndpoints}. A producer that reads both re-sends
 * the batch to the new leader **without** a Metadata round trip, which is what KIP-951 is for; every other answer
 * leaves the structure off the wire, which is what a tagged field whose value is its default does.
 *
 * @since Version 10 of the Produce API (Kafka 3.7, KIP-951)
 *
 * @see docs/protocol/3.9.md, section "The leader discovery of KIP-951 (v10)"
 */
class ProduceResponseCurrentLeader implements BinarySchemaInterface
{
    /**
     * Version of the Produce API that this DTO belongs to
     */
    public const int VERSION = 10;

    /**
     * Value of both fields when the broker does not know the leader, and the default that is never written
     */
    public const int UNKNOWN = -1;

    /**
     * Node id of the current leader of the partition, -1 when the broker does not know it
     */
    public int $leaderId = self::UNKNOWN;

    /**
     * Latest leader epoch the broker knows for the partition, -1 when it does not know one
     */
    public int $leaderEpoch = self::UNKNOWN;

    public function __construct(int $leaderId = self::UNKNOWN, int $leaderEpoch = self::UNKNOWN)
    {
        $this->leaderId    = $leaderId;
        $this->leaderEpoch = $leaderEpoch;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'leaderId'    => BinarySchema::TYPE_INT32,
            'leaderEpoch' => BinarySchema::TYPE_INT32,
        ];
    }
}

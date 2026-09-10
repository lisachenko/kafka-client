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

use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponsePartition;
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponseTopic;

/**
 * OffsetForLeaderEpoch response, version 0 (key 23, Kafka 0.11)
 *
 * <pre>
 *   OffsetForLeaderEpoch Response (Version: 0) => [topics]
 *     topics => topic [partitions]
 *       topic      => STRING
 *       partitions => error_code partition_id end_offset
 *         error_code   => INT16
 *         partition_id => INT32
 *         end_offset   => INT64
 * </pre>
 *
 * The answer has neither a top-level error code nor a throttle time - the api arrived in the same release as
 * KIP-124 but is not one of the fifteen apis it touched, because a follower is not throttled by a client quota.
 * Every partition carries its own code, and a 0.11.0.3 broker reports:
 *
 * | Code | Name                    | Meaning                                                                    |
 * |------|-------------------------|----------------------------------------------------------------------------|
 * | 0    | None                    | The leader resolved the epoch; see the note on `end_offset` below           |
 * | 3    | UnknownTopicOrPartition | The cluster does not host this partition                                    |
 * | 6    | NotLeaderForPartition   | This broker is not the leader of the partition                              |
 * | 31   | ClusterAuthorizationFailed | The client may not perform a `ClusterAction`                             |
 *
 * The code 0 with the offset {@see OffsetForLeaderEpochResponsePartition::UNDEFINED_EPOCH_OFFSET} (`-1`) is the
 * answer for an epoch the leader cannot place: `Log.endOffsetForEpoch` @ 0.11.0.3 has no cache entry above the
 * requested epoch, which is what a partition that has only ever been led by the current leader answers.
 *
 * @see docs/protocol/1.1.md, section "OffsetForLeaderEpoch API (key 23, v0)"
 */
class OffsetForLeaderEpochResponse extends AbstractResponse
{
    /**
     * Version of the OffsetForLeaderEpoch API that this class decodes the answer of
     */
    public const int VERSION = 0;

    /**
     * Answer for every requested topic, indexed by the topic name
     *
     * @var array<string, OffsetForLeaderEpochResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => OffsetForLeaderEpochResponseTopic::class],
        ];
    }
}

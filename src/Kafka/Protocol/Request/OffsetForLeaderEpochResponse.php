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
use Protocol\Kafka\Protocol\Data\OffsetForLeaderEpochResponseTopicV0;

/**
 * OffsetForLeaderEpoch response, version 1 (key 23, Kafka 2.0)
 *
 * <pre>
 *   OffsetForLeaderEpoch Response (Version: 1) => [topics]
 *     topics => topic [partitions]
 *       topic      => STRING
 *       partitions => error_code partition_id leader_epoch end_offset
 *         error_code   => INT16
 *         partition_id => INT32
 *         leader_epoch => INT32     -- since version 1
 *         end_offset   => INT64
 * </pre>
 *
 * **Version 1 (Kafka 2.0, KIP-279) inserted `leader_epoch` into every partition entry**, between the partition id
 * and the end offset, see {@see OffsetForLeaderEpochResponsePartition::$leaderEpoch}. The answer still has neither
 * a top-level error code nor a throttle time - the throttle time is version 2 (Kafka 2.1) and is not part of this
 * line's scope; {@see OffsetForLeaderEpochResponseV0} keeps the frame of version 0.
 *
 * The answer has neither a top-level error code nor a throttle time - the api arrived in the same release as
 * KIP-124 but is not one of the fifteen apis it touched, because a follower is not throttled by a client quota.
 * Every partition carries its own code, and a 2.8.2 broker reports:
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
 * @see docs/protocol/2.8.md, section "OffsetForLeaderEpoch API (key 23, v0 and v1)"
 */
class OffsetForLeaderEpochResponse extends AbstractResponse
{
    /**
     * Version of the OffsetForLeaderEpoch API that this class decodes the answer of
     */
    public const int VERSION = 1;

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
            'topics' => ['topic' => static::topicClass()],
        ];
    }

    /**
     * Returns the class of a topic entry for the version of the API that this class unpacks
     *
     * @return class-string<OffsetForLeaderEpochResponseTopic>
     */
    protected static function topicClass(): string
    {
        return static::VERSION >= 1
            ? OffsetForLeaderEpochResponseTopic::class
            : OffsetForLeaderEpochResponseTopicV0::class;
    }
}

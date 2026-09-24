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
 * The results of one topic of an Initialize-, Write- or DeleteShareGroupState answer (keys 83, 85, 86, v0)
 *
 * <pre>
 *   Result => TopicId [Partitions]
 *     TopicId    => UUID
 *     Partitions => {@see ShareGroupStatePartitionResult}
 * </pre>
 *
 * `InitializeStateResult`, `WriteStateResult` and `DeleteStateResult` @ 4.1.0. The topic is named by its **id**
 * only, as everywhere in the share-group apis: the state of a share partition is keyed by the group, the topic id
 * and the partition, and a topic that is deleted and created again under its name is a different topic. The
 * results are a list, because a uuid is 16 raw bytes and no key of a PHP array a caller wants to read.
 *
 * @see docs/protocol/4.3.md, section "The share-group state apis (keys 83 to 87) — wire only"
 */
class ShareGroupStateTopicResult implements BinarySchemaInterface
{
    /**
     * The 16 raw bytes of the topic id
     */
    public string $topicId = '';

    /**
     * Result of every partition, indexed by the partition index
     *
     * @var array<int, ShareGroupStatePartitionResult>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => ['partition' => ShareGroupStatePartitionResult::class],
        ];
    }
}

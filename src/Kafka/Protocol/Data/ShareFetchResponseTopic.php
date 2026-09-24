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
 * One topic of a ShareFetch answer (key 78, Kafka 4.1, KIP-932), named by its id
 *
 * <pre>
 *   ShareFetchableTopicResponse => topic_id [partitions]
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1 and v2)"
 */
final class ShareFetchResponseTopic implements BinarySchemaInterface
{
    /**
     * The 16 raw bytes of the topic id
     */
    public string $topicId = '';

    /**
     * Partitions of the topic, indexed by the partition index
     *
     * @var array<int, ShareFetchResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => ['partitionIndex' => ShareFetchResponsePartition::class],
        ];
    }
}

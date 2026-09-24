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
 * One topic of a ShareFetch request (key 78, Kafka 4.1, KIP-932), named by its id alone
 *
 * <pre>
 *   FetchTopic => topic_id [partitions]
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "ShareFetch API (key 78, v1)"
 */
final class ShareFetchRequestTopic implements BinarySchemaInterface
{
    /**
     * @param string                           $topicId    The 16 raw bytes of the topic id
     * @param list<ShareFetchRequestPartition> $partitions Partitions of the topic
     */
    public function __construct(
        public string $topicId = '',
        public array $partitions = []
    ) {}

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => [ShareFetchRequestPartition::class],
        ];
    }
}

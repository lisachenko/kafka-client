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
 * One topic of a ShareAcknowledge request (key 79, Kafka 4.1, KIP-932), named by its id
 *
 * <pre>
 *   AcknowledgeTopic => topic_id [partitions]
 * </pre>
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1)"
 */
final class ShareAcknowledgeRequestTopic implements BinarySchemaInterface
{
    /**
     * @param string                                 $topicId    The 16 raw bytes of the topic id
     * @param list<ShareAcknowledgeRequestPartition> $partitions Partitions of the topic
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
            'partitions' => [ShareAcknowledgeRequestPartition::class],
        ];
    }
}

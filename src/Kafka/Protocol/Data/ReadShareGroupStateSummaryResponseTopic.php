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
 * The results of one topic of a ReadShareGroupStateSummary answer (key 87, v0, Kafka 4.1, KIP-932)
 *
 * `ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult` @ 4.1.0.
 *
 * The topic of version 1 (Kafka 4.2, KIP-1226) reads its partitions as
 * {@see ReadShareGroupStateSummaryResponsePartition}, with the `DeliveryCompleteCount` of that version;
 * {@see ReadShareGroupStateSummaryResponseTopicV0} reads them as {@see ReadShareGroupStateSummaryResponsePartitionV0}.
 *
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0 and v1)"
 */
class ReadShareGroupStateSummaryResponseTopic implements BinarySchemaInterface
{
    /**
     * Version of the ReadShareGroupStateSummary API that this DTO is unpacked from
     */
    public const int VERSION = 1;

    /**
     * The 16 raw bytes of the topic id
     */
    public string $topicId = '';

    /**
     * Summary of every partition, indexed by the partition index
     *
     * @var array<int, ReadShareGroupStateSummaryResponsePartition>
     */
    public array $partitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topicId'    => BinarySchema::TYPE_UUID,
            'partitions' => [
                'partition' => static::VERSION >= 1
                    ? ReadShareGroupStateSummaryResponsePartition::class
                    : ReadShareGroupStateSummaryResponsePartitionV0::class,
            ],
        ];
    }
}

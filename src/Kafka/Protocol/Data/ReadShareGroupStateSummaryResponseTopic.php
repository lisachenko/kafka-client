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
 * @see docs/protocol/4.3.md, section "ReadShareGroupStateSummary API (key 87, v0)"
 */
class ReadShareGroupStateSummaryResponseTopic implements BinarySchemaInterface
{
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
            'partitions' => ['partition' => ReadShareGroupStateSummaryResponsePartition::class],
        ];
    }
}

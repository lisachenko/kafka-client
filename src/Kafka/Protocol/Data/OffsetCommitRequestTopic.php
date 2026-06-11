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
/**
 * @author Alexander.Lisachenko
 * @date 14.07.2016
 */

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * OffsetCommitRequestTopic DTO
 *
 * OffsetCommitRequestTopic => topic [partitions]
 *   topic => STRING
 *   partitions => partition offset metadata
 *     partition => INT32
 *     offset => INT64
 *     metadata => NULLABLE_STRING
 */
class OffsetCommitRequestTopic implements BinarySchemaInterface
{
    /**
     * Partitions to commit offset.
     *
     * @var OffsetCommitRequestPartition[]
     */
    public $partitions;

    /**
     * @inheritDoc
     * @param string $topic
     */
    public function __construct(/**
     * Name of the topic
     */
        public $topic,
        array $partitions
    ) {
        $packedPartitions = [];
        foreach ($partitions as $partition => $timestamp) {
            $packedPartitions[$partition] = new OffsetCommitRequestPartition($partition, $timestamp);
        }
        $this->partitions = $packedPartitions;
    }

    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => OffsetCommitRequestPartition::class],
        ];
    }
}

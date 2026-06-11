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
 * OffsetFetch/OffsetCommit DTO
 */
class OffsetFetchResponseTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     *
     * @var string
     */
    public $topic;

    /**
     * Information about each offset for the partition in the topic
     *
     * @var OffsetFetchResponsePartition[]
     */
    public $partitions;


    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => OffsetFetchResponsePartition::class],
        ];
    }
}

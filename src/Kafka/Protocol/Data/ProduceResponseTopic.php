<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Produce response Topic DTO
 */
class ProduceResponseTopic implements BinarySchemaInterface
{
    /**
     * The name of the topic
     *
     * @var string
     */
    public $topic;

    /**
     * Data for all partitions in the topic
     *
     * @var ProduceResponsePartition[]
     */
    public $partitions = [];

    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => ProduceResponsePartition::class],
        ];
    }
}

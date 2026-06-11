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
 * Produce request Topic DTO
 */
class ProduceRequestTopic implements BinarySchemaInterface
{
    /**
     * Data for all partitions in the topic
     *
     * @var ProduceRequestPartition[]
     */
    public $partitions = [];

    /**
     * @inheritDoc
     * @param string $topic
     */
    public function __construct(/**
     * The name of the topic to produce to
     */
        public $topic = '',
        array $partitionData = []
    ) {
        $this->partitions = $partitionData;
    }

    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => ['partition' => ProduceRequestPartition::class],
        ];
    }
}

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
 * Generic topic-partitions structure
 *
 * PartitionsForTopic => [Topic [Partition]]
 *   Topic => string
 *   Partition => int32
 */
class PartitionsForTopic implements BinarySchemaInterface
{
    /**
     * List of partitions from the topic to assign
     *
     * @var array
     */
    public $partitions = [];

    /**
     * @inheritDoc
     * @param string $topic
     */
    public function __construct(/**
     * Name of the topic to assign
     */
        public $topic,
        array $partitions
    ) {
        $this->partitions = $partitions;
    }

    /**
     * Returns definition of binary packet for the class or object
     *
     * @return array
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}

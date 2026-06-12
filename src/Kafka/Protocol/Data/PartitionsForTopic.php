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
 * Generic topic-partitions structure
 *
 * PartitionsForTopic => [Topic [Partition]]
 *   Topic => string
 *   Partition => int32
 */
class PartitionsForTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic to assign
     * @var string
     */
    public $topic;

    /**
     * List of partitions from the topic to assign
     *
     * @var integer[]
     */
    public $partitions = [];

    /**
     * @inheritDoc
     */
    public function __construct(string $topic, array $partitions)
    {
        $this->topic      = $topic;
        $this->partitions = $partitions;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'topic'      => BinarySchema::TYPE_STRING,
            'partitions' => [BinarySchema::TYPE_INT32],
        ];
    }
}

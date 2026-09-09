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
 * Generic topic-partitions structure
 *
 * <pre>
 *   PartitionsForTopic => Topic [Partition]
 *     Topic     => string
 *     Partition => int32
 * </pre>
 */
class PartitionsForTopic implements BinarySchemaInterface
{
    /**
     * Name of the topic
     */
    public string $topic;

    /**
     * List of partitions of that topic
     *
     * @var list<int>
     */
    public array $partitions = [];

    /**
     * @param string    $topic      Name of the topic
     * @param list<int> $partitions List of partitions of that topic
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

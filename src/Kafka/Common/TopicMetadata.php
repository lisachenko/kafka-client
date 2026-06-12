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

namespace Protocol\Kafka\Common;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Topic metadata DTO
 */
class TopicMetadata implements BinarySchemaInterface
{
    use RestorableTrait;

    /**
     * The error code for the given topic.
     *
     * @var integer
     */
    public $topicErrorCode;

    /**
     * The name of the topic
     *
     * @var string
     */
    public $topic;

    /**
     * Indicates if the topic is considered a Kafka internal topic
     *
     * @var boolean
     * @since Version 1 of protocol
     */
    public $isInternal;

    /**
     * Metadata for each partition of the topic.
     *
     * @var PartitionMetadata[]
     */
    public $partitions = [];

    public static function getScheme(): array
    {
        return [
            'topicErrorCode' => BinarySchema::TYPE_INT16,
            'topic'          => BinarySchema::TYPE_STRING,
            'isInternal'     => BinarySchema::TYPE_INT8,
            'partitions'     => [PartitionMetadata::class],
        ];
    }
}

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

namespace Protocol\Kafka\Common;

use Protocol\Kafka\IO\Stream;

/**
 * Topic metadata DTO
 */
class TopicMetadata
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
     * @var PartitionMetadata[]|array
     */
    public $partitions = [];

    /**
     * Unpacks the DTO from the binary buffer
     *
     * @param Stream $stream Binary buffer
     *
     * @return static
     */
    public static function unpack(Stream $stream): static
    {
        $topic = new static();
        [$topic->topicErrorCode, $topicLength] = array_values($stream->read('ntopicErrorCode/ntopicLength'));
        [$topic->topic, $topic->isInternal, $numberOfPartitions] = array_values($stream->read("a{$topicLength}topic/cisInternal/NnumberOfPartition"));

        for ($partition = 0; $partition < $numberOfPartitions; $partition++) {
            $partitionMetadata = PartitionMetadata::unpack($stream);

            $topic->partitions[$partitionMetadata->partitionId] = $partitionMetadata;
        }

        return $topic;
    }
}

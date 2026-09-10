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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ControlledShutdownResponsePartition;

/**
 * ControlledShutdown response object
 *
 * <pre>
 *   ControlledShutdownResponse => ErrorCode [TopicName Partition]
 *     ErrorCode => int16
 *     TopicName => string
 *     Partition => int32
 * </pre>
 *
 * The array holds the topic-partitions that still have a leader or a replica on the broker after the controller has
 * done what it could; an empty array together with the error code 0 means the broker may now be stopped.
 *
 * Both versions of the request share this response: `ControlledShutdownResponse` @ 0.10.2.2 has no version of its own.
 *
 * @see docs/protocol/1.1.md, section "ControlledShutdown API (key 7, v0 and v1)"
 */
class ControlledShutdownResponse extends AbstractResponse
{
    /**
     * Error code of the whole request
     */
    public int $errorCode = 0;

    /**
     * Topic-partitions that could not be moved off the broker
     *
     * @var list<ControlledShutdownResponsePartition>
     */
    public array $remainingTopicPartitions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode' => BinarySchema::TYPE_INT16,
            // A flat list of pairs, not partitions grouped by topic, so it cannot be indexed by the topic name
            'remainingTopicPartitions' => [ControlledShutdownResponsePartition::class],
        ];
    }
}

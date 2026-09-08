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

use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;

/**
 * Offsets (ListOffset) response object (key 2, v0)
 *
 * <pre>
 *   OffsetResponse => [TopicName [Partition ErrorCode [Offset]]]
 *     TopicName => string
 *     Partition => int32
 *     ErrorCode => int16
 *     Offset    => int64
 * </pre>
 *
 * The `Timestamp` field and the single offset of v1 (Kafka 0.10.1) do not exist here: v0 answers with the list of the
 * segment offsets that match the requested time.
 *
 * @see docs/protocol/0.8.2.md, section "Offsets API (key 2, v0), a.k.a. ListOffset"
 */
class OffsetsResponse extends AbstractResponse
{
    /**
     * Offsets for each of the requested topics, indexed by the topic name
     *
     * @var array<string, OffsetsResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => OffsetsResponseTopic::class],
        ];
    }
}

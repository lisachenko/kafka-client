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

use Protocol\Kafka\Protocol\Data\FetchResponseTopic;

/**
 * Fetch response object (key 1, v0)
 *
 * <pre>
 *   FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
 *     TopicName           => string
 *     Partition           => int32
 *     ErrorCode           => int16
 *     HighwaterMarkOffset => int64
 *     MessageSetSize      => int32
 * </pre>
 *
 * The `ThrottleTime` prefix of the response arrived with v1 (Kafka 0.9) and does not exist here.
 *
 * @see docs/protocol/0.9.0.md, section "Fetch API (key 1, v0)"
 */
class FetchResponse extends AbstractResponse
{
    /**
     * Fetch result for each of the requested topics, indexed by the topic name
     *
     * @var array<string, FetchResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => FetchResponseTopic::class],
        ];
    }
}

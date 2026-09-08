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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\Data\ProduceResponseTopic;

/**
 * Produce response object
 *
 * <pre>
 *   ProduceResponse => [TopicName [Partition ErrorCode Offset]]
 * </pre>
 *
 * The `ThrottleTime` of the later protocol lines arrived with version 1 of this API (Kafka 0.9.0).
 *
 * A request with `RequiredAcks = 0` is never answered at all, see {@see ProduceRequest::expectsResponse()}.
 *
 * @see docs/protocol/0.8.2.md, section "Produce API (key 0, v0)"
 */
class ProduceResponse extends AbstractResponse
{
    /**
     * Result for each topic of the request, indexed by the topic name
     *
     * @var array<string, ProduceResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => ProduceResponseTopic::class],
        ];
    }
}

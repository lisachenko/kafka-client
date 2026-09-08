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

use Protocol\Kafka\Protocol\Data\OffsetFetchResponseTopic;

/**
 * OffsetFetch response object
 *
 * <pre>
 *   OffsetFetch Response (Version: 0 and 1) => [responses]
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition offset metadata error_code
 *         partition  => INT32
 *         offset     => INT64
 *         metadata   => NULLABLE_STRING
 *         error_code => INT16
 * </pre>
 *
 * The top-level `error_code` of the version 2 response belongs to Kafka 0.9: in 0.8.2.2 every error is reported per
 * topic-partition.
 *
 * @see docs/protocol/0.8.2.md, section "OffsetFetch API (key 9, v0 and v1)"
 */
class OffsetFetchResponse extends AbstractResponse
{
    /**
     * List of topic responses
     *
     * @var array<string, OffsetFetchResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => OffsetFetchResponseTopic::class],
        ];
    }
}

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

use Protocol\Kafka\Protocol\Data\OffsetCommitResponseTopic;

/**
 * Offset commit response object
 *
 * <pre>
 *   OffsetCommit Response (Version: 0 and 1) => [responses]
 *     responses => topic [partition_responses]
 *       topic               => STRING
 *       partition_responses => partition error_code
 *         partition  => INT32
 *         error_code => INT16
 * </pre>
 *
 * @see docs/protocol/0.9.0.md, section "OffsetCommit API (key 8, v0 and v1)"
 */
class OffsetCommitResponse extends AbstractResponse
{
    /**
     * List of topics with the result for each of their partitions
     *
     * @var array<string, OffsetCommitResponseTopic>
     */
    public array $topics = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => OffsetCommitResponseTopic::class],
        ];
    }
}

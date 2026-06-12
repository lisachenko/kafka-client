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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\Data\OffsetCommitResponseTopic;

/**
 * Offset commit response object
 *
 * OffsetCommit Response (Version: 2) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code
 *       partition => INT32
 *       error_code => INT16
 */
class OffsetCommitResponse extends AbstractResponse
{
    /**
     * List of topics with partition result
     *
     * @var OffsetCommitResponseTopic[]
     */
    public $topics = [];

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => OffsetCommitResponseTopic::class],
        ];
    }
}

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
 * @date 15.07.2016
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\Data\OffsetsResponseTopic;

/**
 * ListOffset response object
 *
 * ListOffsets Response (Version: 1) => [responses]
 *   responses => topic [partition_responses]
 *     topic => STRING
 *     partition_responses => partition error_code timestamp offset
 *       partition => INT32
 *       error_code => INT16
 *       timestamp => INT64
 *       offset => INT64
 */
class OffsetsResponse extends AbstractResponse
{
    /**
     * List of broker metadata info
     *
     * @var OffsetsResponseTopic[]
     */
    public $topics = [];

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'topics' => ['topic' => OffsetsResponseTopic::class],
        ];
    }
}

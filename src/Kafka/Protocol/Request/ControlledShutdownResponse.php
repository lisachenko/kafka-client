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
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\Data\ControlledShutdownResponsePartition;

/**
 * Controlled shutdown response
 *
 * ControlledShutdown Response (Version: 0) => error_code [partitions_remaining]
 *   error_code => INT16
 *   partitions_remaining => topic partition
 *     topic => STRING
 *     partition => INT32
 */
class ControlledShutdownResponse extends AbstractResponse implements BinarySchemaInterface
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * The topic partitions that the broker still leads.
     *
     * @var ControlledShutdownResponsePartition[]
     */
    public $remainingTopicPartitions = [];

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'                => BinarySchema::TYPE_INT16,
            'remainingTopicPartitions' => [ControlledShutdownResponsePartition::class],
        ];
    }
}

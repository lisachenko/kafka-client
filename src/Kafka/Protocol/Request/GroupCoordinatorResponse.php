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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;
use Protocol\Kafka\Protocol\Data\GroupCoordinatorResponseMetadata;

/**
 * Group coordinator response
 */
class GroupCoordinatorResponse extends AbstractResponse implements BinarySchemaInterface
{
    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Host and port information for the coordinator for a consumer group.
     *
     * @var GroupCoordinatorResponseMetadata
     */
    public $coordinator;

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'   => BinarySchema::TYPE_INT16,
            'coordinator' => GroupCoordinatorResponseMetadata::class,
        ];
    }
}

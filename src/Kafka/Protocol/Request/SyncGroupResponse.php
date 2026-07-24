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

use Protocol\Kafka\Protocol\BinarySchema;

/**
 * Sync group response
 *
 * SyncGroup Response (Version: 1) => throttle_time_ms error_code member_assignment
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   member_assignment => BYTES
 */
class SyncGroupResponse extends AbstractResponse
{
    /**
     * Duration in milliseconds for which the request was throttled due to quota violation
     *
     * (Zero if the request did not violate any quota)
     *
     * @var integer
     */
    public $throttleTimeMs;

    /**
     * Error code.
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Assigned data to the member
     *
     * @todo This should be implemented on scheme-level as MemberAssignment
     * @var string
     */
    public $memberAssignment;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs'   => BinarySchema::TYPE_INT32,
            'errorCode'        => BinarySchema::TYPE_INT16,
            'memberAssignment' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}

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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * DescribeGroupResponseMember metadata DTO
 *
 * DescribeGroupResponseMember => member_id client_id client_host member_metadata member_assignment
 *   member_id => STRING
 *   client_id => STRING
 *   client_host => STRING
 *   member_metadata => BYTES
 *   member_assignment => BYTES
 */
class DescribeGroupResponseMember implements BinarySchemaInterface
{
    /**
     * 	The memberId assigned by the coordinator
     *
     * @var string
     */
    public $memberId;

    /**
     * The client id used in the member's latest join group request
     *
     * @var string
     */
    public $clientId;

    /**
     * The client host used in the request session corresponding to the member's join group.
     *
     * @var string
     */
    public $clientHost;

    /**
     * The metadata corresponding to the current group protocol in use (will only be present if the group is stable).
     *
     * @var string Binary data
     */
    public $memberMetadata;

    /**
     * The current assignment provided by the group leader (will only be present if the group is stable).
     *
     * @var string
     */
    public $memberAssignment;

    /**
     * Returns definition of binary packet for the class or object
     *
     * @return array
     */
    public static function getScheme(): array
    {
        return [
            'memberId'         => BinarySchema::TYPE_STRING,
            'clientId'         => BinarySchema::TYPE_STRING,
            'clientHost'       => BinarySchema::TYPE_STRING,
            'memberMetadata'   => BinarySchema::TYPE_BYTEARRAY,
            'memberAssignment' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}

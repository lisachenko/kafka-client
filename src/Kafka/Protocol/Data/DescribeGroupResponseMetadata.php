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
 * DescribeGroup metadata DTO
 *
 * DescibeGroupMetadata => error_code group_id state protocol_type protocol [members]
 *   error_code => INT16
 *   group_id => STRING
 *   state => STRING
 *   protocol_type => STRING
 *   protocol => STRING
 *   members => member_id client_id client_host member_metadata member_assignment
 */
class DescribeGroupResponseMetadata implements BinarySchemaInterface
{
    /**
     * Error code for the group
     *
     * @var integer
     */
    public $errorCode;

    /**
     * Name of the group
     *
     * @var string
     */
    public $groupId;

    /**
     * The current state of the group
     * (one of: Dead, Stable, AwaitingSync, or PreparingRebalance, or empty if there is no active group)
     *
     * @var string
     */
    public $state;

    /**
     * The current group protocol type (will be empty if there is no active group)
     *
     * @var string
     */
    public $protocolType;

    /**
     * The current group protocol (only provided if the group is Stable)
     *
     * @var string
     */
    public $protocol;

    /**
     * Current group members (only provided if the group is not Dead)
     *
     * @var array
     */
    public $members = [];

    /**
     * Returns definition of binary packet for the class or object
     *
     * @return array
     */
    public static function getScheme(): array
    {
        return [
            'errorCode'    => BinarySchema::TYPE_INT16,
            'groupId'      => BinarySchema::TYPE_STRING,
            'state'        => BinarySchema::TYPE_STRING,
            'protocolType' => BinarySchema::TYPE_STRING,
            'protocol'     => BinarySchema::TYPE_STRING,
            'members'      => ['memberId' => DescribeGroupResponseMember::class],
        ];
    }
}

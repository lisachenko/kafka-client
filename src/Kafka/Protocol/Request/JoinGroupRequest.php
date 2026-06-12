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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\JoinGroupRequestProtocol;

/**
 * Join Group Request
 *
 * The join group request is used by a client to become a member of a group. When new members join an existing group,
 * all previous members are required to rejoin by sending a new join group request. When a member first joins the
 * group, the memberId will be empty (i.e. ""), but a rejoining member should use the same memberId from the previous
 * generation.
 */
class JoinGroupRequest extends AbstractRequest
{
    /**
     * @inheritDoc
     */
    public const VERSION = 1;

    /**
     * Member id for self-assigned consumer
     */
    public const DEFAULT_MEMBER_ID = "";

    /**
     * List of protocols that the member supports as key=>value pairs, where value is metadata
     */
    private readonly array $groupProtocols;

    /**
     * @param string $consumerGroup
     * @param int $sessionTimeout
     * @param int $rebalanceTimeout
     * @param string $memberId
     * @param string $protocolType
     */
    public function __construct(
        /**
         * The consumer group id.
         */
        private $consumerGroup,
        /**
         * The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.
         */
        private $sessionTimeout,
        /**
         * The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group
         */
        private $rebalanceTimeout,
        /**
         * The member id assigned by the group coordinator.
         */
        private $memberId,
        /**
         * Unique name for class of protocols implemented by group
         */
        private $protocolType,
        array $groupProtocols,
        $clientId = '',
        $correlationId = 0
    ) {
        $packedProtocols        = [];
        foreach ($groupProtocols as $protocolName => $protocolMetadata) {
            $packedProtocols[$protocolName] = new JoinGroupRequestProtocol($protocolName, $protocolMetadata);
        }

        $this->groupProtocols = $packedProtocols;

        parent::__construct(ApiKeys::JOIN_GROUP, $clientId, $correlationId);
    }

    public static function getScheme(): array
    {
        $header = null;

        return $header + [
            'consumerGroup'    => BinarySchema::TYPE_STRING,
            'sessionTimeout'   => BinarySchema::TYPE_INT32,
            'rebalanceTimeout' => BinarySchema::TYPE_INT32,
            'memberId'         => BinarySchema::TYPE_STRING,
            'protocolType'     => BinarySchema::TYPE_STRING,
            'groupProtocols'   => ['protocolName' => JoinGroupRequestProtocol::class],
        ];
    }
}

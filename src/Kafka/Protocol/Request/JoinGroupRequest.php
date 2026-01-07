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

use Protocol\Kafka\Protocol\ApiKeys;

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
     * @param string $consumerGroup
     * @param int $sessionTimeout
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
         * The member id assigned by the group coordinator.
         */
        private $memberId,
        /**
         * Unique name for class of protocols implemented by group
         */
        private $protocolType,
        /**
         * List of protocols that the member supports as key=>value pairs, where value is metadata
         */
        private readonly array $groupProtocols,
        $correlationId = 0,
        $clientId = ''
    ) {
        parent::__construct(ApiKeys::JOIN_GROUP, $correlationId, $clientId);

    }

    /**
     * @inheritDoc
     *
     * JoinGroup Request (Version: 0) => group_id session_timeout member_id protocol_type [group_protocols]
     *   group_id => STRING
     *   session_timeout => INT32
     *   member_id => STRING
     *   protocol_type => STRING
     *   group_protocols => protocol_name protocol_metadata
     *     protocol_name => STRING
     *     protocol_metadata => BYTES
     */
    protected function packPayload(): string
    {
        $payload        = parent::packPayload();
        $groupLength    = strlen($this->consumerGroup);
        $memberLength   = strlen($this->memberId);
        $protocolLength = strlen($this->protocolType);

        $payload .= pack(
            "na{$groupLength}Nna{$memberLength}na{$protocolLength}N",
            $groupLength,
            $this->consumerGroup,
            $this->sessionTimeout,
            $memberLength,
            $this->memberId,
            $protocolLength,
            $this->protocolType,
            count($this->groupProtocols)
        );

        foreach ($this->groupProtocols as $protocolName => $protocolMetadata) {
            $protocolNameLength = strlen($protocolName);
            $protocolMetaLength = strlen($protocolMetadata);
            $payload .= pack("na{$protocolNameLength}N", $protocolNameLength, $protocolName, $protocolMetaLength);
            $payload .= $protocolMetadata;
        }

        return $payload;
    }
}

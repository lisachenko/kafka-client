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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One protocol that a member of a group supports, as it is advertised by a JoinGroup request.
 *
 * <pre>
 *   JoinGroupRequestProtocol => ProtocolName ProtocolMetadata
 *     ProtocolName     => string
 *     ProtocolMetadata => bytes
 * </pre>
 *
 * The metadata is opaque to this class and to the coordinator alike: the broker stores the bytes, hands them to the
 * leader of the group in the JoinGroup response and never looks inside. What they mean is decided by the
 * `protocol_type` of the request - for `consumer` it is the `Subscription` structure of the consumer protocol.
 *
 * @see docs/protocol/0.10.2.md, section "JoinGroup API (key 11, v0 and v1)"
 */
class JoinGroupRequestProtocol implements BinarySchemaInterface
{
    /**
     * Name of the protocol, e.g. `range` or `roundrobin` for the `consumer` protocol type.
     */
    public string $name;

    /**
     * Protocol-specific metadata of this member, opaque to the coordinator.
     */
    public string $metadata;

    public function __construct(string $name, string $metadata)
    {
        $this->name     = $name;
        $this->metadata = $metadata;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'     => BinarySchema::TYPE_STRING,
            'metadata' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }
}

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

use Protocol\Kafka\Consumer\Subscription;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Join group request protocol DTO
 *
 * JoinGroupRequestProtocol => protocol_name protocol_metadata
 *   protocol_name => STRING
 *   protocol_metadata => BYTES
 */
class JoinGroupRequestProtocol implements BinarySchemaInterface
{
    /**
     * Name of the protocol
     *
     * @var string
     */
    public $name;

    /**
     * Protocol-specific metadata
     *
     * @todo Update scheme to use Subscription instance directly
     * @var string
     */
    public $metadata;

    public function __construct(string $name, Subscription $subscription)
    {
        // TODO: This should be on scheme-level
        $stringStream = new StringStream();
        BinarySchema::writeObjectToStream($subscription, $stringStream);

        $this->name     = $name;
        $this->metadata = $stringStream->getBuffer();
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

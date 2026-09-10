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

namespace Protocol\Kafka\Consumer;

use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * Member metadata that a consumer publishes to the group coordinator, the `protocol_type = "consumer"` payload of
 * the JoinGroup request.
 *
 * <pre>
 *   Subscription => Version [Topic] UserData
 *     Version  => int16
 *     Topic    => string
 *     UserData => bytes
 * </pre>
 *
 * The structure travels as an opaque byte array in the `member_metadata` field of JoinGroup and comes back to the
 * leader of the group in the `members` array of the JoinGroup response, which is where the assignor reads it.
 * Kafka 0.10.2.2 knows exactly one version of it, {@see Subscription::VERSION}; the Java client of 0.10 parses a
 * higher version with the layout of version 0, so new versions may only append fields.
 *
 * The `UserData` is what a custom assignor forwards to the leader - a rack id, the number of cpus of the machine,
 * the assignment of the previous generation for a sticky assignor. The built-in assignors keep no state and send
 * the empty byte array that the Java client sends, `PartitionAssignor.Subscription` defaulting the field to
 * `ByteBuffer.wrap(new byte[0])`.
 *
 * @see docs/protocol/1.1.md, section "Consumer group protocol (protocol_type = consumer)"
 * @see \Protocol\Kafka\Consumer\PartitionAssignorInterface::subscription()
 */
class Subscription implements BinarySchemaInterface
{
    /**
     * Version of the consumer group protocol that Kafka 0.10.2.2 speaks
     */
    public const int VERSION = 0;

    /**
     * Version of the structure, `ConsumerProtocol.CONSUMER_PROTOCOL_V0` in the Java client
     */
    public int $version;

    /**
     * Topics that the member wants to consume
     *
     * @var list<string>
     */
    public array $topics;

    /**
     * Opaque data of the assignor, null for the `bytes` value -1 that the protocol defines as null
     */
    public ?string $userData;

    /**
     * @param list<string> $topics   Topics that the member subscribes to
     * @param int          $version  Version of the structure, 0 in Kafka 0.10.2.2
     * @param string|null  $userData Data that the assignor of the leader receives together with the topics
     */
    public function __construct(array $topics, int $version = self::VERSION, ?string $userData = '')
    {
        $this->topics   = $topics;
        $this->version  = $version;
        $this->userData = $userData;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'version'  => BinarySchema::TYPE_INT16,
            'topics'   => [BinarySchema::TYPE_STRING],
            'userData' => BinarySchema::TYPE_BYTEARRAY,
        ];
    }

    /**
     * Returns the binary representation of this structure, the `member_metadata` of a JoinGroup request
     */
    public function pack(): string
    {
        $stream = new StringStream();
        BinarySchema::writeObjectToStream($this, $stream);

        return $stream->getBuffer();
    }

    /**
     * Restores the structure from the `member_metadata` bytes of a JoinGroup response
     *
     * @param string $bytes Content of the byte array field, without its length prefix
     */
    public static function unpack(string $bytes): static
    {
        /** @var static $subscription */
        $subscription = BinarySchema::readObjectFromStream(static::class, new StringStream($bytes));

        return $subscription;
    }
}

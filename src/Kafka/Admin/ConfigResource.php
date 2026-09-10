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

namespace Protocol\Kafka\Admin;

use InvalidArgumentException;

/**
 * A thing whose configuration the DescribeConfigs and AlterConfigs apis can read or replace: a topic or a broker
 *
 * The pair is `org.apache.kafka.common.requests.Resource` @ 0.11.0.3, whose type ids come from the enum
 * `ResourceType` of the same package - `UNKNOWN(0), ANY(1), TOPIC(2), GROUP(3), BROKER(4)`. The class carries the
 * name the later Java admin client gave it (`org.apache.kafka.common.config.ConfigResource`, Kafka 1.0), because
 * that is the identifier of the concept on the `main` branch; the ids are the ones of 0.11 and are not the ids of
 * the ACL `ResourceType` of the same release.
 *
 * <code>
 *   $admin->describeConfigs([ConfigResource::topic('events'), ConfigResource::broker(0)]);
 * </code>
 *
 * A resource is identified by its type AND its name - the topic `0` and the broker `0` are two different things -
 * so {@see self::key()} is what a result of {@see AdminClient::describeConfigs()} is indexed by, and what
 * {@see AdminClient::alterConfigs()} takes as the key of its argument.
 *
 * @see docs/protocol/1.1.md, sections "DescribeConfigs API (key 32, v0)" and "AlterConfigs API (key 33, v0)"
 */
final class ConfigResource
{
    /**
     * A resource type this client does not know, `ResourceType.UNKNOWN` @ 0.11.0.3
     */
    public const int TYPE_UNKNOWN = 0;

    /**
     * Every resource type, `ResourceType.ANY` @ 0.11.0.3; not accepted by the config apis
     */
    public const int TYPE_ANY = 1;

    /**
     * A topic, `ResourceType.TOPIC` @ 0.11.0.3; the name is the topic name
     */
    public const int TYPE_TOPIC = 2;

    /**
     * A consumer group, `ResourceType.GROUP` @ 0.11.0.3; not accepted by the config apis
     */
    public const int TYPE_GROUP = 3;

    /**
     * A broker, `ResourceType.BROKER` @ 0.11.0.3; the name is the broker id as a decimal string
     */
    public const int TYPE_BROKER = 4;

    /**
     * Name of every type id, used by {@see self::key()} and {@see self::fromKey()}
     *
     * @var array<int, string>
     */
    private const array TYPE_NAMES = [
        self::TYPE_UNKNOWN => 'unknown',
        self::TYPE_ANY     => 'any',
        self::TYPE_TOPIC   => 'topic',
        self::TYPE_GROUP   => 'group',
        self::TYPE_BROKER  => 'broker',
    ];

    /**
     * @param int    $type Type of the resource, one of the `TYPE_*` constants
     * @param string $name Name of the resource: the topic name, or the broker id as a decimal string
     */
    public function __construct(public readonly int $type, public readonly string $name) {}

    /**
     * Names the topic of the given name
     */
    public static function topic(string $topic): self
    {
        return new self(self::TYPE_TOPIC, $topic);
    }

    /**
     * Names the broker of the given node id
     *
     * The name of a broker resource travels as text and has to parse as an integer: a 0.11.0.3 broker answers
     * anything else with the error code 42 and the message `Broker id must be an integer, but it is: …`.
     */
    public static function broker(int|string $brokerId): self
    {
        return new self(self::TYPE_BROKER, (string) $brokerId);
    }

    /**
     * Returns the key this resource is addressed by in the arguments and results of {@see AdminClient}
     *
     * PHP cannot use an object as an array key, so a map of resources is keyed by this string: the lower-case name
     * of the type, a colon and the name of the resource - `topic:events`, `broker:0`.
     */
    public function key(): string
    {
        return self::TYPE_NAMES[$this->type] . ':' . $this->name;
    }

    /**
     * Rebuilds the resource that {@see self::key()} produced
     *
     * @throws InvalidArgumentException If the key does not name a known resource type
     */
    public static function fromKey(string $key): self
    {
        $separator = strpos($key, ':');
        if ($separator === false) {
            throw new InvalidArgumentException("The resource key '{$key}' does not name a type");
        }

        $type = array_search(substr($key, 0, $separator), self::TYPE_NAMES, true);
        if ($type === false) {
            throw new InvalidArgumentException("The resource key '{$key}' does not name a known resource type");
        }

        return new self($type, substr($key, $separator + 1));
    }

    /**
     * Returns the resource of the given type id and name, as they travel on the wire
     */
    public static function fromWire(int $resourceType, string $resourceName): self
    {
        return new self(
            isset(self::TYPE_NAMES[$resourceType]) ? $resourceType : self::TYPE_UNKNOWN,
            $resourceName
        );
    }

    public function __toString(): string
    {
        return $this->key();
    }
}

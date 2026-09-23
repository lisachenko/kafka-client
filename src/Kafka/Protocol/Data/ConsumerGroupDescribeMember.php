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
 * One member of a group in a ConsumerGroupDescribe answer (key 69, Kafka 3.7, KIP-848)
 *
 * <pre>
 *   Member => member_id instance_id rack_id member_epoch client_id client_host [subscribed_topic_names]
 *             subscribed_topic_regex assignment target_assignment
 *     member_id              => COMPACT_STRING
 *     instance_id            => COMPACT_NULLABLE_STRING
 *     rack_id                => COMPACT_NULLABLE_STRING
 *     member_epoch           => INT32
 *     client_id              => COMPACT_STRING
 *     client_host            => COMPACT_STRING
 *     subscribed_topic_names => COMPACT_STRING
 *     subscribed_topic_regex => COMPACT_NULLABLE_STRING
 *     assignment             => [topic_partitions]
 *     target_assignment      => [topic_partitions]
 * </pre>
 *
 * The `Member` structure of `ConsumerGroupDescribeResponse.json` @ 3.9.2. It reports what
 * {@see DescribeGroupResponseMember} of the classic api (key 15) has no field for: the **member epoch** in place
 * of a generation, the **subscription** of every member as plain topic names - the classic answer carries the
 * packed `metadata` byte array of the assignor instead, which a client has to decode itself - and the **pair of
 * assignments**, the one the member owns and the one it is meant to own.
 *
 * `subscribed_topic_regex` is the `subscribe(Pattern)` of KIP-848: a group whose members subscribe by pattern is
 * resolved by the **coordinator**, not by the client, which is the other half of the assignment moving to the
 * broker. This client subscribes by name, so the field is null in everything it produces.
 *
 * @see \Protocol\Kafka\Protocol\Request\ConsumerGroupDescribeResponse
 * @see docs/protocol/3.9.md, section "ConsumerGroupDescribe API (key 69, v0)"
 */
class ConsumerGroupDescribeMember implements BinarySchemaInterface
{
    /**
     * Topics this member subscribed to by name
     *
     * @var list<string>
     */
    public array $subscribedTopicNames = [];

    /**
     * Member id of this member, the uuid it generated for itself
     */
    public string $memberId = '';

    /**
     * `group.instance.id` of a static member, null for a dynamic one
     */
    public ?string $instanceId = null;

    /**
     * `client.rack` of the member (KIP-881), null when it named none
     */
    public ?string $rackId = null;

    /**
     * Current epoch of this member
     */
    public int $memberEpoch = 0;

    /**
     * `client.id` of the consumer behind this member
     */
    public string $clientId = '';

    /**
     * Host the member connected from, as `/address`
     */
    public string $clientHost = '';

    /**
     * Pattern this member subscribed with (KIP-848), null when it named its topics
     */
    public ?string $subscribedTopicRegex = null;

    /**
     * Partitions this member owns right now
     */
    public ConsumerGroupDescribeAssignment $assignment;

    /**
     * Partitions the coordinator wants this member to own; equal to the assignment once the group is settled
     */
    public ConsumerGroupDescribeAssignment $targetAssignment;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId'             => BinarySchema::TYPE_STRING,
            'instanceId'           => BinarySchema::TYPE_NULLABLE_STRING,
            'rackId'               => BinarySchema::TYPE_NULLABLE_STRING,
            'memberEpoch'          => BinarySchema::TYPE_INT32,
            'clientId'             => BinarySchema::TYPE_STRING,
            'clientHost'           => BinarySchema::TYPE_STRING,
            'subscribedTopicNames' => [BinarySchema::TYPE_STRING],
            'subscribedTopicRegex' => BinarySchema::TYPE_NULLABLE_STRING,
            'assignment'           => ConsumerGroupDescribeAssignment::class,
            'targetAssignment'     => ConsumerGroupDescribeAssignment::class,
        ];
    }
}

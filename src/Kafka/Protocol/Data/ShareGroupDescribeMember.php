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
 * One member of a group in a ShareGroupDescribe answer (key 77, Kafka 4.1, KIP-932)
 *
 * <pre>
 *   Member => member_id rack_id member_epoch client_id client_host [subscribed_topic_names] assignment
 *     member_id              => COMPACT_STRING
 *     rack_id                => COMPACT_NULLABLE_STRING
 *     member_epoch           => INT32
 *     client_id              => COMPACT_STRING
 *     client_host            => COMPACT_STRING
 *     subscribed_topic_names => COMPACT_STRING
 *     assignment             => [topic_partitions]
 * </pre>
 *
 * The `Member` structure of `ShareGroupDescribeResponse.json` @ 4.1.0: the member of a consumer group without what a
 * share member has no use for - no instance id (share groups have no static members), no regex, no target
 * assignment, no member type.
 *
 * @see docs/protocol/4.3.md, section "ShareGroupDescribe API (key 77, v1)"
 */
final class ShareGroupDescribeMember implements BinarySchemaInterface
{
    /**
     * Member id, the uuid the member generated for itself
     */
    public string $memberId = '';

    /**
     * `client.rack` of the member, null when it named none
     */
    public ?string $rackId = null;

    /**
     * Current epoch of the member
     */
    public int $memberEpoch = 0;

    /**
     * `client.id` of the consumer behind the member
     */
    public string $clientId = '';

    /**
     * Host the member connected from, as `/address`
     */
    public string $clientHost = '';

    /**
     * Topics the member subscribed to
     *
     * @var list<string>
     */
    public array $subscribedTopicNames = [];

    /**
     * Partitions the member may fetch
     */
    public ShareGroupDescribeAssignment $assignment;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'memberId'             => BinarySchema::TYPE_STRING,
            'rackId'               => BinarySchema::TYPE_NULLABLE_STRING,
            'memberEpoch'          => BinarySchema::TYPE_INT32,
            'clientId'             => BinarySchema::TYPE_STRING,
            'clientHost'           => BinarySchema::TYPE_STRING,
            'subscribedTopicNames' => [BinarySchema::TYPE_STRING],
            'assignment'           => ShareGroupDescribeAssignment::class,
        ];
    }
}

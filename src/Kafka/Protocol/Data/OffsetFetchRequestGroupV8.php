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

/**
 * One group of an OffsetFetch request of version 8 (Kafka 3.0): the entry before KIP-848 gave it a member
 *
 * Version 9 (Kafka 3.7) put a nullable `MemberId` and a `MemberEpoch` between the group id and the topic array
 * of every entry, so that a member of a KIP-848 group can name itself in the request; this version has the two
 * fields the version 8 introduced and nothing else. {@see OffsetFetchRequestGroup} is the entry of version 9, and
 * a member id or an epoch carried in an entry of *this* class never reaches the wire - version 8 has no place
 * for them.
 *
 * @see docs/protocol/3.9.md, section "OffsetFetch API (key 9, v0 to v9)"
 * @see docs/protocol/3.9.md, section "The member id and epoch of KIP-848 (v9)"
 */
final class OffsetFetchRequestGroupV8 extends OffsetFetchRequestGroup
{
    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'groupId'                   => BinarySchema::TYPE_STRING,
            'topicPartitions'           => [
                'topic'                     => PartitionsForTopic::class,
                BinarySchema::FLAG_NULLABLE => true,
            ],
        ];
    }
}

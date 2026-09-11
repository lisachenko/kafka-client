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

/**
 * Description of a single group as the DescribeGroups answer of version 3 reports it
 *
 * Version 3 (KIP-430) appended the `authorized_operations` of the group, and version 4 (KIP-345) gave every
 * **member** entry a `group_instance_id`; this is the entry with the first and without the second.
 *
 * @see docs/protocol/2.8.md, section "DescribeGroups API (key 15, v0 to v5)"
 */
final class DescribeGroupResponseMetadataV3 extends DescribeGroupResponseMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}

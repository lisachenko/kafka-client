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
 * Description of a single group as the DescribeGroups answers of the versions 4 and 5 report it
 *
 * Version 4 (KIP-345) gave every **member** entry a `group_instance_id` and version 5 (KIP-482) encodes the same
 * entry with the compact types; version 6 (KIP-1043, Kafka 4.0) put a nullable `error_message` behind the error
 * code, see {@see DescribeGroupResponseMetadata}. This is the entry without it.
 *
 * @see docs/protocol/4.3.md, section "DescribeGroups API (key 15, v0 to v6)"
 */
final class DescribeGroupResponseMetadataV4 extends DescribeGroupResponseMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}

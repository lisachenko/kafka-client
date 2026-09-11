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
 * Description of a single group as the DescribeGroups answer of the versions 0 to 2 reports it
 *
 * <pre>
 *   DescribeGroupResponseMetadata => ErrorCode GroupId State ProtocolType Protocol [Members]
 * </pre>
 *
 * Version 3 (KIP-430, Kafka 2.3) appended the 32-bit `authorized_operations` bit set behind the member array,
 * see {@see DescribeGroupResponseMetadata}; this is the entry without it.
 *
 * @see docs/protocol/2.8.md, section "The authorized operations of a group (v3, KIP-430)"
 */
final class DescribeGroupResponseMetadataV0 extends DescribeGroupResponseMetadata
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}

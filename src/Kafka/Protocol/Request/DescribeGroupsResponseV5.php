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

namespace Protocol\Kafka\Protocol\Request;

/**
 * DescribeGroups response, version 5 (Kafka 2.4, KIP-482): the answer of version 4 in the flexible encoding
 *
 * Its group entries have no `error_message`: version 6 (KIP-1043, Kafka 4.0) put one behind the error code, and
 * {@see DescribeGroupsResponse} decodes it. An unknown group is the entry of the state `Dead` and the error code 0
 * here, where version 6 answers the 69 `GroupIdNotFound`.
 *
 * @see docs/protocol/4.3.md, section "DescribeGroups API (key 15, v0 to v6)"
 * @see docs/protocol/4.3.md, section "The 69 of an unknown group (v6, KIP-1043)"
 */
final class DescribeGroupsResponseV5 extends DescribeGroupsResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}

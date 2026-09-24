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
 * DescribeGroups request of version 5 (Kafka 2.4, KIP-482): the first flexible version, and the last one that
 * describes an unknown group as `Dead`
 *
 * The request of version 5 is byte for byte the one of version 6 but for the api version of its header. The two
 * differ in what the coordinator ANSWERS: a group it does not hold - one that never existed, one whose offsets
 * expired, and a group of the consumer protocol of KIP-848, which is not a classic group - is the entry of the state
 * `Dead` with the error code 0 at this version, and the **69** `GroupIdNotFound` with a message at version 6
 * (KIP-1043, Kafka 4.0), which {@see DescribeGroupsRequest} sends.
 *
 * @see docs/protocol/4.3.md, section "DescribeGroups API (key 15, v0 to v6)"
 * @see docs/protocol/4.3.md, section "The 69 of an unknown group (v6, KIP-1043)"
 */
final class DescribeGroupsRequestV5 extends DescribeGroupsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 5;
}

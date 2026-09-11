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
 * ElectLeaders request of version 1 (Kafka 2.4, KIP-460), the body of version 2 in the encoding before KIP-482
 *
 * <pre>
 *   ElectLeaders Request (Version: 1) => election_type [topic_partitions] timeout_ms
 * </pre>
 *
 * KIP-460 gave the version 1 the leading `election_type` byte and KIP-482 made the version 2 of the same release
 * the first flexible one; the fields are the same in both, so this class only lowers the version constant.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
final class ElectLeadersRequestV1 extends ElectLeadersRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}

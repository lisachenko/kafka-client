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
 * GroupCoordinator (FindCoordinator) request of version 1 (Kafka 0.11), the frame of version 2
 *
 * <pre>
 *   FindCoordinator Request (Version: 1 and 2) => coordinator_key coordinator_type
 * </pre>
 *
 * Version 2 (KIP-219, Kafka 2.0) is byte for byte the version 1 request - `FindCoordinatorRequest.json` @ 2.8.2
 * gives `Key` the versions `0+` and `KeyType` the versions `1+`, and nothing else was added before the flexible
 * version 3 - so this class only lowers the version field of the header. The answer is unchanged as well and is
 * read with {@see GroupCoordinatorResponseV1}.
 *
 * @see docs/protocol/2.8.md, sections "GroupCoordinator API (key 10, v0 to v3)" and "Quotas and throttle time"
 */
final class GroupCoordinatorRequestV1 extends GroupCoordinatorRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}

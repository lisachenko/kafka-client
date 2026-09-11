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

use Protocol\Kafka\Admin\ElectionType;

/**
 * ElectLeaders request of version 0 (Kafka 2.2), the frame of version 1 WITHOUT the election type
 *
 * <pre>
 *   ElectLeaders Request (Version: 0) => [topic_partitions] timeout_ms
 * </pre>
 *
 * The api arrived as **ElectPreferredLeaders** (KIP-183) and could ask for one kind of election only, so its
 * version 0 has no field for it: `ElectLeadersRequest.electionType()` @ 2.8.2 answers
 * `ElectionType.PREFERRED` for every request of this version. Kafka 2.4 renamed the api and gave the version 1
 * the leading `election_type` byte of KIP-460 ({@see ElectionType}).
 *
 * The constructor is the one of {@see ElectLeadersRequest} and its `$electionType` is ignored here - the byte has
 * no place in this frame - so a caller that means the unclean election has to send the version 1.
 *
 * @see docs/protocol/2.8.md, section "ElectLeaders API (key 43, v0 to v2)"
 */
final class ElectLeadersRequestV0 extends ElectLeadersRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}

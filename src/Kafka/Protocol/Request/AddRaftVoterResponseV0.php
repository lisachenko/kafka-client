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
 * AddRaftVoter answer of version 0 (Kafka 3.9, KIP-853), byte for byte the answer of version 1
 *
 * The `ack_when_committed` of version 1 (Kafka 4.2) changed when a controller answers, not what it writes back. The
 * class exists because a version is a class on this line even when its schema is identical: a frame recorded at
 * version 0 is replayed through the class of version 0, and a caller that sends an {@see AddRaftVoterRequestV0}
 * reads its answer with it.
 *
 * @see docs/protocol/4.3.md, sections "AddRaftVoter API (key 80, v0 and v1)" and "The acknowledgement mode of
 *      Kafka 4.2 (v1)"
 */
final class AddRaftVoterResponseV0 extends AddRaftVoterResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}

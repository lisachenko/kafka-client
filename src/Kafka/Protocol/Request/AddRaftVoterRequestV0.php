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
 * AddRaftVoter request of version 0 (Kafka 3.9, KIP-853): the voter and its endpoints, without the acknowledgement
 * mode
 *
 * The frame of {@see AddRaftVoterRequest} minus its last field: version 1 (Kafka 4.2) appended the boolean
 * `ack_when_committed`, and this is the request below it. The {@see AddRaftVoterRequest::$ackWhenCommitted} of an
 * instance of this class never reaches the wire, and the controller answers once the new voter set is committed -
 * the `"default": "true"` of the field, which is the only behaviour a version 0 frame knows.
 *
 * @see docs/protocol/4.3.md, sections "AddRaftVoter API (key 80, v0 and v1)" and "The acknowledgement mode of
 *      Kafka 4.2 (v1)"
 */
final class AddRaftVoterRequestV0 extends AddRaftVoterRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}

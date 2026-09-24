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
 * WriteShareGroupState answer of version 0 (Kafka 4.1, KIP-932), byte for byte the answer of version 1
 *
 * The `DeliveryCompleteCount` of version 1 (Kafka 4.2, KIP-1226) is a field of the request only. The class exists
 * because a version is a class on this line even when its schema is identical: a frame recorded at version 0 is
 * replayed through the class of version 0, and a caller that sends a {@see WriteShareGroupStateRequestV0} reads its
 * answer with it.
 *
 * @see docs/protocol/4.3.md, section "WriteShareGroupState API (key 85, v0 and v1)"
 */
final class WriteShareGroupStateResponseV0 extends WriteShareGroupStateResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}

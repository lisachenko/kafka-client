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
 * EndTxn answer of version 4, the frame of version 5 without the producer id and the epoch
 *
 * "Version 5 enables bumping epoch on every transaction (KIP-890 Part 2), so producer ID and epoch are included in
 * the response" (`EndTxnResponse.json` @ 4.0.0): the version 4 of Kafka 3.8 is the throttle time and the one error
 * code of the whole request, and the producer that sent it keeps its producer id and epoch - which is why
 * {@see EndTxnResponse::hasProducerIdAndEpoch()} answers `false` for it.
 *
 * @see docs/protocol/4.3.md, section "EndTxn API (key 26, v0 to v5)"
 */
final class EndTxnResponseV4 extends EndTxnResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}

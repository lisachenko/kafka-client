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
 * AddOffsetsToTxn request of version 0 (Kafka 0.11), the frame of version 1 with a lower version field
 *
 * <pre>
 *   AddOffsetsToTxn Request (Version: 0) => transactional_id producer_id producer_epoch consumer_group_id
 * </pre>
 *
 * `ADD_OFFSETS_TO_TXN_REQUEST_V1 = ADD_OFFSETS_TO_TXN_REQUEST_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see AddOffsetsToTxnRequest::getScheme()} follows.
 *
 * What the higher version buys is the promise of KIP-219: a client that sends it honours `throttle_time_ms`
 * itself, so a 2.8.2 broker answers a throttled request of it FIRST and mutes the channel afterwards
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2: "Regardless of throttling, send the response
 * immediately") instead of holding the answer back for the throttle time.
 *
 * @see docs/protocol/2.8.md, section "AddOffsetsToTxn API (key 25, v0 and v1)"
 */
final class AddOffsetsToTxnRequestV0 extends AddOffsetsToTxnRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}

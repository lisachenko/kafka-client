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
 * InitProducerId request of version 0 (Kafka 0.11), the frame of version 1 with a lower version field
 *
 * <pre>
 *   InitProducerId Request (Version: 0) => transactional_id transaction_timeout_ms
 * </pre>
 *
 * `INIT_PRODUCER_ID_REQUEST_V1 = INIT_PRODUCER_ID_REQUEST_V0` in `Protocol.java` @ 2.0.1.
 * Kafka 2.0 raised the api by one version without touching a single byte of the frame, so this class only lowers
 * the version constant that {@see InitProducerIdRequest::getScheme()} follows.
 *
 * What the higher version buys is the promise of KIP-219: a client that sends it honours `throttle_time_ms`
 * itself, so a 2.8.2 broker answers a throttled request of it FIRST and mutes the channel afterwards
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2: "Regardless of throttling, send the response
 * immediately") instead of holding the answer back for the throttle time.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v3)"
 */
final class InitProducerIdRequestV0 extends InitProducerIdRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param string|null $transactionalId      Transactional id whose producer id is asked for, `null` for a plain
     *        idempotent producer
     * @param int         $transactionTimeoutMs How long the coordinator waits for a status update of an open
     *        transaction before it aborts it
     * @param string      $clientId             A user specified identifier for the client making the request
     * @param int         $correlationId        A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        ?string $transactionalId = null,
        int $transactionTimeoutMs = self::DEFAULT_TRANSACTION_TIMEOUT_MS,
        string $clientId = '',
        int $correlationId = 0
    ) {
        // The `producer_id` and the `producer_epoch` of KIP-360 are fields of the version 3 and have no place in
        // this frame, so this version can only ever ask for a NEW producer id
        parent::__construct(
            $transactionalId,
            $transactionTimeoutMs,
            self::NO_PRODUCER_ID,
            self::NO_PRODUCER_EPOCH,
            $clientId,
            $correlationId
        );
    }

}

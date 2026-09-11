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
 * InitProducerId, version 1: the KIP-219 bump of Kafka 2.0 (ApiKey 22)
 *
 * <pre>
 *   InitProducerId Request (Version: 1) => transactional_id transaction_timeout_ms
 * </pre>
 *
 * The last version of this api in the plain encoding: version 2 (Kafka 2.4) is the first flexible one and is what
 * {@see InitProducerIdRequest} sends. Version 1 itself is the frame of version 0 with a higher number in the
 * header, which is the client's promise that it honours a `throttle_time_ms` itself.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v3)"
 */
final class InitProducerIdRequestV1 extends InitProducerIdRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

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

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
 * InitProducerId request of version 2 (Kafka 2.4, KIP-482), the frame WITHOUT the pair of KIP-360
 *
 * <pre>
 *   InitProducerId Request (Version: 2) => transactional_id transaction_timeout_ms
 * </pre>
 *
 * Kafka 2.4 made the version 2 the first flexible one of this api and Kafka 2.5 appended the `producer_id` and
 * the `producer_epoch` of KIP-360 to the version 3; this class is the frame in between, which can only ask for a
 * NEW producer id. Its constructor is the one of {@see InitProducerIdRequest} and the two fields it inherits are
 * simply not written.
 *
 * @see docs/protocol/2.8.md, section "InitProducerId API (key 22, v0 to v3)"
 */
final class InitProducerIdRequestV2 extends InitProducerIdRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

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

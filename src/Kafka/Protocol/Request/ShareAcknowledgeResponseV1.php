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
 * ShareAcknowledge answer of version 1 (Kafka 4.1, KIP-932): the answer without the acquisition lock timeout
 *
 * Version 2 (Kafka 4.2, KIP-1222) put the `acquisition_lock_timeout_ms` behind the error message
 * ({@see ShareAcknowledgeResponse}); an answer of this version leaves {@see self::$acquisitionLockTimeoutMs} at 0.
 *
 * @see docs/protocol/4.3.md, section "ShareAcknowledge API (key 79, v1 and v2)"
 */
final class ShareAcknowledgeResponseV1 extends ShareAcknowledgeResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;
}

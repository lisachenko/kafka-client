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
 * EndTxn answer of version 3, the frame of version 4 with a lower version field
 *
 * "Version 4 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" (`EndTxnResponse.json` @ 3.8.1):
 * the throttle time and the one error code of the whole request. The **90** of KIP-588, which this api is the one
 * place of the four where it was really measured, is answered at both versions, and so is the **48** of an abort
 * that follows a commit; the 120 is not written here by a 3.9.2 coordinator at all. The producer id and the
 * epoch of the **version 5** are Kafka 3.9's.
 *
 * @see docs/protocol/4.3.md, section "EndTxn API (key 26, v0 to v5)"
 */
final class EndTxnResponseV3 extends EndTxnResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}

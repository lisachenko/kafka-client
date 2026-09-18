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
 * InitProducerId answer of version 4, the frame of version 5 with a lower version field
 *
 * "Version 5 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)" (`InitProducerIdResponse.json` @
 * 3.8.1): the same four values, and a code a 3.9.2 coordinator never writes on this api - there is no partition
 * to verify here, so the version 5 is answered exactly what this version is, the **90** of a fenced producer
 * included. The version 4 is what a broker below Kafka 3.8 answers.
 *
 * @see docs/protocol/3.9.md, section "InitProducerId API (key 22, v0 to v5)"
 */
final class InitProducerIdResponseV4 extends InitProducerIdResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 4;
}

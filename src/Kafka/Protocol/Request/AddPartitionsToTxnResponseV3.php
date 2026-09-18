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
 * AddPartitionsToTxn answer of version 3 (Kafka 2.8, KIP-482): the topics of one transaction, without a top-level code
 *
 * Version 4 (Kafka 3.5, KIP-890) put an `error_code` and a `results_by_transaction` array in place of the `errors`
 * array of this version ({@see AddPartitionsToTxnResponse}). This class decodes the answer of the versions a
 * client sends, where everything the coordinator has to say - the errors of the transactional id included - is
 * repeated on every requested partition, and {@see AddPartitionsToTxnResponse::resultOf()} reads it as one
 * transaction all the same.
 *
 * @see docs/protocol/3.9.md, section "AddPartitionsToTxn API (key 24, v0 to v5)"
 */
final class AddPartitionsToTxnResponseV3 extends AddPartitionsToTxnResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 3;
}

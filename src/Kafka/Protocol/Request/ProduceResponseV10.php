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
 * Produce response of version 10 (key 0)
 *
 * The leader discovery of KIP-951 (Kafka 3.7) and the last answer below the `TransactionAbortable` of KIP-890:
 * the tagged `current_leader` of a partition entry and the tagged `node_endpoints` of the body, exactly as
 * {@see ProduceResponse} decodes them - `ProduceResponse.json` @ 3.8.1 declares no field of version 11 either,
 * and its comment is "Version 11 adds support for new error code TRANSACTION_ABORTABLE (KIP-890)".
 *
 * What version 11 changes is the **error code** a partition of a transactional produce is refused with, and
 * `KafkaApis.handleProduceRequest` @ 3.9.2 decides it on the api version alone:
 * `val transactionSupportedOperation = if (request.header.apiVersion > 10) genericError else defaultError`. A
 * partition that the transaction coordinator has not verified is answered **120** `TransactionAbortable` above
 * version 10 and, through this version, the **48** `InvalidTxnState` with the message "Partition was not added to
 * the transaction" that `AddPartitionsToTxnManager` @ 3.9.2 maps it back to "for backward compatibility with
 * clients". Both frames are measured on the node, see the section of the document.
 *
 * @see docs/protocol/4.3.md, sections "Produce API (key 0, v0 to v12)" and "The abortable transaction error of
 *      KIP-890 (v11)"
 */
final class ProduceResponseV10 extends ProduceResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 10;
}

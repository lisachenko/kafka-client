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

namespace Protocol\Kafka\Common\Errors;

/**
 * The broker answered with a correlation id that does not belong to the request that was just sent.
 *
 * A broker echoes the correlation id of a request back in the header of its response and answers the requests of one
 * connection strictly in order, so a different value can only mean that the connection is out of sync: an earlier
 * response was left unread, or a fire-and-forget produce request (acks = 0) was answered by mistake. Reading any
 * further from such a connection would parse the bytes of one message as another, therefore the client drops it.
 *
 * This is a client-side condition, it has no wire error code in `kafka/common/ErrorMapping.scala`.
 *
 * @see docs/protocol/0.8.2.md, section "Responses"
 */
class CorrelationIdMismatchException extends KafkaException implements ClientExceptionInterface {}

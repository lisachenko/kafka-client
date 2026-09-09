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

namespace Protocol\Kafka\Network;

use Protocol\Kafka\Common\Errors\CorrelationIdMismatchException;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Request\AbstractResponse;

/**
 * Reads one response frame off a connection and checks that it belongs to the request that was sent.
 *
 * The only thing that ties a response to its request is the correlation id that the broker echoes back in the
 * response header, so every answer is checked against the id the client generated for the request. A mismatch means
 * the connection is out of sync and any further byte read from it would be parsed at the wrong offset, therefore it
 * is reported as a {@see CorrelationIdMismatchException} and the connection is dropped by the caller.
 *
 * @see docs/protocol/0.10.2.md, sections "Requests" and "Responses"
 */
final class ResponseValidator
{
    /**
     * Reads a response of the given class and verifies its correlation id
     *
     * @template T of AbstractResponse
     *
     * @param class-string<T>      $responseClass         Class of the expected response
     * @param Stream               $stream                Connection to read the answer from
     * @param int                  $expectedCorrelationId Correlation id that was sent with the request
     * @param array<string, mixed> $context               Additional context for the exception
     *
     * @return T
     *
     * @throws CorrelationIdMismatchException If the broker answered with a different correlation id
     */
    public static function read(
        string $responseClass,
        Stream $stream,
        int $expectedCorrelationId,
        array $context = []
    ): AbstractResponse {
        $response = $responseClass::unpack($stream);
        self::assertCorrelationId($expectedCorrelationId, $response, $context);

        return $response;
    }

    /**
     * Verifies that the response carries the correlation id of the request it answers
     *
     * @param int                  $expectedCorrelationId Correlation id that was sent with the request
     * @param AbstractResponse     $response              Response that came back from the broker
     * @param array<string, mixed> $context               Additional context for the exception
     *
     * @throws CorrelationIdMismatchException If the broker answered with a different correlation id
     */
    public static function assertCorrelationId(
        int $expectedCorrelationId,
        AbstractResponse $response,
        array $context = []
    ): void {
        $actualCorrelationId = $response->getCorrelationId();
        if ($actualCorrelationId === $expectedCorrelationId) {
            return;
        }

        throw new CorrelationIdMismatchException(
            [
                'expected' => $expectedCorrelationId,
                'received' => $actualCorrelationId,
                'response' => $response::class,
            ] + $context
        );
    }
}

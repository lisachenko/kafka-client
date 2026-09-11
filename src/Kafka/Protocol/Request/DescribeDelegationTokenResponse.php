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

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\DescribeDelegationTokenResponseToken;

/**
 * DescribeDelegationToken response object, version 1 (key 41)
 *
 * <pre>
 *   DescribeDelegationToken Response (Version: 0 and 1) => error_code [token_details] throttle_time_ms
 *     error_code    => INT16
 *     token_details => owner issue_timestamp expiry_timestamp max_timestamp token_id hmac [renewers]
 *     throttle_time_ms => INT32
 * </pre>
 *
 * `throttle_time_ms` is the **last** field here as well, see {@see CreateDelegationTokenResponse}.
 *
 * The token array is indexed by the token id in this implementation, because that identifier is unique within the
 * cluster; the wire form is an ordinary array in the order the token cache of the broker iterates in, which is not
 * an order the api guarantees.
 *
 * The error code is a **top-level** one and says something about the broker or about the connection - 61 for a
 * broker without a `delegation.token.master.key`, 64 for a connection that authenticated nobody. A caller that may
 * see no token at all is not an error: the answer is the code 0 with an empty array, and the same is true for a
 * request whose `owners` array is empty.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TOKEN_DESCRIBE_RESPONSE_V1 =
 * TOKEN_DESCRIBE_RESPONSE_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see DescribeDelegationTokenResponseV0} is the same frame with the version field of Kafka 1.1.
 *
 * @see docs/protocol/2.8.md, section "DescribeDelegationToken API (key 41, v0 and v1)"
 */
class DescribeDelegationTokenResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * Error code of the request, 0 when the tokens were listed
     */
    public int $errorCode = 0;

    /**
     * Every token the caller may see, indexed by the token id
     *
     * @var array<string, DescribeDelegationTokenResponseToken>
     */
    public array $tokenDetails = [];

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation
     */
    public int $throttleTimeMs = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'      => BinarySchema::TYPE_INT16,
            'tokenDetails'   => ['tokenId' => DescribeDelegationTokenResponseToken::class],
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
        ];
    }
}

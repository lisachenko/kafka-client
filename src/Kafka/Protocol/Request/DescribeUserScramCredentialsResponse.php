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
use Protocol\Kafka\Protocol\Data\DescribeUserScramCredentialsResult;

/**
 * DescribeUserScramCredentials response object, version 0 (key 50, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   DescribeUserScramCredentials Response (Version: 0) => throttle_time_ms error_code error_message [results]
 *     throttle_time_ms => INT32
 *     error_code       => INT16
 *     error_message    => COMPACT_NULLABLE_STRING
 *     results          => user error_code error_message [credential_infos]
 *       credential_infos => mechanism iterations
 * </pre>
 *
 * **There is a top-level error code and a per-user one**, and they mean different things: the top level is for
 * what the request as a whole ran into - 29/31 without the right acl, 15 while the credential cache is not ready -
 * and the per-user code for the user itself, which in practice is **91** (`ResourceNotFound`, *"User not found"*)
 * for a user without a single credential.
 *
 * **No answer ever carries a password.** The credential of a user is a salted password, and the api describes only
 * its mechanism and iteration count.
 *
 * @see docs/protocol/2.8.md, section "DescribeUserScramCredentials API (key 50, v0)"
 */
class DescribeUserScramCredentialsResponse extends AbstractResponse
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Duration in milliseconds for which the request was throttled due to a quota violation, zero without quotas
     */
    public int $throttleTimeMs = 0;

    /**
     * Error of the request as a whole, 0 when the users could be looked up
     */
    public int $errorCode;

    /**
     * Human readable description of the top-level error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Result of every user the broker answered, indexed by the user name
     *
     * @var array<string, DescribeUserScramCredentialsResult>
     */
    public array $results = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'throttleTimeMs' => BinarySchema::TYPE_INT32,
            'errorCode'      => BinarySchema::TYPE_INT16,
            'errorMessage'   => BinarySchema::TYPE_NULLABLE_STRING,
            'results'        => ['user' => DescribeUserScramCredentialsResult::class],
        ];
    }
}

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
use Protocol\Kafka\Protocol\Data\AlterUserScramCredentialsResult;

/**
 * AlterUserScramCredentials response object, version 0 (key 51, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   AlterUserScramCredentials Response (Version: 0) => throttle_time_ms [results]
 *     throttle_time_ms => INT32
 *     results          => user error_code error_message
 * </pre>
 *
 * **There is no top-level error code**: one result per **affected user**, not one per change, so a request that
 * deletes one credential of a user and writes another is answered with a single entry for them. The codes
 * measured on the container are **0**, **91** (`ResourceNotFound`) for the deletion of a credential that is not
 * there, **92** (`DuplicateResource`) when one request names the same user and mechanism twice, and **93**
 * (`UnacceptableCredential`) for an iteration count the mechanism does not allow.
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
class AlterUserScramCredentialsResponse extends AbstractResponse
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
     * Result of every affected user, indexed by the user name
     *
     * @var array<string, AlterUserScramCredentialsResult>
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
            'results'        => ['user' => AlterUserScramCredentialsResult::class],
        ];
    }
}

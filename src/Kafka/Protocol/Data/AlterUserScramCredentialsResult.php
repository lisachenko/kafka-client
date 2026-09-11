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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The result of one user of an AlterUserScramCredentials answer (key 51, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   AlterUserScramCredentialsResult => User ErrorCode ErrorMessage
 *     User         => COMPACT_STRING
 *     ErrorCode    => INT16
 *     ErrorMessage => COMPACT_NULLABLE_STRING
 * </pre>
 *
 * **One result per affected user, not per change**: a request that deletes the SHA-256 credential of a user and
 * writes their SHA-512 one is answered with a single entry, and there is no top-level error code at all.
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
class AlterUserScramCredentialsResult implements BinarySchemaInterface
{
    /**
     * Name of the user this result belongs to
     */
    public string $user;

    /**
     * Error of this user, 0 when every change of theirs was applied
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'user'         => BinarySchema::TYPE_STRING,
            'errorCode'    => BinarySchema::TYPE_INT16,
            'errorMessage' => BinarySchema::TYPE_NULLABLE_STRING,
        ];
    }
}

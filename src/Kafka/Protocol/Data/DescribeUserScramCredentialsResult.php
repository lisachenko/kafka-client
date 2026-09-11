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
 * The result of one user of a DescribeUserScramCredentials answer (key 50, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   DescribeUserScramCredentialsResult => User ErrorCode ErrorMessage CredentialInfos
 *     User            => COMPACT_STRING
 *     ErrorCode       => INT16
 *     ErrorMessage    => COMPACT_NULLABLE_STRING
 *     CredentialInfos => COMPACT_ARRAY of {@see ScramCredentialInfo}
 * </pre>
 *
 * A user that has no SCRAM credential at all is the error code **91** (`ResourceNotFound`) with the message
 * *"User not found"* - the api distinguishes "this user has no credential" from "this user was not asked about",
 * which a silent empty list could not.
 *
 * @see docs/protocol/2.8.md, section "DescribeUserScramCredentials API (key 50, v0)"
 */
class DescribeUserScramCredentialsResult implements BinarySchemaInterface
{
    /**
     * Name of the user this result belongs to
     */
    public string $user;

    /**
     * Error of this user, 0 when the credentials could be described
     */
    public int $errorCode;

    /**
     * Human readable description of the error, null when there is none
     */
    public ?string $errorMessage = null;

    /**
     * Credentials of this user, indexed by the SCRAM mechanism
     *
     * @var array<int, ScramCredentialInfo>
     */
    public array $credentialInfos = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'user'            => BinarySchema::TYPE_STRING,
            'errorCode'       => BinarySchema::TYPE_INT16,
            'errorMessage'    => BinarySchema::TYPE_NULLABLE_STRING,
            'credentialInfos' => ['mechanism' => ScramCredentialInfo::class],
        ];
    }
}

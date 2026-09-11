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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Data\ScramCredentialDeletion;
use Protocol\Kafka\Protocol\Data\ScramCredentialUpsertion;

/**
 * AlterUserScramCredentials, version 0: writes and removes SCRAM credentials (ApiKey 51, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   AlterUserScramCredentials Request (Version: 0) => [deletions] [upsertions]
 *     deletions  => name mechanism
 *     upsertions => name mechanism iterations salt salted_password
 * </pre>
 *
 * **The deletions come first on the wire**, and the broker applies them first as well, which is what makes
 * "replace the credential of this user" one request. Flexible from the version 0, and served by any
 * ZooKeeper-backed broker.
 *
 * **The client derives the salted password.** `salted_password` is `Hi(password, salt, iterations)` of RFC 5802 -
 * `hash_pbkdf2('sha256', $password, $salt, $iterations, 32, true)` for SCRAM-SHA-256 - so the broker never sees a
 * password, only something it can verify a login against. `iterations` below the minimum of the mechanism (4096
 * for both mechanisms of 2.8.2) is refused with **93** (`UnacceptableCredential`).
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
class AlterUserScramCredentialsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ALTER_USER_SCRAM_CREDENTIALS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Credentials to remove, in the order they were given
     *
     * @var list<ScramCredentialDeletion>
     */
    protected readonly array $deletions;

    /**
     * Credentials to write, in the order they were given
     *
     * @var list<ScramCredentialUpsertion>
     */
    protected readonly array $upsertions;

    /**
     * @param list<ScramCredentialDeletion>  $deletions     Credentials to remove
     * @param list<ScramCredentialUpsertion> $upsertions    Credentials to write
     * @param string                         $clientId      A user specified identifier for the client
     * @param int                            $correlationId A value the broker passes back unmodified
     */
    public function __construct(
        array $deletions,
        array $upsertions,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->deletions  = array_values($deletions);
        $this->upsertions = array_values($upsertions);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'deletions'  => [ScramCredentialDeletion::class],
            'upsertions' => [ScramCredentialUpsertion::class],
        ];
    }
}

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
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ScramUserName;

/**
 * DescribeUserScramCredentials, version 0: which SCRAM credentials a user has (ApiKey 50, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   DescribeUserScramCredentials Request (Version: 0) => [users]
 *     users => name
 *       name => COMPACT_STRING
 * </pre>
 *
 * KIP-554 gave `kafka-configs.sh --entity-type users --add-config 'SCRAM-SHA-256=[…]'` a protocol of its own, so
 * that SCRAM users no longer have to be written into ZooKeeper by hand. The api is **flexible from its version 0**
 * - Kafka 2.7 is well past KIP-482 - and is answered by any ZooKeeper-backed broker
 * (`"listeners": ["zkBroker"]`), from the credential cache it keeps for authentication.
 *
 * **The user array is nullable, and `null` and the empty array mean the same thing here**: describe every user
 * that has a credential. `DescribeUserScramCredentialsRequest.json` @ 2.8.2 says so in as many words - *"or
 * null/empty to describe all users"* - which is the one nullable array of this protocol whose empty form is not
 * "nothing".
 *
 * @see docs/protocol/2.8.md, section "DescribeUserScramCredentials API (key 50, v0)"
 */
class DescribeUserScramCredentialsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_USER_SCRAM_CREDENTIALS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 0;

    /**
     * Users to describe, null for every user of the cluster
     *
     * @var list<ScramUserName>|null
     */
    protected readonly ?array $users;

    /**
     * @param list<string>|null $users         Names of the users to describe, null (or []) for all of them
     * @param string            $clientId      A user specified identifier for the client
     * @param int               $correlationId A value the broker passes back unmodified
     */
    public function __construct(?array $users, string $clientId = '', int $correlationId = 0)
    {
        $this->users = $users === null
            ? null
            : array_map(static fn(string $name): ScramUserName => new ScramUserName($name), array_values($users));

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'users' => [ScramUserName::class, BinarySchema::FLAG_NULLABLE => true],
        ];
    }

    /**
     * Returns the users this request asks about, null for every user of the cluster
     *
     * @return list<ScramUserName>|null
     */
    public function getUsers(): ?array
    {
        return $this->users;
    }
}

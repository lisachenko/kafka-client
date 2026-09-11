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

use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;

/**
 * CreateDelegationToken, version 2: issues a delegation token for the principal of the connection (ApiKey 38,
 * Kafka 1.1, KIP-48)
 *
 * <pre>
 *   CreateDelegationToken Request (Version: 0 and 1) => [renewers] max_life_time
 *     renewers => principal_type name
 *       principal_type => STRING
 *       name           => STRING
 *     max_life_time => INT64
 * </pre>
 *
 * The **owner** of the token is not in the request: it is the principal of the connection the request arrived on,
 * `request.session.principal` in `KafkaApis.handleCreateTokenRequest` @ 1.1.1. That is also why the api is refused
 * with the error code 64 (`DelegationTokenRequestNotAllowed`) on a channel that authenticated nobody - a PLAINTEXT
 * listener, a one-way SSL one, or a channel that itself authenticated with a token.
 *
 * `renewers` are the principals that may renew or expire the token besides its owner; every one of them has to be
 * of the type {@see KafkaPrincipal::USER_TYPE}, anything else is answered with 67 (`InvalidPrincipalType`) before
 * the token manager is asked for anything.
 *
 * `max_life_time` is a **period** in milliseconds, not a timestamp: the broker caps it at its own
 * `delegation.token.max.lifetime.ms` (7 days by default) and a value of `-1`
 * ({@see self::DEFAULT_MAX_LIFE_TIME}, i.e. anything `<= 0`) asks for exactly that maximum.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TOKEN_CREATE_REQUEST_V1 =
 * TOKEN_CREATE_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 *
 * **Kafka 2.4 added version 2**, the first **flexible** version of the api (`"flexibleVersions": "2+"` in
 * `CreateDelegationTokenRequest.json` @ 2.8.2): the request header v2, a compact array of renewers whose two
 * strings are compact as well, and a tagged-field section at the end of every structure. No field is added, and
 * `throttle_time_ms` stays the **last** field of the answer, where KIP-124 put it for the four token apis.
 * {@see CreateDelegationTokenRequestV1} and {@see CreateDelegationTokenRequestV0} are the same frame in the plain
 * encoding.
 *
 * @see docs/protocol/2.8.md, section "CreateDelegationToken API (key 38, v0 to v2)"
 */
class CreateDelegationTokenRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::CREATE_DELEGATION_TOKEN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Asks for the `delegation.token.max.lifetime.ms` of the broker instead of a lifetime of its own
     */
    public const int DEFAULT_MAX_LIFE_TIME = -1;

    /**
     * Principals that may renew this token besides its owner
     *
     * @var list<KafkaPrincipal>
     */
    protected readonly array $renewers;

    /**
     * @param list<KafkaPrincipal|string> $renewers Principals that may renew the token, as objects or as
     *        `<type>:<name>` strings
     * @param int    $maxLifeTime   Maximum lifetime of the token in milliseconds, or
     *        {@see self::DEFAULT_MAX_LIFE_TIME} for the maximum of the broker
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(
        array $renewers = [],
        /**
         * Maximum lifetime of the token in milliseconds, capped by `delegation.token.max.lifetime.ms`
         */
        protected readonly int $maxLifeTime = self::DEFAULT_MAX_LIFE_TIME,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->renewers = KafkaPrincipal::listOf($renewers);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the principals that may renew the token besides its owner
     *
     * @return list<KafkaPrincipal>
     */
    public function getRenewers(): array
    {
        return $this->renewers;
    }

    /**
     * Returns the maximum lifetime the request asks for, in milliseconds
     */
    public function getMaxLifeTime(): int
    {
        return $this->maxLifeTime;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'renewers'    => [KafkaPrincipal::class],
            'maxLifeTime' => BinarySchema::TYPE_INT64,
        ];
    }
}

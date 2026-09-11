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
 * DescribeDelegationToken, version 1: lists the tokens the caller may see (ApiKey 41, Kafka 1.1, KIP-48)
 *
 * <pre>
 *   DescribeDelegationToken Request (Version: 0 and 1) => [owners]
 *     owners => principal_type name     (nullable array)
 *       principal_type => STRING
 *       name           => STRING
 * </pre>
 *
 * The array is **nullable and the three cases are three different questions**, which
 * `KafkaApis.handleDescribeTokensRequest` @ 1.1.1 tells apart before it filters anything:
 *
 *  * `null` ({@see self::ALL_OWNERS}) asks for every token the caller may see - its own ones, the ones it may
 *    renew, and, on a cluster with an authorizer, the ones it has a Describe permission on;
 *  * a **non-empty** array asks for the tokens whose owner or renewer is one of the named principals, intersected
 *    with the same permission check;
 *  * an **empty** array asks for nothing and is answered with the error code 0 and an empty token array, without
 *    the token cache being read at all (`ownersListEmpty()`).
 *
 * Unlike the other three apis of KIP-48 this one has a top-level error code that says something about the broker
 * rather than about a token: a broker without a `delegation.token.master.key` answers 61
 * (`DelegationTokenAuthDisabled`) here, and every listener that authenticated nobody the 64.
 *
 * **Kafka 2.0 added version 1** and changed nothing about the bytes: `TOKEN_DESCRIBE_REQUEST_V1 =
 * TOKEN_DESCRIBE_REQUEST_V0` in `Protocol.java` @ 2.0.1. The higher version is the client's promise of KIP-219 -
 * that it honours `throttle_time_ms` itself - and a 2.8.2 broker acts on it by answering a throttled request
 * FIRST and muting the channel afterwards, instead of holding the answer back
 * (`RequestHandlerHelper.sendResponseMaybeThrottle` @ 2.8.2).
 * {@see DescribeDelegationTokenRequestV0} is the same frame with the version field of Kafka 1.1.
 *
 * **Kafka 2.5 added the version 2** (KIP-482), the same fields in the flexible encoding: every string and array of
 * the frame is compact, the header carries a tag buffer and every structure ends in one. Not a field changed.
 *
 * @see docs/protocol/2.8.md, section "DescribeDelegationToken API (key 41, v0 to v2)"
 */
class DescribeDelegationTokenRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::DESCRIBE_DELEGATION_TOKEN;

    /**
     * @inheritdoc
     */
    public const int VERSION = 2;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 2;

    /**
     * Asks for every token the caller may see, i.e. the null array on the wire
     */
    public const ?array ALL_OWNERS = null;

    /**
     * Owners whose tokens are asked for, null for every token the caller may see
     *
     * @var list<KafkaPrincipal>|null
     */
    protected readonly ?array $owners;

    /**
     * @param list<KafkaPrincipal|string>|null $owners Owners to ask for, as objects or as `<type>:<name>` strings;
     *        {@see self::ALL_OWNERS} for every token the caller may see, an empty array for none
     * @param string $clientId      A user specified identifier for the client making the request
     * @param int    $correlationId A user-supplied value that the broker passes back unmodified
     */
    public function __construct(?array $owners = self::ALL_OWNERS, string $clientId = '', int $correlationId = 0)
    {
        $this->owners = $owners === null ? null : KafkaPrincipal::listOf($owners);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * Returns the owners the request asks for, null meaning every token the caller may see
     *
     * @return list<KafkaPrincipal>|null
     */
    public function getOwners(): ?array
    {
        return $this->owners;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'owners' => [KafkaPrincipal::class, BinarySchema::FLAG_NULLABLE => true],
        ];
    }
}

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

use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One token of a DescribeDelegationToken answer, i.e. one entry of the `token_details` array
 *
 * <pre>
 *   DescribeDelegationTokenResponseToken => owner issue_timestamp expiry_timestamp max_timestamp token_id hmac
 *                                           [renewers]
 *     owner => principal_type name
 *     issue_timestamp  => INT64
 *     expiry_timestamp => INT64
 *     max_timestamp    => INT64
 *     token_id         => STRING
 *     hmac             => BYTES
 *     renewers         => principal_type name
 * </pre>
 *
 * `TOKEN_DETAILS_V0` in `DescribeDelegationTokenResponse.java` @ 1.1.1, which decodes it into a
 * `DelegationToken` - a `TokenInformation` and the hmac - and which this package mirrors as
 * {@see \Protocol\Kafka\Admin\DelegationToken}.
 *
 * The entry carries **everything** the CreateDelegationToken answer carried, the hmac included, plus the renewers
 * that the create request had named and that the create answer did not repeat. A caller that may describe a token
 * can therefore renew and expire it: on a cluster without an authorizer only the owner and the renewers of a token
 * see it at all, which is what `DelegationTokenManager.filterToken` guarantees.
 *
 * @see docs/protocol/2.8.md, section "DescribeDelegationToken API (key 41, v0 and v1)"
 */
class DescribeDelegationTokenResponseToken implements BinarySchemaInterface
{
    /**
     * Principal the token was issued for
     */
    public KafkaPrincipal $owner;

    /**
     * Milliseconds since the epoch at which the broker issued the token
     */
    public int $issueTimestamp;

    /**
     * Milliseconds since the epoch at which the token expires unless it is renewed before
     */
    public int $expiryTimestamp;

    /**
     * Milliseconds since the epoch beyond which no renewal can move the expiry
     */
    public int $maxTimestamp;

    /**
     * Identifier of the token, a base64 uuid
     */
    public string $tokenId;

    /**
     * Raw bytes of the HMAC of the token
     */
    public string $hmac;

    /**
     * Principals that may renew this token besides its owner
     *
     * @var list<KafkaPrincipal>
     */
    public array $renewers;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'owner'           => KafkaPrincipal::class,
            'issueTimestamp'  => BinarySchema::TYPE_INT64,
            'expiryTimestamp' => BinarySchema::TYPE_INT64,
            'maxTimestamp'    => BinarySchema::TYPE_INT64,
            'tokenId'         => BinarySchema::TYPE_STRING,
            'hmac'            => BinarySchema::TYPE_BYTEARRAY,
            'renewers'        => [KafkaPrincipal::class],
        ];
    }
}

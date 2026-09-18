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
use Protocol\Kafka\Protocol\InlineStruct;

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
 * **Kafka 3.3 added the requester** ("Version 3 adds token requester details" of
 * `DescribeDelegationTokenResponse.json` @ 3.3.2): two strings between the owner and the timestamps that name the
 * principal which **asked** for the token, which is the owner itself for every token below KIP-373 and the caller
 * for a token that was issued for somebody else. {@see DescribeDelegationTokenResponseTokenV2} is the entry of
 * every version below 3.
 *
 * @see docs/protocol/3.9.md, section "DescribeDelegationToken API (key 41, v0 to v3)"
 */
class DescribeDelegationTokenResponseToken implements BinarySchemaInterface
{
    /**
     * Version of the answer this entry belongs to; the version 3 of Kafka 3.3 is the first one with a requester
     */
    public const int VERSION = 3;

    /**
     * Principal the token was issued for
     */
    public KafkaPrincipal $owner;

    /**
     * Principal that asked for the token, the owner itself unless it was issued for somebody else
     *
     * @since Version 3 of protocol (Kafka 3.3, KIP-373)
     */
    public KafkaPrincipal $tokenRequester;

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
     * Returns the principal that asked for the token, which is the owner below the version 3
     *
     * A version below 3 does not carry the field at all, so the property stays uninitialized there and the owner
     * is the only answer the entry has.
     */
    public function requester(): KafkaPrincipal
    {
        return $this->tokenRequester ?? $this->owner;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $scheme = [
            // The two principal fields of a `DescribedDelegationToken` are flat fields of the specification, not a
            // structure of their own, so they are inlined: in the flexible v2 of this api (Kafka 2.5) a structure
            // would carry a tagged-field section here and the broker sends none
            'owner' => new InlineStruct(KafkaPrincipal::class),
        ];
        // The requester of KIP-373 is two more flat fields, right behind the owner
        if (static::VERSION >= 3) {
            $scheme['tokenRequester'] = new InlineStruct(KafkaPrincipal::class);
        }
        $scheme += [
            'issueTimestamp'  => BinarySchema::TYPE_INT64,
            'expiryTimestamp' => BinarySchema::TYPE_INT64,
            'maxTimestamp'    => BinarySchema::TYPE_INT64,
            'tokenId'         => BinarySchema::TYPE_STRING,
            'hmac'            => BinarySchema::TYPE_BYTEARRAY,
            'renewers'        => [KafkaPrincipal::class],
        ];

        return $scheme;
    }
}

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

namespace Protocol\Kafka\Admin;

use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Protocol\Data\DescribeDelegationTokenResponseToken;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenResponse;

/**
 * Everything about a delegation token except its secret, i.e. the public half of it
 *
 * `org.apache.kafka.common.security.token.delegation.TokenInformation` @ 1.1.1: the identifier of the token, who
 * owns it, who else may renew it, and the three timestamps of its life. It is what the broker stores in ZooKeeper
 * under `/delegation_token/<tokenId>` and what `kafka-delegation-tokens.sh --describe` prints; the secret half -
 * the hmac - lives next to it in {@see DelegationToken}.
 *
 * The three timestamps are absolute milliseconds since the epoch of the **broker's** clock:
 *
 *  * `issueTimestamp` is when the token was created;
 *  * `expiryTimestamp` is when it stops working unless it is renewed before, and it is the only one a renewal
 *    moves ({@see AdminClient::renewDelegationToken()});
 *  * `maxTimestamp` is the hard end of its life, `issueTimestamp` plus the maximum lifetime the create request
 *    asked for, capped by `delegation.token.max.lifetime.ms`; no renewal ever moves the expiry beyond it.
 *
 * @see docs/protocol/2.8.md, section "CreateDelegationToken API (key 38, v0)"
 */
final class TokenInformation
{
    /**
     * The argument order is the one of `new TokenInformation(tokenId, owner, renewers, issueTimestamp,
     * maxTimestamp, expiryTimestamp)` @ 1.1.1, the maximum before the expiry.
     *
     * @param string               $tokenId         Identifier of the token, a base64 uuid
     * @param KafkaPrincipal       $owner           Principal the token was issued for
     * @param list<KafkaPrincipal> $renewers        Principals that may renew the token besides its owner
     * @param int                  $issueTimestamp  Milliseconds since the epoch at which the token was issued
     * @param int                  $maxTimestamp    Milliseconds since the epoch beyond which it cannot be renewed
     * @param int                  $expiryTimestamp Milliseconds since the epoch at which it expires
     */
    public function __construct(
        public readonly string $tokenId,
        public readonly KafkaPrincipal $owner,
        public readonly array $renewers,
        public readonly int $issueTimestamp,
        public readonly int $maxTimestamp,
        public readonly int $expiryTimestamp
    ) {}

    /**
     * Builds the information of a token out of one entry of a DescribeDelegationToken answer
     */
    public static function fromResponseToken(DescribeDelegationTokenResponseToken $token): self
    {
        return new self(
            $token->tokenId,
            $token->owner,
            array_values($token->renewers),
            $token->issueTimestamp,
            $token->maxTimestamp,
            $token->expiryTimestamp
        );
    }

    /**
     * Builds the information of a freshly issued token out of the answer and the renewers the request had named
     *
     * The CreateDelegationToken answer does **not** repeat the renewers of the request, so the caller's list is the
     * only place they can come from - which is exactly what the Scala `AdminClient.createToken` @ 1.1.1 does.
     *
     * @param list<KafkaPrincipal> $renewers Renewers of the {@see CreateDelegationTokenRequest} that was answered
     */
    public static function fromCreateResponse(CreateDelegationTokenResponse $response, array $renewers = []): self
    {
        return new self(
            $response->tokenId,
            $response->owner,
            array_values($renewers),
            $response->issueTimestamp,
            $response->maxTimestamp,
            $response->expiryTimestamp
        );
    }

    /**
     * Returns the owner in the `<type>:<name>` form every Kafka tool prints, `TokenInformation.ownerAsString()`
     */
    public function ownerAsString(): string
    {
        return (string) $this->owner;
    }

    /**
     * Returns the renewers in the `<type>:<name>` form, `TokenInformation.renewersAsString()`
     *
     * @return list<string>
     */
    public function renewersAsString(): array
    {
        return array_map(static fn(KafkaPrincipal $renewer): string => (string) $renewer, $this->renewers);
    }

    /**
     * Checks whether the given principal may renew or expire this token, `TokenInformation.ownerOrRenewer()`
     *
     * It is the check `DelegationTokenManager.allowedToRenew` @ 1.1.1 performs before it touches a token; a
     * principal that fails it is answered with the error code 63 (`DelegationTokenOwnerMismatch`).
     */
    public function ownerOrRenewer(KafkaPrincipal $principal): bool
    {
        if ($this->owner->equals($principal)) {
            return true;
        }

        foreach ($this->renewers as $renewer) {
            if ($renewer->equals($principal)) {
                return true;
            }
        }

        return false;
    }
}

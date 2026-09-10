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
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenResponse;

/**
 * A delegation token of KIP-48: what it is about, and the secret that proves it
 *
 * `org.apache.kafka.common.security.token.delegation.DelegationToken` @ 1.1.1 - a {@see TokenInformation} and the
 * raw bytes of the HMAC the broker derived from its `delegation.token.master.key`. The two halves are used for
 * different things:
 *
 *  * the **hmac** is what names the token in every later request: {@see AdminClient::renewDelegationToken()} and
 *    {@see AdminClient::expireDelegationToken()} take the raw bytes, and `kafka-delegation-tokens.sh` takes the
 *    base64 form of them that {@see self::hmacAsBase64String()} returns;
 *  * the **token id** is the user name of a SASL/SCRAM login that authenticates *with* the token, the base64 hmac
 *    being its password.
 *
 * **This client cannot use a token to authenticate.** The login of KIP-48 is `SCRAM-SHA-256`/`SCRAM-SHA-512` with
 * `tokenauth=true` in the SASL extensions, and this package implements the mechanism `PLAIN` alone
 * ({@see \Protocol\Kafka\Common\Security\SaslMechanism}). The four apis that issue, renew, expire and describe a
 * token are implemented and verified against a real broker; what the token is then handed to has to be a client
 * that speaks SCRAM.
 *
 * @see docs/protocol/1.1.md, section "Delegation tokens (KIP-48)"
 */
final class DelegationToken
{
    /**
     * @param TokenInformation $tokenInformation The public half of the token
     * @param string           $hmac             Raw bytes of the HMAC, the secret half of the token
     */
    public function __construct(
        public readonly TokenInformation $tokenInformation,
        public readonly string $hmac
    ) {}

    /**
     * Builds a token out of one entry of a DescribeDelegationToken answer
     */
    public static function fromResponseToken(DescribeDelegationTokenResponseToken $token): self
    {
        return new self(TokenInformation::fromResponseToken($token), $token->hmac);
    }

    /**
     * Builds a freshly issued token out of the answer and the renewers the request had named
     *
     * @param list<KafkaPrincipal> $renewers Renewers of the request that was answered
     */
    public static function fromCreateResponse(CreateDelegationTokenResponse $response, array $renewers = []): self
    {
        return new self(TokenInformation::fromCreateResponse($response, $renewers), $response->hmac);
    }

    /**
     * Returns the identifier of the token, `DelegationToken.tokenInfo().tokenId()`
     */
    public function tokenId(): string
    {
        return $this->tokenInformation->tokenId;
    }

    /**
     * Returns the HMAC in the base64 form that every Kafka tool prints and accepts,
     * `DelegationToken.hmacAsBase64String()`
     */
    public function hmacAsBase64String(): string
    {
        return base64_encode($this->hmac);
    }
}

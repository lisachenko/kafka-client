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

/**
 * The four delegation-token apis Kafka 1.1 added with KIP-48: CreateDelegationToken (38), RenewDelegationToken
 * (39), ExpireDelegationToken (40) and DescribeDelegationToken (41), all at version 0.
 *
 * A delegation token is a short-lived shared secret that a broker issues to the principal of the **connection**.
 * Three properties decide how this example is written:
 *
 *  - the apis need an **authenticated** channel. A PLAINTEXT or one-way-SSL connection has the principal
 *    `User:ANONYMOUS`, and every one of the four is refused with the error code 64 before it reads the body, so
 *    this example runs over SASL_PLAINTEXT (9094) or SASL_SSL (9095) - the very transport of
 *    {@see examples/sasl.php};
 *  - a token is named by the raw bytes of its **HMAC** in every request, never by its id. `hmacAsBase64String()`
 *    is the printable form, and it is what `kafka-delegation-tokens.sh` shows;
 *  - the broker needs a `delegation.token.master.key`. Without one all four apis answer 61 instead of doing
 *    anything; the container of `docker-compose.yml` sets one.
 *
 * What is **not** here is the other half of KIP-48: authenticating *with* a token is a SASL/SCRAM login whose user
 * name is the token id and whose password is the base64 HMAC, and this client speaks SASL/PLAIN only. A token can
 * be issued, renewed, expired and described from PHP; it cannot be used to log in.
 *
 *   docker compose up -d
 *   php examples/delegation-tokens.php
 *   KAFKA_SASL_SSL_BOOTSTRAP_SERVERS=127.0.0.1:9095 php examples/delegation-tokens.php   # inside TLS
 *
 * @see docs/protocol/1.1.md, sections "Delegation tokens (KIP-48)", "CreateDelegationToken API (key 38, v0)",
 *      "RenewDelegationToken API (key 39, v0)", "ExpireDelegationToken API (key 40, v0)" and
 *      "DescribeDelegationToken API (key 41, v0)"
 */

use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;

require dirname(__DIR__) . '/vendor/autoload.php';

$saslSslBootstrapServer = getenv('KAFKA_SASL_SSL_BOOTSTRAP_SERVERS') ?: '';
$bootstrapServer        = $saslSslBootstrapServer !== ''
    ? $saslSslBootstrapServer
    : (getenv('KAFKA_SASL_BOOTSTRAP_SERVERS') ?: '127.0.0.1:9094');
$securityProtocol       = $saslSslBootstrapServer !== ''
    ? SecurityProtocol::SASL_SSL
    : SecurityProtocol::SASL_PLAINTEXT;
$certificate            = dirname(__DIR__) . '/docker/kafka-1.1.1/ssl/broker.crt';

$configuration = [
    ClientConfig::BOOTSTRAP_SERVERS   => ['tcp://' . trim(explode(',', $bootstrapServer)[0])],
    ClientConfig::CLIENT_ID           => 'example-delegation-tokens',
    ClientConfig::SECURITY_PROTOCOL   => $securityProtocol,
    ClientConfig::SASL_MECHANISM      => SaslMechanism::PLAIN,
    ClientConfig::SASL_USERNAME       => getenv('KAFKA_SASL_USERNAME') ?: 'kafkatest',
    ClientConfig::SASL_PASSWORD       => getenv('KAFKA_SASL_PASSWORD') ?: 'kafkatest-secret',
    ClientConfig::REQUEST_TIMEOUT_MS  => 10000,
];

if ($securityProtocol === SecurityProtocol::SASL_SSL) {
    $configuration[ClientConfig::SSL_CA_CERT_LOCATION] = $certificate;
}

$cluster = Cluster::bootstrap($configuration);
$admin   = new AdminClient($cluster, $configuration);

$moment = static fn(int $milliseconds): string => date('Y-m-d H:i:s', intdiv($milliseconds, 1000));

// CreateDelegationToken (38). The owner is the principal of this connection - `User:kafkatest` here - and is never
// sent; the renewers are the principals that may renew or expire the token besides its owner. `maxLifeTimeMs` is
// capped by `delegation.token.max.lifetime.ms` of the broker, and -1 asks for that maximum.
$token = $admin->createDelegationToken([KafkaPrincipal::fromString('User:admin')], 3600 * 1000);

echo "Created a token\n";
echo "  token id:  {$token->tokenId()}\n";
echo "  owner:     {$token->tokenInformation->ownerAsString()}\n";
echo '  renewers:  ' . (implode(', ', $token->tokenInformation->renewersAsString()) ?: 'none') . "\n";
echo "  issued at: {$moment($token->tokenInformation->issueTimestamp)}\n";
echo "  expires:   {$moment($token->tokenInformation->expiryTimestamp)}";
echo " (never after {$moment($token->tokenInformation->maxTimestamp)})\n";
echo '  hmac:      ' . substr($token->hmacAsBase64String(), 0, 24) . "...\n";

// DescribeDelegationToken (41). A null owner list asks for every token this principal may see, an empty array for
// none of them; the answer carries the HMAC of each token, which is why the api is an authorization decision on a
// broker that has an authorizer.
echo "\nTokens this principal can describe\n";
foreach ($admin->describeDelegationToken() as $described) {
    $isThisOne = $described->tokenId() === $token->tokenId() ? ' <- the one above' : '';
    echo "  {$described->tokenId()} of {$described->tokenInformation->ownerAsString()}"
        . ", expires {$moment($described->tokenInformation->expiryTimestamp)}{$isThisOne}\n";
}

// RenewDelegationToken (39). The answer is the new expiry timestamp, which is
// min(maxTimestamp, now + renewTimePeriodMs) - a renewal can never push a token past the maximum lifetime it was
// created with, so asking for a week here still lands on the hour the token was given.
$expiry = $admin->renewDelegationToken($token->hmac, 7 * 24 * 3600 * 1000);
echo "\nRenewed for seven days, and the broker answered {$moment($expiry)}";
echo $expiry === $token->tokenInformation->maxTimestamp ? " - the maximum lifetime of the token\n" : "\n";

// ExpireDelegationToken (40). A negative period **removes** the token at once; a positive one moves its expiry
// forward. The two are different states afterwards: a removed token is 62 (DelegationTokenNotFound) for every
// later request, while one that merely ran past its expiry is 66 (DelegationTokenExpired) and can only be swept
// away by the broker itself.
$removedAt = $admin->expireDelegationToken($token->hmac, -1);
echo "\nRemoved the token at {$moment($removedAt)}\n";

try {
    $admin->expireDelegationToken($token->hmac, -1);
} catch (KafkaException $exception) {
    echo '  expiring it again: error code ' . $exception->getCode() . ' (' . $exception::class . ")\n";
}

Node::closeConnections();

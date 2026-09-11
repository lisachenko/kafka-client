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

namespace Protocol\Kafka\Tests\Integration;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\DelegationToken;
use Protocol\Kafka\Admin\TokenInformation;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\DelegationTokenExpiredException;
use Protocol\Kafka\Common\Errors\DelegationTokenNotFoundException;
use Protocol\Kafka\Common\Errors\DelegationTokenOwnerMismatchException;
use Protocol\Kafka\Common\Errors\InvalidPrincipalTypeException;
use Protocol\Kafka\Common\Errors\UnsupportedByAuthenticationException;
use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Protocol\Data\DescribeDelegationTokenResponseToken;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenResponse;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenResponse;

/**
 * Exercises the four delegation token apis (keys 38 to 41) against a real Kafka 1.1.1 broker.
 *
 * KIP-48 issues a token to the principal of the **connection**, so every one of these tests runs over a SASL
 * listener of the container, where that principal is the SASL/PLAIN user - `User:kafkatest` for the client under
 * test and `User:admin` for the second one, which is what the owner-mismatch cases need. The container carries a
 * `delegation.token.master.key`; without it every call would be the error code 61 instead.
 *
 * Every token this class creates is expired again, with the one documented exception of the token whose maximum
 * lifetime is a single millisecond: a token that is past its expiry can no longer be expired through the protocol
 * at all (the broker answers 66 for that too), and only the broker's own sweeper removes it.
 *
 * @see docs/protocol/2.8.md, sections "Delegation tokens (KIP-48)", "CreateDelegationToken API (key 38, v0 to v2)",
 *      "RenewDelegationToken API (key 39, v0 and v1)", "ExpireDelegationToken API (key 40, v0 and v1)" and
 *      "DescribeDelegationToken API (key 41, v0 and v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DelegationToken::class)]
#[CoversClass(TokenInformation::class)]
#[CoversClass(KafkaPrincipal::class)]
#[CoversClass(CreateDelegationTokenRequest::class)]
#[CoversClass(CreateDelegationTokenResponse::class)]
#[CoversClass(RenewDelegationTokenRequest::class)]
#[CoversClass(RenewDelegationTokenResponse::class)]
#[CoversClass(ExpireDelegationTokenRequest::class)]
#[CoversClass(ExpireDelegationTokenResponse::class)]
#[CoversClass(DescribeDelegationTokenRequest::class)]
#[CoversClass(DescribeDelegationTokenResponse::class)]
#[CoversClass(DescribeDelegationTokenResponseToken::class)]
final class DelegationTokenApiTest extends IntegrationTestCase
{
    /**
     * Client id sent along with every request of this test class
     */
    private const string CLIENT_ID = 'kafka-client-t7-tokens';

    /**
     * The two users of `docker/kafka-2.8.2/jaas.conf`
     */
    private const string OWNER_USER = 'kafkatest';

    private const string OWNER_PASSWORD = 'kafkatest-secret';

    private const string OTHER_USER = 'admin';

    private const string OTHER_PASSWORD = 'admin-secret';

    /**
     * `delegation.token.expiry.time.ms` of a broker that configures none, i.e. the default renewal period
     */
    private const int DEFAULT_EXPIRY_MS = 86400000;

    /**
     * `delegation.token.max.lifetime.ms` of a broker that configures none
     */
    private const int DEFAULT_MAX_LIFETIME_MS = 604800000;

    /**
     * Maximum lifetime this class asks for, one hour - well below both defaults
     */
    private const int MAX_LIFETIME_MS = 3600000;

    /**
     * Hmacs of the tokens this test created, removed again in the tear down
     *
     * @var list<string>
     */
    private array $createdTokens = [];

    protected function setUp(): void
    {
        parent::setUp();

        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(
                self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, the token apis need an authenticated channel'
            );
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->createdTokens as $hmac) {
            try {
                $this->adminClient()->expireDelegationToken($hmac);
            } catch (DelegationTokenNotFoundException | DelegationTokenExpiredException) {
                // Already gone, or past its expiry and therefore beyond the reach of the api - see the class docblock
            }
        }
        $this->createdTokens = [];

        ConnectionFactory::closeAll();
    }

    /**
     * The two SASL listeners of the container, both of which authenticate a real principal
     *
     * @return iterable<string, array{string}>
     */
    public static function saslListeners(): iterable
    {
        yield 'SASL over plaintext' => [SecurityProtocol::SASL_PLAINTEXT];
        yield 'SASL over TLS'       => [SecurityProtocol::SASL_SSL];
    }

    /**
     * The whole life of a token on both SASL listeners: create, describe, renew, expire.
     *
     * `SASL_SSL` carries the very same frames inside the TLS channel, which matters here more than elsewhere: the
     * hmac of a token is a secret that a `SASL_PLAINTEXT` listener puts on the wire in clear text.
     */
    #[DataProvider('saslListeners')]
    public function testATokenIsIssuedRenewedAndExpiredOverAnAuthenticatedChannel(string $listener): void
    {
        $admin   = $this->adminClient($listener);
        $renewer = KafkaPrincipal::user(self::OTHER_USER);

        $before = self::now();
        $token  = $admin->createDelegationToken([$renewer], self::MAX_LIFETIME_MS);
        $after  = self::now();
        $this->createdTokens[] = $token->hmac;

        $information = $token->tokenInformation;
        self::assertSame('User:' . self::OWNER_USER, $information->ownerAsString(), 'the principal of the channel');
        self::assertNotSame('', $information->tokenId);
        self::assertSame(64, strlen($token->hmac), 'the HmacSHA512 of the token id');
        self::assertSame($token->hmac, base64_decode($token->hmacAsBase64String(), true));
        self::assertSame([(string) $renewer], $information->renewersAsString());

        self::assertGreaterThanOrEqual($before, $information->issueTimestamp, 'the clock of the broker');
        self::assertLessThanOrEqual($after, $information->issueTimestamp);
        self::assertSame(
            $information->issueTimestamp + self::MAX_LIFETIME_MS,
            $information->maxTimestamp,
            'the maximum lifetime is a period counted from the moment the token was issued'
        );
        self::assertSame(
            min($information->maxTimestamp, $information->issueTimestamp + self::DEFAULT_EXPIRY_MS),
            $information->expiryTimestamp,
            'and the first expiry is the earlier of that maximum and the default renewal period of the broker'
        );

        $described = $admin->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]);
        self::assertArrayHasKey($information->tokenId, $described);
        self::assertSame(
            $token->hmac,
            $described[$information->tokenId]->hmac,
            'a described token carries its secret as well, so it can be renewed and expired'
        );
        self::assertSame(
            [(string) $renewer],
            $described[$information->tokenId]->tokenInformation->renewersAsString(),
            'and the renewers that the create answer did not repeat'
        );

        $beforeRenew = self::now();
        $expiry      = $admin->renewDelegationToken($token->hmac, 600000);
        $afterRenew  = self::now();

        self::assertGreaterThanOrEqual($beforeRenew + 600000, $expiry, 'the expiry is now plus the period asked for');
        self::assertLessThanOrEqual($afterRenew + 600000, $expiry);
        self::assertLessThanOrEqual($information->maxTimestamp, $expiry);

        $beforeExpire = self::now();
        $expired      = $admin->expireDelegationToken($token->hmac);
        $afterExpire  = self::now();

        self::assertGreaterThanOrEqual($beforeExpire, $expired, 'a deleted token answers the clock of the broker');
        self::assertLessThanOrEqual($afterExpire, $expired);
        self::assertArrayNotHasKey(
            $information->tokenId,
            $admin->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]),
            'the token is gone from the cache of the broker'
        );
    }

    public function testARenewalIsCappedAtTheMaximumLifetimeOfTheToken(): void
    {
        $admin = $this->adminClient();
        $token = $admin->createDelegationToken([], self::MAX_LIFETIME_MS);
        $this->createdTokens[] = $token->hmac;

        $maximum = $token->tokenInformation->maxTimestamp;

        self::assertSame(
            $maximum,
            $admin->renewDelegationToken($token->hmac, self::DEFAULT_MAX_LIFETIME_MS),
            'a renewal for seven days on a token whose maximum is an hour away stops at that maximum'
        );
        self::assertSame(
            $maximum,
            $admin->renewDelegationToken($token->hmac, RenewDelegationTokenRequest::DEFAULT_RENEW_TIME_PERIOD),
            'and so does the default renewal period of the broker, which is 24 hours'
        );
    }

    public function testANonNegativePeriodOnlyMovesTheExpiryAndLeavesTheTokenInPlace(): void
    {
        $admin = $this->adminClient();
        $token = $admin->createDelegationToken([], self::MAX_LIFETIME_MS);
        $this->createdTokens[] = $token->hmac;

        $before = self::now();
        $expiry = $admin->expireDelegationToken($token->hmac, 120000);
        $after  = self::now();

        self::assertGreaterThanOrEqual($before + 120000, $expiry);
        self::assertLessThanOrEqual($after + 120000, $expiry);

        $described = $admin->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]);
        self::assertArrayHasKey($token->tokenId(), $described, 'the token is still there');
        self::assertSame(
            $expiry,
            $described[$token->tokenId()]->tokenInformation->expiryTimestamp,
            'and it carries the expiry the answer reported'
        );
    }

    public function testExpiringATokenTwiceIsTheNotFoundCodeAndNotTheExpiredOne(): void
    {
        $admin = $this->adminClient();
        $token = $admin->createDelegationToken([], self::MAX_LIFETIME_MS);

        $admin->expireDelegationToken($token->hmac);

        // 62, not 66: for the broker the token is not "expired", it is gone from ZooKeeper and from the cache
        $this->expectException(DelegationTokenNotFoundException::class);

        $admin->expireDelegationToken($token->hmac);
    }

    public function testAnHmacNoTokenOfTheClusterHasIsTheNotFoundCode(): void
    {
        $this->expectException(DelegationTokenNotFoundException::class);

        $this->adminClient()->renewDelegationToken(random_bytes(64), 600000);
    }

    public function testOnlyTheOwnerAndTheRenewersOfATokenMayRenewIt(): void
    {
        $owner = $this->adminClient();

        $withRenewer = $owner->createDelegationToken([KafkaPrincipal::user(self::OTHER_USER)], self::MAX_LIFETIME_MS);
        $withoutOne  = $owner->createDelegationToken([], self::MAX_LIFETIME_MS);
        $this->createdTokens[] = $withRenewer->hmac;
        $this->createdTokens[] = $withoutOne->hmac;

        $other = $this->adminClient(SecurityProtocol::SASL_PLAINTEXT, self::OTHER_USER, self::OTHER_PASSWORD);

        self::assertGreaterThan(
            0,
            $other->renewDelegationToken($withRenewer->hmac, 600000),
            'a principal the token names as a renewer may renew it'
        );

        $this->expectException(DelegationTokenOwnerMismatchException::class);

        $other->renewDelegationToken($withoutOne->hmac, 600000);
    }

    public function testATokenThatRanOutOfItsMaximumLifetimeCanNeitherBeRenewedNorExpired(): void
    {
        // A maximum lifetime of one millisecond: the token is past its expiry before the answer arrives here, and
        // there is no api call that removes it afterwards - only the sweeper of the broker does, every
        // `delegation.token.expiry.check.interval.ms`
        $token = $this->adminClient()->createDelegationToken([], 1);

        self::assertSame(
            $token->tokenInformation->issueTimestamp + 1,
            $token->tokenInformation->maxTimestamp,
            'the broker accepts a maximum lifetime of a single millisecond'
        );
        self::assertSame($token->tokenInformation->maxTimestamp, $token->tokenInformation->expiryTimestamp);

        usleep(50000);

        try {
            $this->adminClient()->renewDelegationToken($token->hmac, 600000);
            self::fail('a token that ran out of its maximum lifetime can not be renewed');
        } catch (DelegationTokenExpiredException) {
            self::assertTrue(true);
        }

        $this->expectException(DelegationTokenExpiredException::class);

        $this->adminClient()->expireDelegationToken($token->hmac);
    }

    public function testARenewerWhoseTypeIsNotUserIsRefusedAndNoTokenIsCreated(): void
    {
        $admin  = $this->adminClient();
        $before = $admin->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]);

        try {
            $admin->createDelegationToken([new KafkaPrincipal('Group', 'analytics')], self::MAX_LIFETIME_MS);
            self::fail('a renewer that is not a User has to be refused');
        } catch (InvalidPrincipalTypeException) {
            self::assertTrue(true);
        }

        $after = $admin->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]);
        self::assertSame(
            [],
            array_values(array_diff(array_keys($after), array_keys($before))),
            'the request is refused before a token is issued'
        );
    }

    public function testAnEmptyOwnerArrayAsksForNothingAndIsNotTheSameAsANullOne(): void
    {
        $admin = $this->adminClient();
        $token = $admin->createDelegationToken([], self::MAX_LIFETIME_MS);
        $this->createdTokens[] = $token->hmac;

        self::assertSame([], $admin->describeDelegationToken([]), 'an empty array asks for no token at all');
        self::assertArrayHasKey(
            $token->tokenId(),
            $admin->describeDelegationToken(),
            'a null array asks for every token the caller may see'
        );
        self::assertSame(
            [],
            $admin->describeDelegationToken([KafkaPrincipal::user('nobody-' . bin2hex(random_bytes(4)))]),
            'and an owner without a token is answered with an empty array, not with an error'
        );
    }

    /**
     * Without an authorizer the broker lets every authenticated principal describe every token, hmac included.
     *
     * `DelegationTokenManager.filterToken` falls back to `authorize(session, Describe, DelegationToken(tokenId))`,
     * and `KafkaApis.authorize` is `authorizer.forall(...)` - which is `true` when the broker has no authorizer at
     * all, as the container has none. Renewing is not affected: it checks owner-or-renewer only.
     */
    public function testWithoutAnAuthorizerEveryPrincipalCanDescribeEveryToken(): void
    {
        $token = $this->adminClient()->createDelegationToken([], self::MAX_LIFETIME_MS);
        $this->createdTokens[] = $token->hmac;

        $other = $this->adminClient(SecurityProtocol::SASL_PLAINTEXT, self::OTHER_USER, self::OTHER_PASSWORD);

        $visible = $other->describeDelegationToken();
        self::assertArrayHasKey($token->tokenId(), $visible, 'a foreign token is visible without an authorizer');
        self::assertSame($token->hmac, $visible[$token->tokenId()]->hmac);

        self::assertSame(
            [],
            $other->describeDelegationToken([KafkaPrincipal::user(self::OTHER_USER)]),
            'naming itself as the owner filters the foreign token out again'
        );
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function tokenApiCalls(): iterable
    {
        yield 'createDelegationToken'   => ['create'];
        yield 'renewDelegationToken'    => ['renew'];
        yield 'expireDelegationToken'   => ['expire'];
        yield 'describeDelegationToken' => ['describe'];
    }

    /**
     * All four apis are refused with 64 on a channel that authenticated nobody, before the body is even read.
     */
    #[DataProvider('tokenApiCalls')]
    public function testEveryTokenApiIsRefusedOnThePlaintextListener(string $call): void
    {
        $admin = $this->adminClient(SecurityProtocol::PLAINTEXT);

        $this->expectException(UnsupportedByAuthenticationException::class);

        match ($call) {
            'create'   => $admin->createDelegationToken(),
            'renew'    => $admin->renewDelegationToken(random_bytes(64), 600000),
            'expire'   => $admin->expireDelegationToken(random_bytes(64)),
            'describe' => $admin->describeDelegationToken(),
        };
    }

    /**
     * Builds an admin client whose connections authenticate as the given user on the given listener
     */
    private function adminClient(
        string $securityProtocol = SecurityProtocol::SASL_PLAINTEXT,
        string $user = self::OWNER_USER,
        string $password = self::OWNER_PASSWORD
    ): AdminClient {
        $configuration = $this->configurationFor($securityProtocol, $user, $password);

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * The client configuration that reaches the listener of the given security protocol
     *
     * @return array<string, mixed>
     */
    private function configurationFor(string $securityProtocol, string $user, string $password): array
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . $this->listenerFor($securityProtocol)],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];

        if ($securityProtocol === SecurityProtocol::PLAINTEXT) {
            return $configuration;
        }

        $configuration += [
            ClientConfig::SECURITY_PROTOCOL => $securityProtocol,
            ClientConfig::SASL_MECHANISM    => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME     => $user,
            ClientConfig::SASL_PASSWORD     => $password,
        ];

        if ($securityProtocol === SecurityProtocol::SASL_SSL) {
            $configuration[ClientConfig::SSL_CA_CERT_LOCATION] = self::brokerCertificateFile();
        }

        return $configuration;
    }

    /**
     * Returns the listener of the given security protocol, skipping the test when it is not configured
     */
    private function listenerFor(string $securityProtocol): string
    {
        if ($securityProtocol === SecurityProtocol::PLAINTEXT) {
            return self::firstBootstrapServer();
        }
        if ($securityProtocol !== SecurityProtocol::SASL_SSL) {
            return self::saslBootstrapServer();
        }

        if (self::saslSslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_SSL_BOOTSTRAP_SERVERS_ENV . ' is not set');
        }
        if (!extension_loaded('openssl')) {
            self::markTestSkipped('The openssl extension is required for security.protocol = SASL_SSL');
        }
        if (!is_readable(self::brokerCertificateFile())) {
            self::markTestSkipped('The certificate of the test broker is not available');
        }

        return self::saslSslBootstrapServer();
    }

    /**
     * The current wall clock in the milliseconds the broker answers with
     */
    private static function now(): int
    {
        return (int) (microtime(true) * 1000);
    }
}

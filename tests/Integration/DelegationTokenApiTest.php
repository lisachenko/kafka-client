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
use Protocol\Kafka\Common\Errors\DelegationTokenAuthorizationException;
use Protocol\Kafka\Common\Errors\DelegationTokenExpiredException;
use Protocol\Kafka\Common\Errors\DelegationTokenNotFoundException;
use Protocol\Kafka\Common\Errors\DelegationTokenOwnerMismatchException;
use Protocol\Kafka\Common\Errors\InvalidPrincipalTypeException;
use Protocol\Kafka\Common\Errors\KafkaException;
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
 * Exercises the four delegation token apis (keys 38 to 41) against the Kafka 3.9.2 KRaft node of this line.
 *
 * KIP-48 issues a token to the principal of the **connection**, so every one of these tests runs over a SASL
 * listener of the container, where that principal is the SASL/PLAIN user - `User:kafkatest` for the client under
 * test and `User:admin` for the second one, which is what the owner-mismatch cases need. The container carries a
 * `delegation.token.secret.key`; without it every call would be the error code 61 instead.
 *
 * **KIP-900 moved the four apis to the KRaft controller** (Kafka 3.6): `DelegationTokenControlManager` writes a
 * `DelegationTokenRecord` or a `RemoveDelegationTokenRecord` to the metadata log, and the token cache that every
 * later request is answered from - the controller's own and the one of every broker - is filled by the
 * `DelegationTokenPublisher` when that record is *replayed*, which happens after the answer of the request that
 * wrote it has already reached the client. A token is therefore **not yet there for a moment**: measured on the
 * node, a create is followed by about 66 ms in which a renew or an expire of that very token answers 62
 * (`DelegationTokenNotFound`) and about 77 ms in which a describe does not list it. Every test of this class waits
 * for the token instead of assuming it ({@see self::awaitToken()}, {@see self::awaitRenewable()}); a fixed sleep
 * would be a guess, and the container is shared with three other suites.
 *
 * Every token this class creates is expired again. The token whose maximum lifetime is a single millisecond can be
 * expired as well on this node, where a 2.8.2 broker answered 66 for it - see
 * {@see self::testATokenThatRanOutOfItsMaximumLifetimeCanNotBeRenewedButCanStillBeExpired()}.
 *
 * @see docs/protocol/4.3.md, sections "Delegation tokens (KIP-48)", "CreateDelegationToken API (key 38, v0 to v3)",
 *      "RenewDelegationToken API (key 39, v0 to v2)", "ExpireDelegationToken API (key 40, v0 to v2)" and
 *      "DescribeDelegationToken API (key 41, v0 to v3)"
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
     * The two users of `docker/kafka-4.3.1/jaas.conf`
     */
    private const string OWNER_USER = 'kafkatest';

    private const string OWNER_PASSWORD = 'kafkatest-secret';

    private const string OTHER_USER = 'admin';

    private const string OTHER_PASSWORD = 'admin-secret';

    /**
     * The one SASL user of the node that `super.users` does not list, i.e. the one the authorizer really decides for
     */
    private const string UNPRIVILEGED_USER = 'acltest';

    private const string UNPRIVILEGED_PASSWORD = 'acltest-secret';

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

    /**
     * Tokens this test asked for on behalf of {@see self::UNPRIVILEGED_USER} (KIP-373), as token id => hmac
     *
     * They are expired over the connection of that user, because the controller of a KRaft node lets the owner
     * and the renewers of a token expire it and **not** the principal that asked for it - and the tear down waits
     * until they are really gone, or the next test of the class sees them in the token cache of the broker.
     *
     * @var array<string, string>
     */
    private array $createdForeignTokens = [];

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

        $owner = $this->adminClient(
            SecurityProtocol::SASL_PLAINTEXT,
            self::UNPRIVILEGED_USER,
            self::UNPRIVILEGED_PASSWORD
        );
        foreach ($this->createdForeignTokens as $tokenId => $hmac) {
            try {
                $owner->expireDelegationToken($hmac);
                self::awaitTokenGone($owner, $tokenId);
            } catch (DelegationTokenNotFoundException | DelegationTokenExpiredException) {
                // The same best effort: the node must not keep a token of this suite, whatever a test did
            }
        }
        $this->createdForeignTokens = [];

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

        $described = self::awaitToken($admin, $information->tokenId, [KafkaPrincipal::user(self::OWNER_USER)]);
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
        $expiry      = self::awaitRenewable($admin, $token->hmac, 600000);
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
            self::awaitTokenGone($admin, $information->tokenId, [KafkaPrincipal::user(self::OWNER_USER)]),
            'the token is gone from the token cache of the node once the removal record is replayed'
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
            self::awaitRenewable($admin, $token->hmac, self::DEFAULT_MAX_LIFETIME_MS),
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

        self::awaitRenewable($admin, $token->hmac, 600000);

        $before = self::now();
        $expiry = $admin->expireDelegationToken($token->hmac, 120000);
        $after  = self::now();

        self::assertGreaterThanOrEqual($before + 120000, $expiry);
        self::assertLessThanOrEqual($after + 120000, $expiry);

        $described = self::awaitExpiry($admin, $token->tokenId(), $expiry);
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

        self::awaitRenewable($admin, $token->hmac, 600000);
        $admin->expireDelegationToken($token->hmac);

        // 62, not 66: for the controller the token is not "expired", the RemoveDelegationTokenRecord took it out
        // of the metadata log and out of the token cache. The 4.3.1 node answered a second expire that followed the
        // first one immediately with 0 in the baseline of the 4.x line - the controller looks the hmac up in a
        // `DelegationTokenCache` (`DelegationTokenControlManager` @ 4.3.1), which may learn of the removal a moment
        // after the first answer - so the second expire is repeated for a bounded while, and only the 62 ends it
        $deadline = microtime(true) + 10.0;
        do {
            try {
                $admin->expireDelegationToken($token->hmac);
            } catch (DelegationTokenNotFoundException $notFound) {
                self::assertSame(KafkaException::DELEGATION_TOKEN_NOT_FOUND, $notFound->getCode());

                return;
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail('an expired token must end as the 62 of a token the cluster does not have');
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

    public function testATokenThatRanOutOfItsMaximumLifetimeCanNotBeRenewedButCanStillBeExpired(): void
    {
        // A maximum lifetime of one millisecond: the token is past its expiry before the answer arrives here. On
        // the 2.8.2 ZooKeeper broker it was then beyond the reach of the protocol altogether - a renew and an
        // expire both answered 66 - and only the sweeper removed it. `DelegationTokenControlManager` @ 3.9.2
        // orders the checks of `expireDelegationToken` differently: the **negative** period is handled before the
        // expiry test, so an immediate expiry of an expired token writes the RemoveDelegationTokenRecord and
        // answers the clock of the node. A renew still answers 66, which is checked before the ownership.
        $admin = $this->adminClient();
        $token = $admin->createDelegationToken([], 1);

        self::assertSame(
            $token->tokenInformation->issueTimestamp + 1,
            $token->tokenInformation->maxTimestamp,
            'the node accepts a maximum lifetime of a single millisecond'
        );
        self::assertSame($token->tokenInformation->maxTimestamp, $token->tokenInformation->expiryTimestamp);

        $deadline = microtime(true) + 15.0;
        do {
            try {
                $admin->renewDelegationToken($token->hmac, 600000);
                self::fail('a token that ran out of its maximum lifetime can not be renewed');
            } catch (DelegationTokenExpiredException) {
                break;
            } catch (DelegationTokenNotFoundException) {
                // The DelegationTokenRecord of the create has not been replayed yet - see the class docblock
                usleep(20000);
            }
        } while (microtime(true) < $deadline);

        $expired = $admin->expireDelegationToken($token->hmac);

        self::assertGreaterThan(
            $token->tokenInformation->maxTimestamp,
            $expired,
            'an immediate expiry of an expired token is accepted on a KRaft node and answers the clock of the node'
        );

        $this->expectException(DelegationTokenNotFoundException::class);

        $admin->expireDelegationToken($token->hmac);
    }

    public function testARenewerWhoseTypeIsNotUserIsRefusedAndNoTokenIsCreated(): void
    {
        $admin   = $this->adminClient();
        $renewer = new KafkaPrincipal('Group', 'analytics');

        try {
            $admin->createDelegationToken([$renewer], self::MAX_LIFETIME_MS);
            self::fail('a renewer that is not a User has to be refused');
        } catch (InvalidPrincipalTypeException) {
            self::assertTrue(true);
        }

        // Every suite of this line authenticates as the same `User:kafkatest`, so the tokens of the owner are a
        // shared resource: a token another suite issued between the two describes of this test is visible here.
        // What the refusal really promises is that no token of this owner carries the renewer it named.
        foreach ($admin->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]) as $tokenId => $token) {
            self::assertNotContains(
                (string) $renewer,
                $token->tokenInformation->renewersAsString(),
                "the refused request issued the token {$tokenId} after all"
            );
        }
    }

    public function testAnEmptyOwnerArrayAsksForNothingAndIsNotTheSameAsANullOne(): void
    {
        $admin = $this->adminClient();
        $token = $admin->createDelegationToken([], self::MAX_LIFETIME_MS);
        $this->createdTokens[] = $token->hmac;
        self::awaitToken($admin, $token->tokenId(), [KafkaPrincipal::user(self::OWNER_USER)]);

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
     * A super user describes every token of the node, an ordinary principal only the ones it owns or may renew.
     *
     * The 2.x line measured this on a broker **without an authorizer**, where `KafkaApis.authorize` is
     * `authorizer.forall(...)` and therefore `true` for everybody, so every authenticated principal saw every
     * token. The node of this line runs the `StandardAuthorizer`, and
     * `DelegationTokenManager.filterToken` @ 3.9.2 asks it for `DESCRIBE_TOKENS` on the `User` resource of the
     * owner first and falls back to "owner or renewer" only when that is refused. `super.users` decides the
     * question here: `admin` and `kafkatest` are super users and see everything, while `acltest` - the one SASL
     * user of `docker/kafka-4.3.1/jaas.conf` that is not listed there - sees its own tokens and nothing else, and
     * is answered with an **empty array rather than an error** for a foreign owner.
     *
     * Renewing is not affected by any of it: it checks owner-or-renewer only.
     */
    public function testASuperUserDescribesEveryTokenAndAnOrdinaryPrincipalOnlyItsOwn(): void
    {
        $token = $this->adminClient()->createDelegationToken([], self::MAX_LIFETIME_MS);
        $this->createdTokens[] = $token->hmac;

        $superUser = $this->adminClient(SecurityProtocol::SASL_PLAINTEXT, self::OTHER_USER, self::OTHER_PASSWORD);

        $visible = self::awaitToken($superUser, $token->tokenId());
        self::assertArrayHasKey($token->tokenId(), $visible, 'a super user sees a token of another owner');
        self::assertSame($token->hmac, $visible[$token->tokenId()]->hmac, 'with its hmac');

        self::assertArrayNotHasKey(
            $token->tokenId(),
            $superUser->describeDelegationToken([KafkaPrincipal::user(self::OTHER_USER)]),
            'naming itself as the owner filters a token it neither owns nor may renew out again - a superset'
            . ' assertion, because the tokens of the SASL users are shared with every other suite of the node'
        );

        $ordinary = $this->adminClient(SecurityProtocol::SASL_PLAINTEXT, self::UNPRIVILEGED_USER, self::UNPRIVILEGED_PASSWORD);

        self::assertArrayNotHasKey(
            $token->tokenId(),
            $ordinary->describeDelegationToken(),
            'a principal the authorizer applies to does not see a token it neither owns nor may renew'
        );
        $ofTheForeignOwner = $ordinary->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]);

        self::assertArrayNotHasKey(
            $token->tokenId(),
            $ofTheForeignOwner,
            'and asking for a foreign owner is the code 0 with that token filtered out, not an error - `acltest`'
            . ' neither owns nor may renew a token of `kafkatest`'
        );
        foreach ($ofTheForeignOwner as $visible) {
            // A superset assertion, because the tokens of the SASL users are shared with every other suite of the
            // node: since KIP-373 an owner filter matches the REQUESTER of a token as well, so a token that
            // `kafkatest` asked for on behalf of `acltest` is a legitimate answer here - what may never show up
            // is a token that `acltest` neither owns nor asked for nor may renew
            self::assertTrue(
                $visible->tokenInformation->ownerOrRenewer(KafkaPrincipal::user(self::UNPRIVILEGED_USER)),
                'a token the calling principal has nothing to do with was answered'
            );
        }
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
     * The owner principal of KIP-373 (Kafka 3.3): a token that belongs to somebody else than the caller
     *
     * The version 3 of CreateDelegationToken carries the owner, the version 3 of its answer and of a described
     * token the **requester**, and the two are only different when a request asked for them to be. A super user
     * needs no acl for it - `kafkatest` is one of the `super.users` of the node - while anybody else needs
     * `CREATE_TOKENS` on the `USER` resource of the owner.
     */
    public function testATokenCanBeIssuedForAnotherPrincipal(): void
    {
        $admin = $this->adminClient();

        $token = $admin->createDelegationToken([], self::MAX_LIFETIME_MS, KafkaPrincipal::user(self::UNPRIVILEGED_USER));
        $this->createdForeignTokens[$token->tokenId()] = $token->hmac;

        $information = $token->tokenInformation;
        self::assertSame('User:' . self::UNPRIVILEGED_USER, $information->ownerAsString(), 'the owner of KIP-373');
        self::assertSame(
            'User:' . self::OWNER_USER,
            $information->tokenRequesterAsString(),
            'and the principal of the connection as the requester the version 3 added to the answer'
        );
        self::assertTrue($information->isIssuedForAnotherPrincipal());
        self::assertSame([], $information->renewersAsString(), 'this request named no renewer at all');

        // The requester counts as an owner for the describe filter of the broker, so it sees the token itself
        $described = self::awaitToken($admin, $token->tokenId());
        self::assertSame(
            'User:' . self::UNPRIVILEGED_USER,
            $described[$token->tokenId()]->tokenInformation->ownerAsString()
        );
        self::assertSame(
            'User:' . self::OWNER_USER,
            $described[$token->tokenId()]->tokenInformation->tokenRequesterAsString(),
            'a described token names its requester since the version 3 as well'
        );

        // and the owner sees it over its own connection
        $owner = $this->adminClient(
            SecurityProtocol::SASL_PLAINTEXT,
            self::UNPRIVILEGED_USER,
            self::UNPRIVILEGED_PASSWORD
        );
        $ofTheOwner = self::awaitToken($owner, $token->tokenId());
        self::assertSame(
            'User:' . self::OWNER_USER,
            $ofTheOwner[$token->tokenId()]->tokenInformation->tokenRequesterAsString()
        );
    }

    /**
     * A token of another owner is asked for by the owner filter of that owner, not by the one of the requester
     */
    public function testATokenOfAnotherPrincipalIsFoundByTheOwnerFilterOfItsOwner(): void
    {
        $admin = $this->adminClient();

        $token = $admin->createDelegationToken([], self::MAX_LIFETIME_MS, 'User:' . self::UNPRIVILEGED_USER);
        $this->createdForeignTokens[$token->tokenId()] = $token->hmac;

        $byOwner = self::awaitToken($admin, $token->tokenId(), [KafkaPrincipal::user(self::UNPRIVILEGED_USER)]);

        self::assertArrayHasKey($token->tokenId(), $byOwner);
        self::assertSame(
            'User:' . self::OWNER_USER,
            $byOwner[$token->tokenId()]->tokenInformation->tokenRequesterAsString()
        );
    }

    /**
     * The requester may see the token it asked for, but the controller of a KRaft node does not let it renew one
     *
     * `TokenInformation.ownerOrRenewer` @ 3.9.2 counts the requester, which is what the describe filter asks,
     * while `DelegationTokenControlManager.allowedToRenew` @ 3.9.2 - the controller half - is the owner and the
     * renewers alone. The two halves therefore disagree, and this is the measurement of it.
     */
    public function testTheRequesterOfATokenOfAnotherOwnerMayNotRenewOrExpireIt(): void
    {
        $requester = $this->adminClient();
        $owner     = $this->adminClient(
            SecurityProtocol::SASL_PLAINTEXT,
            self::UNPRIVILEGED_USER,
            self::UNPRIVILEGED_PASSWORD
        );

        $token = $requester->createDelegationToken([], self::MAX_LIFETIME_MS, KafkaPrincipal::user(self::UNPRIVILEGED_USER));
        $this->createdForeignTokens[$token->tokenId()] = $token->hmac;
        self::awaitToken($requester, $token->tokenId());

        try {
            $requester->renewDelegationToken($token->hmac, 600000);
            self::fail('The requester of the token was allowed to renew it');
        } catch (DelegationTokenOwnerMismatchException) {
            // 63: the controller of a KRaft node does not count the requester among the principals that may renew
        }

        try {
            $requester->expireDelegationToken($token->hmac);
            self::fail('The requester of the token was allowed to expire it');
        } catch (DelegationTokenOwnerMismatchException) {
            // the same 63 from the expire api
        }

        // The owner itself may do both, which is what ends this test and takes the token off the node
        $expiry = $owner->expireDelegationToken($token->hmac);
        self::assertLessThanOrEqual(self::now() + 1000, $expiry, 'an immediate expiry answers the clock of the broker');
        self::awaitTokenGone($owner, $token->tokenId());
    }

    /**
     * Asking for a token of a principal one has no `CREATE_TOKENS` acl for is the 65 of KIP-373
     */
    public function testAPrincipalWithoutTheCreateTokensAclMayNotAskForATokenOfAnotherOwner(): void
    {
        $unprivileged = $this->adminClient(
            SecurityProtocol::SASL_PLAINTEXT,
            self::UNPRIVILEGED_USER,
            self::UNPRIVILEGED_PASSWORD
        );

        try {
            $token = $unprivileged->createDelegationToken([], self::MAX_LIFETIME_MS, KafkaPrincipal::user(self::OTHER_USER));
            $this->createdTokens[] = $token->hmac;
            // Unreachable on this node: `acltest` has no CREATE_TOKENS acl on `User:admin`
            self::fail('A principal outside super.users issued a token for somebody else without an acl');
        } catch (DelegationTokenAuthorizationException $refused) {
            self::assertSame(
                KafkaException::DELEGATION_TOKEN_AUTHORIZATION_FAILED,
                $refused->getCode(),
                'the 65 of KIP-373, not the 31 the ACL apis answer'
            );
        }
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
     * Waits until the node lists the token of the given id and returns every token the client may see
     *
     * The create answer of a KRaft node arrives before the `DelegationTokenRecord` it wrote has been replayed into
     * the token caches, so a describe that follows it immediately does not list the token yet - about 77 ms of it,
     * measured on this node. The read is repeated until it does; nothing here sleeps for a fixed time.
     *
     * @return array<string, DelegationToken>
     */
    private static function awaitToken(AdminClient $admin, string $tokenId, ?array $owners = null): array
    {
        $deadline = microtime(true) + 15.0;
        do {
            $tokens = $admin->describeDelegationToken($owners);
            if (isset($tokens[$tokenId])) {
                return $tokens;
            }
            usleep(20000);
        } while (microtime(true) < $deadline);

        self::fail("The node did not list the token {$tokenId} in time");
    }

    /**
     * Waits until the node no longer lists the token of the given id and returns what it does list
     *
     * The mirror image of {@see self::awaitToken()}: a `RemoveDelegationTokenRecord` reaches the caches after the
     * answer of the expire request that wrote it.
     *
     * @param list<KafkaPrincipal|string>|null $owners Owners to ask for, null for every token the client may see
     *
     * @return array<string, DelegationToken>
     */
    private static function awaitTokenGone(AdminClient $admin, string $tokenId, ?array $owners = null): array
    {
        $deadline = microtime(true) + 15.0;
        do {
            $tokens = $admin->describeDelegationToken($owners);
            if (!isset($tokens[$tokenId])) {
                return $tokens;
            }
            usleep(20000);
        } while (microtime(true) < $deadline);

        self::fail("The node still listed the token {$tokenId} after it was expired");
    }

    /**
     * Renews a token as soon as the controller knows it, and returns the expiry it answered
     *
     * A renew of a token that was just created is answered with 62 until the controller has replayed its own
     * record - about 66 ms on this node - and that 62 is indistinguishable from the one of an hmac that names no
     * token at all, so it is the only code this retries.
     */
    private static function awaitRenewable(AdminClient $admin, string $hmac, int $renewTimePeriodMs): int
    {
        $deadline = microtime(true) + 15.0;
        do {
            try {
                return $admin->renewDelegationToken($hmac, $renewTimePeriodMs);
            } catch (DelegationTokenNotFoundException) {
                usleep(20000);
            }
        } while (microtime(true) < $deadline);

        self::fail('The controller did not accept a renewal of the token it had just issued');
    }

    /**
     * Waits until the token of the given id carries the expiry the answer of the api reported
     *
     * @return array<string, DelegationToken>
     */
    private static function awaitExpiry(AdminClient $admin, string $tokenId, int $expiryTimestamp): array
    {
        $deadline = microtime(true) + 15.0;
        do {
            $tokens = $admin->describeDelegationToken([KafkaPrincipal::user(self::OWNER_USER)]);
            if (($tokens[$tokenId] ?? null)?->tokenInformation->expiryTimestamp === $expiryTimestamp) {
                return $tokens;
            }
            usleep(20000);
        } while (microtime(true) < $deadline);

        self::fail("The node did not report the new expiry of the token {$tokenId} in time");
    }

    /**
     * The current wall clock in the milliseconds the broker answers with
     */
    private static function now(): int
    {
        return (int) (microtime(true) * 1000);
    }
}

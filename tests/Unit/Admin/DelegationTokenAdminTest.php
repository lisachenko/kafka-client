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

namespace Protocol\Kafka\Tests\Unit\Admin;

use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\DelegationToken;
use Protocol\Kafka\Admin\TokenInformation;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\DelegationTokenExpiredException;
use Protocol\Kafka\Common\Errors\DelegationTokenNotFoundException;
use Protocol\Kafka\Common\Errors\DelegationTokenOwnerMismatchException;
use Protocol\Kafka\Common\Errors\InvalidPrincipalTypeException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnsupportedByAuthenticationException;
use Protocol\Kafka\Common\Security\KafkaPrincipal;
use Protocol\Kafka\Protocol\Data\DescribeDelegationTokenResponseToken;
use Protocol\Kafka\Protocol\Request\AbstractRequest;
use Protocol\Kafka\Protocol\Request\CreateDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\DescribeDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\ExpireDelegationTokenRequest;
use Protocol\Kafka\Protocol\Request\RenewDelegationTokenRequest;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * Tests the four delegation-token methods of the AdminClient against the documented wire vectors.
 *
 * Every canned answer is a frame that the real 1.1.1 container sent, replayed by a scripted broker connection, so
 * this suite and the compliance suite cannot disagree about what the broker says. What is asserted here is the
 * mapping alone: which request the client builds, what it makes of the answer, and which exception each error
 * code becomes.
 *
 * @see docs/protocol/2.8.md, section "Delegation tokens (KIP-48)"
 * @see \Protocol\Kafka\Tests\Integration\DelegationTokenApiTest for the same calls against a real broker
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DelegationToken::class)]
#[CoversClass(TokenInformation::class)]
final class DelegationTokenAdminTest extends TestCase
{
    /**
     * Address the cluster is bootstrapped from
     */
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    /**
     * Address of the single broker that the metadata answer announces
     */
    private const string BROKER_ADDRESS = 'tcp://127.0.0.1:9092';

    /**
     * Token id of the version 0 vectors, i.e. the token that `User:kafkatest` created with the renewer `User:admin`
     */
    private const string TOKEN_ID = 'MopLsgz-Q1uJqTR8T9s4aQ';

    /**
     * Token id of the **version 2** answer, which is the frame the client reads today: a capture of its own, made
     * on the 2.8.2 container when the api was raised to the flexible v2 of Kafka 2.4
     */
    private const string TOKEN_ID_V2 = '72e565j9SJiE6_eIDUUeRA';

    /**
     * Token id of the **version 2** describe answer, captured with the token apis of Kafka 2.5
     */
    private const string DESCRIBED_TOKEN_ID_V2 = 'wG0KYNS6REW0U9AN8r_Bnw';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testCreateDelegationTokenReturnsTheTokenOfTheAnswerWithTheRenewersOfTheRequest(): void
    {
        $broker = $this->scriptBroker(self::vector('createdelegationtoken.response.v2'));

        $token = $this->adminClient()->createDelegationToken([KafkaPrincipal::user('admin')], 3600000);

        self::assertSame(self::TOKEN_ID_V2, $token->tokenId());
        self::assertSame('User:kafkatest', $token->tokenInformation->ownerAsString());
        self::assertSame(64, strlen($token->hmac), 'the HmacSHA512 of the token id is 64 bytes');
        self::assertStringEndsWith('==', $token->hmacAsBase64String());
        self::assertSame(1789141554021, $token->tokenInformation->issueTimestamp);
        self::assertSame(1789145154021, $token->tokenInformation->expiryTimestamp);
        self::assertSame(1789145154021, $token->tokenInformation->maxTimestamp);
        self::assertSame(
            ['User:admin'],
            $token->tokenInformation->renewersAsString(),
            'the answer does not repeat the renewers, so they come from the request'
        );

        self::assertSame(
            [self::requestFrame(
                new CreateDelegationTokenRequest(
                    [KafkaPrincipal::user('admin')],
                    3600000,
                    't7',
                    $broker->getReceivedCorrelationIds()[0]
                )
            )],
            $broker->getReceivedFrames(),
            'the renewers and the maximum lifetime of the call are what goes on the wire'
        );
    }

    public function testACreateAnswerWithAnErrorCodeBecomesTheExceptionOfThatCode(): void
    {
        $this->scriptBroker(self::vector('createdelegationtoken.response.v2.invalid-principal-type'));

        $this->expectException(InvalidPrincipalTypeException::class);

        $this->adminClient()->createDelegationToken([new KafkaPrincipal('Group', 'analytics')]);
    }

    public function testACreateOnAnUnauthenticatedChannelBecomesTheUnsupportedByAuthenticationException(): void
    {
        $this->scriptBroker(self::vector('createdelegationtoken.response.v2.not-allowed'));

        $this->expectException(UnsupportedByAuthenticationException::class);

        $this->adminClient()->createDelegationToken();
    }

    public function testRenewDelegationTokenReturnsTheNewExpiryTimestamp(): void
    {
        $broker = $this->scriptBroker(self::vector('renewdelegationtoken.response.v2'));

        $expiry = $this->adminClient()->renewDelegationToken(self::hmacOfTheVectors(), 600000);

        self::assertSame(1789153754840, $expiry);

        self::assertSame(
            [self::requestFrame(new RenewDelegationTokenRequest(
                self::hmacOfTheVectors(),
                600000,
                't7',
                $broker->getReceivedCorrelationIds()[0]
            ))],
            $broker->getReceivedFrames(),
            'the token is named by the raw bytes of its hmac'
        );
    }

    public function testARenewByAPrincipalThatMayNotBecomesTheOwnerMismatchException(): void
    {
        $this->scriptBroker(self::timestampAnswer(KafkaException::DELEGATION_TOKEN_OWNER_MISMATCH));

        $this->expectException(DelegationTokenOwnerMismatchException::class);

        $this->adminClient()->renewDelegationToken(self::hmacOfTheVectors(), 600000);
    }

    public function testARenewOfAnExpiredTokenBecomesTheExpiredException(): void
    {
        $this->scriptBroker(self::timestampAnswer(KafkaException::DELEGATION_TOKEN_EXPIRED));

        $this->expectException(DelegationTokenExpiredException::class);

        $this->adminClient()->renewDelegationToken(self::hmacOfTheVectors(), 600000);
    }

    public function testExpireDelegationTokenDefaultsToDeletingTheTokenAtOnce(): void
    {
        $broker = $this->scriptBroker(self::vector('expiredelegationtoken.response.v2'));

        $expiry = $this->adminClient()->expireDelegationToken(self::hmacOfTheVectors());

        self::assertSame(1789153154952, $expiry, 'the clock of the broker at the moment it deleted the token');

        self::assertSame(
            [self::requestFrame(new ExpireDelegationTokenRequest(
                self::hmacOfTheVectors(),
                ExpireDelegationTokenRequest::EXPIRE_IMMEDIATELY,
                't7',
                $broker->getReceivedCorrelationIds()[0]
            ))],
            $broker->getReceivedFrames(),
            'the default period is the negative one that deletes the token'
        );
    }

    public function testExpiringATokenThatIsAlreadyGoneBecomesTheNotFoundException(): void
    {
        $this->scriptBroker(self::timestampAnswer(KafkaException::DELEGATION_TOKEN_NOT_FOUND));

        $this->expectException(DelegationTokenNotFoundException::class);

        $this->adminClient()->expireDelegationToken(self::hmacOfTheVectors());
    }

    public function testDescribeDelegationTokenIndexesTheTokensByTheirId(): void
    {
        $broker = $this->scriptBroker(self::vector('describedelegationtoken.response.v2'));

        $tokens = $this->adminClient()->describeDelegationToken([KafkaPrincipal::user('admin')]);

        self::assertSame([self::DESCRIBED_TOKEN_ID_V2], array_keys($tokens));

        $token = $tokens[self::DESCRIBED_TOKEN_ID_V2];
        self::assertSame('User:admin', $token->tokenInformation->ownerAsString());
        self::assertSame(['User:kafkatest'], $token->tokenInformation->renewersAsString());
        self::assertSame(64, strlen($token->hmac), 'a described token carries its secret as well');

        self::assertSame(
            [self::requestFrame(new DescribeDelegationTokenRequest(
                [KafkaPrincipal::user('admin')],
                't7',
                $broker->getReceivedCorrelationIds()[0]
            ))],
            $broker->getReceivedFrames()
        );
    }

    public function testDescribeDelegationTokenAsksForEveryVisibleTokenByDefault(): void
    {
        $broker = $this->scriptBroker(self::vector('describedelegationtoken.response.v2'));

        $this->adminClient()->describeDelegationToken();

        self::assertSame(
            [self::requestFrame(
                new DescribeDelegationTokenRequest(null, 't7', $broker->getReceivedCorrelationIds()[0])
            )],
            $broker->getReceivedFrames(),
            'a null owner array is the default, not an empty one'
        );
    }

    public function testADescribeOnAnUnauthenticatedChannelBecomesTheUnsupportedByAuthenticationException(): void
    {
        $this->scriptBroker(self::describeAnswer(KafkaException::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED));

        $this->expectException(UnsupportedByAuthenticationException::class);

        $this->adminClient()->describeDelegationToken();
    }

    public function testATokenKnowsWhoMayRenewIt(): void
    {
        $information = new TokenInformation(
            'token-id',
            KafkaPrincipal::user('kafkatest'),
            [KafkaPrincipal::user('admin')],
            1,
            3,
            2
        );

        self::assertTrue($information->ownerOrRenewer(KafkaPrincipal::user('kafkatest')), 'the owner may');
        self::assertTrue($information->ownerOrRenewer(KafkaPrincipal::user('admin')), 'a renewer may');
        self::assertFalse($information->ownerOrRenewer(KafkaPrincipal::user('nobody')), 'nobody else may');
        self::assertSame(1, $information->issueTimestamp);
        self::assertSame(3, $information->maxTimestamp, 'the maximum stands before the expiry, as in Java');
        self::assertSame(2, $information->expiryTimestamp);
    }

    public function testATokenIsBuiltFromADescribedEntry(): void
    {
        $entry                  = new DescribeDelegationTokenResponseToken();
        $entry->owner           = KafkaPrincipal::user('kafkatest');
        $entry->issueTimestamp  = 1;
        $entry->expiryTimestamp = 2;
        $entry->maxTimestamp    = 3;
        $entry->tokenId         = 'token-id';
        $entry->hmac            = "\xde\xad\xbe\xef";
        $entry->renewers        = [KafkaPrincipal::user('admin')];

        $token = DelegationToken::fromResponseToken($entry);

        self::assertSame('token-id', $token->tokenId());
        self::assertSame('3q2+7w==', $token->hmacAsBase64String());
        self::assertSame(2, $token->tokenInformation->expiryTimestamp);
        self::assertSame(3, $token->tokenInformation->maxTimestamp);
        self::assertSame(['User:admin'], $token->tokenInformation->renewersAsString());
    }

    /**
     * Scripts the answers of the single broker of the cluster and installs the connections
     */
    private function scriptBroker(string ...$responses): BrokerConnection
    {
        $broker = new BrokerConnection(...$responses);
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection(self::clusterMetadata()))
            ->on(self::BROKER_ADDRESS, $broker)
            ->install();

        return $broker;
    }

    /**
     * Builds an admin client on the cluster that the scripted brokers answer for
     */
    private function adminClient(): AdminClient
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't7',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * The Metadata answer of the one-broker cluster these tests run against
     */
    private static function clusterMetadata(): string
    {
        return ResponseFrame::metadata(0, [[0, '127.0.0.1', 9092]]);
    }

    /**
     * Returns the frame of a request as a scripted broker records it, i.e. without the leading Size field
     */
    private static function requestFrame(AbstractRequest $request): string
    {
        return substr((string) $request, 4);
    }

    /**
     * Returns the raw hmac of the token the vectors were captured for
     */
    private static function hmacOfTheVectors(): string
    {
        foreach (VectorFile::read('delegation-tokens')['vectors'] as $vector) {
            if ($vector['id'] === 'createdelegationtoken.response.v0') {
                return (string) hex2bin((string) $vector['fields']['hmac']['$bytes']);
            }
        }

        self::fail('There is no create answer among the delegation token vectors');
    }

    /**
     * Builds the flexible answer of RenewDelegationToken and ExpireDelegationToken, whose frame is the same one.
     *
     * The error cases of the two apis were captured before Kafka 2.5 raised them to the flexible version 2, and an
     * answer that carries an error carries the timestamp **-1** and nothing else, so the frame is written here
     * instead of re-capturing four broker errors that the vectors of the version 0 already document.
     */
    private static function timestampAnswer(int $errorCode): string
    {
        return ResponseFrame::of(
            0,
            "\x00"                                  // the tagged-field section of the response header v1
            . pack('n', $errorCode)
            . pack('J', -1)                          // DelegationTokenManager.ErrorTimestamp
            . pack('N', 0)                           // throttle_time_ms, LAST in these apis
            . "\x00"                                 // the tagged-field section of the body
        );
    }

    /**
     * Builds the flexible answer of DescribeDelegationToken that carries an error and no token at all
     */
    private static function describeAnswer(int $errorCode): string
    {
        return ResponseFrame::of(
            0,
            "\x00"
            . pack('n', $errorCode)
            . "\x01"                                 // an empty compact array is a single byte
            . pack('N', 0)
            . "\x00"
        );
    }

    /**
     * Returns the raw frame of a documented wire vector of the delegation token apis
     */
    private static function vector(string $id): string
    {
        foreach (VectorFile::read('delegation-tokens')['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) hex2bin($vector['hex']);
            }
        }

        self::fail("There is no wire vector {$id} in docs/protocol/vectors/delegation-tokens.json");
    }
}

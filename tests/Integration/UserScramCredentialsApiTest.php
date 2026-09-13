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
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\FeatureUpdate;
use Protocol\Kafka\Admin\ScramMechanism;
use Protocol\Kafka\Admin\UserScramCredentialDeletion;
use Protocol\Kafka\Admin\UserScramCredentialUpsertion;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\DuplicateResourceException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\ResourceNotFoundException;
use Protocol\Kafka\Common\Errors\UnacceptableCredentialException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Request\AlterUserScramCredentialsRequest;
use Protocol\Kafka\Protocol\Request\AlterUserScramCredentialsResponse;
use Protocol\Kafka\Protocol\Request\DescribeUserScramCredentialsRequest;
use Protocol\Kafka\Protocol\Request\DescribeUserScramCredentialsResponse;
use Protocol\Kafka\Protocol\Request\UpdateFeaturesRequest;
use Throwable;

/**
 * Exercises the three apis Kafka 2.7 added - DescribeUserScramCredentials (50), AlterUserScramCredentials (51) and
 * UpdateFeatures (57) - against a real Kafka 2.8.2 broker.
 *
 * KIP-554 is the end of writing SCRAM users into ZooKeeper by hand, and the interesting half of it happens in the
 * **client**: the salted password of RFC 5802 is derived here and the password never reaches the wire. What this
 * class can not show is a *login* with such a credential - this client speaks SASL/PLAIN only, the same limit the
 * delegation tokens of KIP-48 carry.
 *
 * KIP-584 has nothing to finalize on a ZooKeeper-backed 2.8.2 cluster, so what is measured of UpdateFeatures is
 * the frame and the refusals.
 *
 * Every user of this class carries a `t1-scram-` prefix of its own and every credential it writes is removed again
 * in {@see self::tearDownAfterClass()}.
 *
 * @see docs/protocol/2.8.md, sections "DescribeUserScramCredentials API (key 50, v0)",
 *      "AlterUserScramCredentials API (key 51, v0)" and "UpdateFeatures API (key 57, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeUserScramCredentialsRequest::class)]
#[CoversClass(DescribeUserScramCredentialsResponse::class)]
#[CoversClass(AlterUserScramCredentialsRequest::class)]
#[CoversClass(AlterUserScramCredentialsResponse::class)]
#[CoversClass(UpdateFeaturesRequest::class)]
#[CoversClass(ScramMechanism::class)]
final class UserScramCredentialsApiTest extends IntegrationTestCase
{
    /**
     * Users this class wrote a credential for, cleaned up when it is done
     *
     * @var list<string>
     */
    private static array $users = [];

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Removes every credential this class wrote from the shared container
     */
    public static function tearDownAfterClass(): void
    {
        $users       = self::$users;
        self::$users = [];

        if (self::bootstrapServers() === [] || $users === []) {
            return;
        }

        try {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID          => 'kafka-client-t1-scram',
                ClientConfig::REQUEST_TIMEOUT_MS => 20000,
            ];
            $admin     = new AdminClient(Cluster::bootstrap($configuration), $configuration);
            $deletions = [];
            // Only the credentials a user really has may be named: the changes of one user are applied
            // all-or-nothing, so a deletion of a mechanism they do not have would fail the whole entry
            foreach ($admin->describeUserScramCredentials($users) as $user => $description) {
                foreach ($description->credentialInfos as $credential) {
                    $deletions[] = new UserScramCredentialDeletion($user, $credential->mechanism);
                }
            }
            if ($deletions !== []) {
                $admin->alterUserScramCredentials($deletions);
            }
        } catch (Throwable) {
            // A broker that is gone or busy is not a failure of these tests - every user name is unique per run
        }
    }

    /**
     * A credential written with 51 is described by 50, and the answer names the mechanism and the iterations only
     */
    public function testACredentialIsWrittenAndDescribedWithoutItsPassword(): void
    {
        $user = $this->user('roundtrip');

        $result = $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 8192),
        ]);

        self::assertSame([$user => null], $result);

        $described = $this->admin->describeUserScramCredentials([$user]);

        self::assertArrayHasKey($user, $described);
        self::assertTrue($described[$user]->has(ScramMechanism::ScramSha256));
        self::assertFalse($described[$user]->has(ScramMechanism::ScramSha512));
        self::assertSame(8192, $described[$user]->credentialFor(ScramMechanism::ScramSha256)?->iterations);
        self::assertStringNotContainsString(
            't1-secret',
            serialize($described),
            'nothing the broker sends back is derived from the password'
        );
    }

    /**
     * A user may hold one credential per mechanism, and removing one leaves the other alone
     */
    public function testAUserCanHoldOneCredentialPerMechanism(): void
    {
        $user = $this->user('two');

        $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 4096),
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha512, 't1-secret', 4096),
        ]);

        $described = $this->admin->describeUserScramCredentials([$user]);

        self::assertTrue($described[$user]->has(ScramMechanism::ScramSha256));
        self::assertTrue($described[$user]->has(ScramMechanism::ScramSha512));

        $this->admin->alterUserScramCredentials([
            new UserScramCredentialDeletion($user, ScramMechanism::ScramSha256),
        ]);

        $described = $this->admin->describeUserScramCredentials([$user]);

        self::assertFalse($described[$user]->has(ScramMechanism::ScramSha256), 'the one that was deleted is gone');
        self::assertTrue($described[$user]->has(ScramMechanism::ScramSha512), 'and the other one is untouched');
    }

    /**
     * The null user array and the empty one both ask for every user that has a credential
     */
    public function testTheNullAndTheEmptyUserArrayBothDescribeEveryUser(): void
    {
        $user = $this->user('everybody');
        $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 4096),
        ]);

        $fromNull  = $this->admin->describeUserScramCredentials(null);
        $fromEmpty = $this->admin->describeUserScramCredentials([]);

        self::assertArrayHasKey($user, $fromNull);
        self::assertSame(
            array_keys($fromNull),
            array_keys($fromEmpty),
            'the specification says "null/empty to describe all users", and the broker agrees'
        );
    }

    /**
     * A user without a credential is answered with 91 per user, not left out of the answer
     */
    public function testAUserWithoutACredentialIsAnsweredWithResourceNotFound(): void
    {
        $unknown  = 't1-scram-nobody-' . bin2hex(random_bytes(4));
        $response = $this->describeRaw([$unknown]);

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'the top level is fine');
        self::assertSame(KafkaException::RESOURCE_NOT_FOUND, $response->results[$unknown]->errorCode);
        self::assertStringContainsString(
            'Attempt to describe a user credential that does not exist',
            (string) $response->results[$unknown]->errorMessage
        );
        self::assertSame(
            [],
            $this->admin->describeUserScramCredentials([$unknown]),
            'and the admin client turns that entry into an absent key'
        );
    }

    /**
     * An iteration count below the minimum of the mechanism is 93
     */
    public function testTooFewIterationsAreRefusedWithUnacceptableCredential(): void
    {
        $user = $this->user('iterations');

        $result = $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 1024),
        ]);

        self::assertInstanceOf(UnacceptableCredentialException::class, $result[$user]);
        self::assertSame(KafkaException::UNACCEPTABLE_CREDENTIAL, $result[$user]->getCode());
        self::assertStringContainsString('Too few iterations', $result[$user]->getMessage());
        self::assertSame([], $this->admin->describeUserScramCredentials([$user]), 'and nothing was written');
    }

    /**
     * The same user and mechanism twice in one request is 92, and the answer still carries one entry
     */
    public function testTheSameCredentialTwiceInOneRequestIsADuplicate(): void
    {
        $user = $this->user('duplicate');

        $result = $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 4096),
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-other', 4096),
        ]);

        self::assertSame([$user], array_keys($result), 'the results of this api are per user, not per change');
        self::assertInstanceOf(DuplicateResourceException::class, $result[$user]);
        self::assertSame(KafkaException::DUPLICATE_RESOURCE, $result[$user]->getCode());
    }

    /**
     * Deleting a credential a user does not have is 91, the same code the describe api uses
     */
    public function testDeletingACredentialThatIsNotThereIsResourceNotFound(): void
    {
        $user = $this->user('nodelete');

        $result = $this->admin->alterUserScramCredentials([
            new UserScramCredentialDeletion($user, ScramMechanism::ScramSha512),
        ]);

        self::assertInstanceOf(ResourceNotFoundException::class, $result[$user]);
        self::assertStringContainsString(
            'Attempt to delete a user credential that does not exist',
            $result[$user]->getMessage()
        );
    }

    /**
     * The changes of one user are applied all-or-nothing: one impossible change fails the whole entry
     */
    public function testOneImpossibleChangeFailsEveryChangeOfThatUser(): void
    {
        $user = $this->user('atomic');

        $result = $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 4096),
            new UserScramCredentialDeletion($user, ScramMechanism::ScramSha512),
        ]);

        self::assertInstanceOf(ResourceNotFoundException::class, $result[$user], 'the deletion cannot be done');
        self::assertSame(
            [],
            $this->admin->describeUserScramCredentials([$user]),
            'and the upsertion of the same request was not written either'
        );
    }

    /**
     * A ZooKeeper-backed 2.8.2 cluster finalizes no feature, and says so through the ApiVersions v3 answer
     */
    public function testTheClusterOfThisContainerFinalizesNoFeature(): void
    {
        $features = $this->admin->describeFeatures();

        self::assertSame([], $features->supportedFeatures, 'a 2.8.2 broker on ZooKeeper supports no feature');
        self::assertSame([], $features->finalizedFeatures);
        self::assertSame(0, $features->finalizedFeaturesEpoch, 'and the epoch of that empty agreement is 0');
    }

    /**
     * Every feature update is refused per feature, because there is no feature to update
     */
    public function testAFeatureUpdateIsRefusedPerFeature(): void
    {
        $result = $this->admin->updateFeatures([new FeatureUpdate('t1-nonsense', 1)]);

        self::assertInstanceOf(InvalidRequestException::class, $result['t1-nonsense']);
        self::assertSame(KafkaException::INVALID_REQUEST, $result['t1-nonsense']->getCode());
        self::assertStringContainsString(
            'the provided feature is not supported',
            $result['t1-nonsense']->getMessage()
        );

        $deletion = $this->admin->updateFeatures([FeatureUpdate::delete('t1-nonsense')]);

        self::assertInstanceOf(InvalidRequestException::class, $deletion['t1-nonsense']);
        self::assertStringContainsString(
            'Can not delete non-existing finalized feature',
            $deletion['t1-nonsense']->getMessage()
        );
    }

    /**
     * An empty update list is not refused at all, although the Java admin client refuses it before sending
     */
    public function testAnEmptyFeatureUpdateListIsAnsweredWithNothing(): void
    {
        self::assertSame(
            [],
            $this->admin->updateFeatures([]),
            'the controller iterates an empty collection and answers the top-level 0 with no result'
        );
    }

    /**
     * Sends a DescribeUserScramCredentials request and returns the answer untouched
     *
     * @param list<string> $users
     */
    private function describeRaw(array $users): DescribeUserScramCredentialsResponse
    {
        $stream  = $this->connect();
        $request = new DescribeUserScramCredentialsRequest($users, 'kafka-client-t1-scram', 8101);
        $request->writeTo($stream);
        $size = $stream->read('NmessageSize')['messageSize'];
        $body = $stream->read("a{$size}data")['data'];

        return DescribeUserScramCredentialsResponse::unpack(new StringStream(pack('N', $size) . $body));
    }

    /**
     * Names a user of this run and remembers it for the cleanup
     */
    private function user(string $kind): string
    {
        return self::$users[] = 't1-scram-' . $kind . '-' . bin2hex(random_bytes(4));
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 'kafka-client-t1-scram',
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}

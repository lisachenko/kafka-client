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
use Protocol\Kafka\Admin\UserScramCredentialsDescription;
use Protocol\Kafka\Admin\UserScramCredentialUpsertion;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\DuplicateResourceException;
use Protocol\Kafka\Common\Errors\InvalidUpdateVersionException;
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
 * UpdateFeatures (57) - against the Kafka 3.9.2 KRaft node of this line.
 *
 * KIP-554 is the end of writing SCRAM users into ZooKeeper by hand, and the interesting half of it happens in the
 * **client**: the salted password of RFC 5802 is derived here and the password never reaches the wire. What this
 * class can not show is a *login* with such a credential - this client speaks SASL/PLAIN only, the same limit the
 * delegation tokens of KIP-48 carry.
 *
 * Both apis are answered by the **KRaft controller** on this line: `ScramControlManager` writes a
 * `UserScramCredentialRecord` or a `RemoveUserScramCredentialRecord` to the metadata log and the describe reads
 * the metadata image of the broker, which is a moment behind it (about 30 ms, measured on the node). A read that
 * follows a write therefore polls ({@see self::awaitCredentials()}) instead of assuming the answer is already there.
 *
 * Two changes of the **same user** in one request are refused with 92 on this node even when they name different
 * mechanisms, where a 2.8.2 broker took one change per user *and* mechanism: `ScramControlManager.alterCredentials`
 * @ 3.9.2 keys its `userToUpsert`/`userToDeletion` maps by the user name alone.
 *
 * KIP-584 is not empty here either: a KRaft node **finalizes** `metadata.version`, and UpdateFeatures is answered
 * by a controller that knows the feature - see {@see self::testTheNodeFinalizesItsMetadataVersion()}.
 *
 * Every user of this class carries a `t1-scram-` prefix of its own and every credential it writes is removed again
 * in {@see self::tearDownAfterClass()}.
 *
 * @see docs/protocol/4.3.md, sections "DescribeUserScramCredentials API (key 50, v0)",
 *      "AlterUserScramCredentials API (key 51, v0)" and "UpdateFeatures API (key 57, v0 to v2)"
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

        $described = self::awaitCredentials($this->admin, $user);

        self::assertTrue($described->has(ScramMechanism::ScramSha256));
        self::assertFalse($described->has(ScramMechanism::ScramSha512));
        self::assertSame(8192, $described->credentialFor(ScramMechanism::ScramSha256)?->iterations);
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

        // One request per mechanism: `ScramControlManager.alterCredentials` @ 3.9.2 keys the changes it collects
        // by the **user name** alone, so the two upsertions of one user that a 2.8.2 broker applied together are
        // "A user credential cannot be altered twice in the same request" here - the 92 of
        // {@see self::testTwoChangesOfOneUserInOneRequestAreADuplicateWhateverTheMechanism()}
        $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 4096),
        ]);
        $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha512, 't1-secret', 4096),
        ]);

        $described = self::awaitCredentials(
            $this->admin,
            $user,
            static fn(UserScramCredentialsDescription $it): bool => $it->has(ScramMechanism::ScramSha512)
        );

        self::assertTrue($described->has(ScramMechanism::ScramSha256));
        self::assertTrue($described->has(ScramMechanism::ScramSha512));

        $this->admin->alterUserScramCredentials([
            new UserScramCredentialDeletion($user, ScramMechanism::ScramSha256),
        ]);

        $described = self::awaitCredentials(
            $this->admin,
            $user,
            static fn(UserScramCredentialsDescription $it): bool => !$it->has(ScramMechanism::ScramSha256)
        );

        self::assertFalse($described->has(ScramMechanism::ScramSha256), 'the one that was deleted is gone');
        self::assertTrue($described->has(ScramMechanism::ScramSha512), 'and the other one is untouched');
    }

    /**
     * Two changes of one user in one request are 92, whatever the mechanisms are
     */
    public function testTwoChangesOfOneUserInOneRequestAreADuplicateWhateverTheMechanism(): void
    {
        $user = $this->user('two-in-one');

        $result = $this->admin->alterUserScramCredentials([
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha256, 't1-secret', 4096),
            new UserScramCredentialUpsertion($user, ScramMechanism::ScramSha512, 't1-secret', 4096),
        ]);

        self::assertInstanceOf(DuplicateResourceException::class, $result[$user]);
        self::assertSame(KafkaException::DUPLICATE_RESOURCE, $result[$user]->getCode());
        self::assertSame(
            'A user credential cannot be altered twice in the same request',
            $result[$user]->getContext()['error'],
            'the controller does not look at the mechanism at all'
        );
        self::assertSame([], $this->admin->describeUserScramCredentials([$user]), 'and nothing was written');
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
        self::awaitCredentials($this->admin, $user);

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
     * The node finalizes its seven features, and says so through the tagged fields of the ApiVersions answer
     *
     * A ZooKeeper-backed 2.8.2 cluster finalized nothing at all, and the 3.9.2 node of the 3.x line finalized
     * `metadata.version` alone. The 4.3.1 node of this line was formatted `--standalone` with the defaults of the
     * release, and the **ApiVersions v4** this client sends carries every feature it supports and every feature it
     * finalized: `metadata.version` from the level 7 (`3.3-IV3`, the lowest a 4.x node accepts) to 30 (`4.3-IV0`),
     * finalized at 30, and six features finalized above 0 - `kraft.version` 1 among them, the dynamic quorum of
     * KIP-853, which is why it is in the finalized map now and was not on the static quorum of 3.9.2. The epoch is
     * the **offset of the metadata log** at which the agreement was written, so it grows with every write the
     * cluster does and can only be asserted to be positive.
     */
    public function testTheNodeFinalizesItsMetadataVersion(): void
    {
        $features = $this->admin->describeFeatures();
        $expected = [
            'eligible.leader.replicas.version' => [0, 1, 1],
            'group.version'                    => [0, 1, 1],
            'kraft.version'                    => [0, 1, 1],
            'metadata.version'                 => [7, 30, 30],
            'share.version'                    => [0, 1, 1],
            'streams.version'                  => [0, 1, 1],
            'transaction.version'              => [0, 2, 2],
        ];

        $supported = array_keys($features->supportedFeatures);
        $finalized = array_keys($features->finalizedFeatures);
        sort($supported);
        sort($finalized);

        self::assertSame(array_keys($expected), $supported, 'the seven features a 4.3.1 node supports');
        self::assertSame(array_keys($expected), $finalized, 'and every one of them is finalized above the level 0');

        foreach ($expected as $name => [$minVersion, $maxVersion, $level]) {
            self::assertSame($minVersion, $features->supportedFeatures[$name]->minVersion, $name);
            self::assertSame($maxVersion, $features->supportedFeatures[$name]->maxVersion, $name);
            self::assertSame($level, $features->finalizedFeatures[$name]->minVersionLevel, $name);
            self::assertSame($level, $features->finalizedFeatures[$name]->maxVersionLevel, $name);
        }

        self::assertGreaterThan(
            0,
            $features->finalizedFeaturesEpoch,
            'the epoch is the offset of the metadata log, which is beyond 0 as soon as the cluster was formatted'
        );
    }

    /**
     * A feature update the controller cannot make refuses the whole request, with 95 and the reason
     *
     * On the 2.8.2 broker every update was the 42 (`InvalidRequest`) of a cluster that finalizes nothing, and the
     * 3.9.2 controller answered **95** (`InvalidUpdateVersion`) per feature. `QuorumController.updateFeatures` @
     * 4.0.0 applies the updates atomically: the first feature it refuses is the top-level 95 of the whole request,
     * behind the sentence `The update failed for all features since the following feature had an error:`, and
     * `AdminClient::updateFeatures()` throws it. A **deletion** of a feature no controller knows - the level 0 that
     * the 3.9.2 controller accepted and wrote - is refused as well: `Feature.featureFromName` @ 4.0.0 knows the
     * features of the release and nothing else.
     */
    public function testAFeatureUpdateIsRefusedAsAWhole(): void
    {
        $prefix  = 'The update failed for all features since the following feature had an error: ';
        $refusal = function (FeatureUpdate $update): KafkaException {
            try {
                $this->admin->updateFeatures([$update]);
            } catch (KafkaException $exception) {
                return $exception;
            }

            self::fail('the controller accepted ' . $update->feature);
        };

        $unknown = $refusal(new FeatureUpdate('t1-nonsense', 1));

        self::assertInstanceOf(InvalidUpdateVersionException::class, $unknown);
        self::assertSame(KafkaException::INVALID_UPDATE_VERSION, $unknown->getCode());
        self::assertSame(
            $prefix . 'Invalid update version 1 for feature t1-nonsense. Local controller 1 does not support this'
            . ' feature.',
            $unknown->getContext()['error']
        );

        self::assertSame(
            $prefix . 'Invalid update version 99 for feature metadata.version. Local controller 1 only supports'
            . ' versions 7-30',
            $refusal(new FeatureUpdate('metadata.version', 99))->getContext()['error']
        );

        self::assertSame(
            $prefix . 'Invalid update version 1 for feature metadata.version. Local controller 1 only supports'
            . ' versions 7-30',
            $refusal(new FeatureUpdate('metadata.version', 1))->getContext()['error'],
            'the level 1 of 3.9.2 is below the range of a 4.x controller, which starts at 3.3-IV3'
        );

        self::assertSame(
            $prefix . 'Invalid update version 29 for feature metadata.version. Can\'t downgrade the version of this'
            . ' feature without setting the upgrade type to either safe or unsafe downgrade.',
            $refusal(new FeatureUpdate('metadata.version', 29))->getContext()['error'],
            'the `upgrade_type` of KIP-778 that would allow it is the version 1 of the api, which Kafka 3.3 added'
        );

        self::assertSame(
            $prefix . 'Invalid update version 0 for feature t1-nonsense. Feature t1-nonsense not found.',
            $refusal(FeatureUpdate::delete('t1-nonsense'))->getContext()['error'],
            'and the deletion of a feature no controller knows is refused, where 3.9.2 wrote it'
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
     * Waits until the metadata image of the node carries the credentials of the given user and returns them
     *
     * AlterUserScramCredentials is answered by the controller once its record is committed; the describe api reads
     * the metadata image of the broker, which replays that record a moment later - about 30 ms on this node. The
     * optional predicate says which state of the entry is the one that is being waited for; without it, the mere
     * presence of the user is.
     *
     * @param (callable(UserScramCredentialsDescription): bool)|null $until
     */
    private static function awaitCredentials(
        AdminClient $admin,
        string $user,
        ?callable $until = null
    ): UserScramCredentialsDescription {
        $deadline = microtime(true) + 15.0;
        do {
            $description = $admin->describeUserScramCredentials([$user])[$user] ?? null;
            if ($description !== null && ($until === null || $until($description))) {
                return $description;
            }
            usleep(20000);
        } while (microtime(true) < $deadline);

        self::fail("The node did not describe the credentials of {$user} in time");
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

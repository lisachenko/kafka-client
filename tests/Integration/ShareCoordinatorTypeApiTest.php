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
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\CoordinatorLookup;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\IO\Stream;
use Protocol\Kafka\Protocol\Data\FindCoordinatorResponseCoordinator;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequest;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequestV4;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorRequestV5;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponse;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponseV4;
use Protocol\Kafka\Protocol\Request\GroupCoordinatorResponseV5;
use RuntimeException;

/**
 * What Kafka 3.9 added to the group apis, against the 4.3.1 KRaft node: **FindCoordinator v6** (KIP-932).
 *
 * The version adds no field to either half of the api - *"Version 6 adds support for share groups (KIP-932)"*
 * stands over `FindCoordinatorRequest.json` and `FindCoordinatorResponse.json` @ 3.9.2 and the field lists below
 * it are the ones of version 4. What it adds is a value the `coordinator_type` byte may take: `CoordinatorType`
 * @ 3.9.2 is `GROUP(0)`, `TRANSACTION(1)`, `SHARE(2)`, and the type 2 is refused with the error code 42 while
 * `apiVersion < 6`.
 *
 * The 3.9.2 node of the 3.x line answered every legal share lookup the **15** `CoordinatorNotAvailable`, because
 * it had no share coordinator; a 4.3.1 node has one (`share.version` 1), and what it answers is decided by the key.
 * A share coordinator is not the coordinator of a GROUP but of one **share partition**: `KafkaApis.getCoordinator`
 * @ 4.3.1 validates the key with `SharePartitionKey.validate`, which takes `<group id>:<topic id>:<partition>`
 * only, and answers anything else the **42** `InvalidRequest`; a valid key is hashed onto a partition of the
 * internal `__share_group_state` and answered its leader - the 15 of a node that has not created that topic yet
 * is the retriable one of a first lookup. Share groups themselves are the 4.1 wave of this line. Four things are
 * driven here:
 *
 * * the version gate, at the versions 4, 5 and 6 of the same frame, which puts it exactly at the 6;
 * * the 42 of a key that is not a share-partition key, and the coordinator of one that is;
 * * {@see CoordinatorLookup}, which waits out the 15 and reports the 42 at once;
 * * the three resources the three types are authorized against, measured with the SASL user `acltest`, the one
 *   principal of the image that `super.users` does not name.
 *
 * Every key of this class is named `t3-39-share-…`; not one of them is ever created, because a coordinator
 * lookup registers nothing - the broker only hashes the key onto a partition of an internal topic.
 *
 * @see docs/protocol/4.3.md, section "GroupCoordinator API (key 10, v0 to v6)"
 */
#[CoversClass(GroupCoordinatorRequest::class)]
#[CoversClass(GroupCoordinatorRequestV4::class)]
#[CoversClass(GroupCoordinatorRequestV5::class)]
#[CoversClass(GroupCoordinatorResponse::class)]
#[CoversClass(GroupCoordinatorResponseV4::class)]
#[CoversClass(GroupCoordinatorResponseV5::class)]
#[CoversClass(FindCoordinatorResponseCoordinator::class)]
#[CoversClass(CoordinatorLookup::class)]
final class ShareCoordinatorTypeApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t3-39-share';

    /**
     * A coordinator type that `CoordinatorType` @ 3.9.2 does not define at all
     */
    private const int UNKNOWN_COORDINATOR_TYPE = 99;

    /**
     * The one SASL user of the image that `super.users` does not name
     */
    private const string UNPRIVILEGED_USER = 'acltest';

    private const string UNPRIVILEGED_PASSWORD = 'acltest-secret';

    private const int REQUEST_TIMEOUT_MS = 30000;

    private string $key;

    protected function setUp(): void
    {
        parent::setUp();

        $this->key = 't3-39-share-' . bin2hex(random_bytes(4));
    }

    /**
     * The version gate of `KafkaApis.getCoordinator` @ 3.9.2 is exact: the very same frame is the 42 at the
     * versions 4 and 5 and something else entirely at the version 6
     */
    public function testTheShareCoordinatorTypeIsRefusedBelowVersionSix(): void
    {
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);

        $frames = [
            [4, GroupCoordinatorRequestV4::class, GroupCoordinatorResponseV4::class],
            [5, GroupCoordinatorRequestV5::class, GroupCoordinatorResponseV5::class],
        ];
        foreach ($frames as [$version, $requestClass, $responseClass]) {
            $entry = $this->lookup($stream, $requestClass, $responseClass, 4001);

            self::assertSame(
                KafkaException::INVALID_REQUEST,
                $entry->errorCode,
                "the version {$version} may not ask for the coordinator type 2"
            );
            self::assertSame('', $entry->errorMessage, 'the refusal comes out of the ordinary handler');
            self::assertSame(-1, $entry->nodeId);
            self::assertSame('', $entry->host);
            self::assertSame(-1, $entry->port);
        }
    }

    /**
     * At the version the type exists for, a key that is not a share-partition key is the 42 of the key validation
     */
    public function testVersionSixRefusesAKeyThatIsNotASharePartitionKeyWithTheFortyTwo(): void
    {
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);

        foreach ([$this->key, $this->key . ':not-a-uuid:0', $this->sharePartitionKey() . 'x'] as $index => $key) {
            GroupCoordinatorRequest::forKeys(
                [$key],
                GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE,
                self::CLIENT_ID,
                4002 + $index
            )->writeTo($stream);
            $entry = GroupCoordinatorResponse::unpack($stream)->coordinatorOf($key);

            self::assertSame(
                KafkaException::INVALID_REQUEST,
                $entry->errorCode,
                "`SharePartitionKey.validate` @ 4.3.1 wants <group>:<topic id>:<partition>, {$key} is not one"
            );
            self::assertSame('', $entry->errorMessage, 'the refusal comes out of the ordinary handler');
            self::assertSame(-1, $entry->nodeId, 'with the placeholder coordinator of an error');
            self::assertSame('', $entry->host);
            self::assertSame(-1, $entry->port);
        }
    }

    /**
     * A share-partition key is answered the node that leads its partition of `__share_group_state`
     */
    public function testVersionSixAnswersASharePartitionKeyWithItsCoordinator(): void
    {
        $key   = $this->sharePartitionKey();
        $entry = $this->shareCoordinatorOf($key);

        self::assertSame(KafkaException::NO_ERROR, $entry->errorCode);
        self::assertSame('', $entry->errorMessage);
        self::assertGreaterThanOrEqual(0, $entry->nodeId, 'the share coordinator of KIP-932 exists on a 4.x node');
        self::assertNotSame('', $entry->host);
        self::assertGreaterThan(0, $entry->port);
    }

    /**
     * The type is one field in front of the batch, so every key of a share lookup gets its own answer
     */
    public function testEveryKeyOfAShareBatchIsAnsweredSeparately(): void
    {
        // the internal topic of the share coordinator exists once one lookup has been answered
        $this->shareCoordinatorOf($this->sharePartitionKey());

        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);
        $keys   = [$this->sharePartitionKey(0), $this->sharePartitionKey(1), $this->key];

        GroupCoordinatorRequest::forKeys($keys, GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE, self::CLIENT_ID, 4010)
            ->writeTo($stream);
        $answer = GroupCoordinatorResponse::unpack($stream);

        self::assertSame($keys, array_keys($answer->coordinators), 'every key of the batch is answered');
        self::assertSame(KafkaException::NO_ERROR, $answer->coordinators[$keys[0]]->errorCode);
        self::assertSame(KafkaException::NO_ERROR, $answer->coordinators[$keys[1]]->errorCode);
        self::assertSame(
            KafkaException::INVALID_REQUEST,
            $answer->coordinators[$keys[2]]->errorCode,
            'and the plain group id of the same batch is refused on its own'
        );
    }

    /**
     * The version says nothing about the types the enum of the broker does not define
     */
    public function testATypeTheEnumDoesNotKnowIsStillRefusedAtVersionSix(): void
    {
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);

        GroupCoordinatorRequest::forKeys(
            [$this->key],
            self::UNKNOWN_COORDINATOR_TYPE,
            self::CLIENT_ID,
            4004
        )->writeTo($stream);
        $entry = GroupCoordinatorResponse::unpack($stream)->coordinatorOf($this->key);

        self::assertSame(KafkaException::INVALID_REQUEST, $entry->errorCode);
        self::assertNotSame(
            '',
            $entry->errorMessage,
            'this 42 is built by the error path, which fills the message from `Errors.message()`'
        );
        self::assertSame(-1, $entry->nodeId);
    }

    /**
     * Nothing else changed: the two types a client of this package asks for answer what they answered at v5
     */
    public function testTheOrdinaryLookupsAreUnchangedAtVersionSix(): void
    {
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);

        foreach (
            [
                GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP,
                GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION,
            ] as $coordinatorType
        ) {
            $sixth = $this->lookup(
                $stream,
                GroupCoordinatorRequest::class,
                GroupCoordinatorResponse::class,
                4005,
                $coordinatorType
            );
            $fifth = $this->lookup(
                $stream,
                GroupCoordinatorRequestV5::class,
                GroupCoordinatorResponseV5::class,
                4006,
                $coordinatorType
            );

            self::assertSame(KafkaException::NO_ERROR, $sixth->errorCode);
            self::assertSame($fifth->nodeId, $sixth->nodeId, 'the same coordinator as at the version below');
            self::assertSame($fifth->host, $sixth->host);
            self::assertSame($fifth->port, $sixth->port);
            self::assertSame('', $sixth->errorMessage, 'still the empty message of the batched handler');
        }
    }

    /**
     * {@see CoordinatorLookup} finds the share coordinator of a share-partition key - waiting out the retriable 15
     * of a node that is still creating `__share_group_state` - and reports the 42 of any other key at once
     */
    public function testTheCoordinatorLookupFindsTheShareCoordinatorOfASharePartitionKey(): void
    {
        $lookup = new CoordinatorLookup(
            Cluster::bootstrap($this->configuration()),
            $this->configuration()
        );

        $node = $lookup->findCoordinator(
            $this->sharePartitionKey(),
            GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE,
            self::REQUEST_TIMEOUT_MS
        );

        self::assertGreaterThanOrEqual(0, $node->nodeId);

        $this->expectException(InvalidRequestException::class);

        $lookup->findCoordinator($this->key, GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE, 300);
    }

    /**
     * The three coordinator types are authorized against three different resources, which the one principal of
     * the image that is not a super user shows in one exchange per type
     */
    public function testEachCoordinatorTypeIsAuthorizedAgainstItsOwnResource(): void
    {
        $stream = $this->unprivileged();

        $expected = [
            GroupCoordinatorRequest::COORDINATOR_TYPE_GROUP       => KafkaException::GROUP_AUTHORIZATION_FAILED,
            GroupCoordinatorRequest::COORDINATOR_TYPE_TRANSACTION =>
                KafkaException::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE       => KafkaException::CLUSTER_AUTHORIZATION_FAILED,
        ];

        foreach ($expected as $coordinatorType => $errorCode) {
            $entry = $this->lookup(
                $stream,
                GroupCoordinatorRequest::class,
                GroupCoordinatorResponse::class,
                4007,
                $coordinatorType
            );

            self::assertSame(
                $errorCode,
                $entry->errorCode,
                "the coordinator type {$coordinatorType} is refused with its own authorization code"
            );
            self::assertSame(-1, $entry->nodeId, 'a per-key refusal, never a top-level one');
        }
    }

    /**
     * `getCoordinator` @ 3.9.2 checks the version gate in front of the `CLUSTER_ACTION` of a share lookup, so an
     * unprivileged principal is told about the version and not about the acl
     */
    public function testTheVersionGateIsCheckedBeforeTheAuthorization(): void
    {
        $entry = $this->lookup(
            $this->unprivileged(),
            GroupCoordinatorRequestV5::class,
            GroupCoordinatorResponseV5::class,
            4008
        );

        self::assertSame(KafkaException::INVALID_REQUEST, $entry->errorCode);
    }

    /**
     * Builds a share-partition key of the group of this test: `<group id>:<topic id>:<partition>`
     *
     * The topic id is a random one - the coordinator validates the format and hashes the key, it does not look the
     * topic up - in the URL-safe base64 of Kafka's `Uuid.toString()`, which `Uuid.fromString` parses back.
     */
    private function sharePartitionKey(int $partition = 0): string
    {
        static $topicId = null;
        $topicId ??= rtrim(strtr(base64_encode(random_bytes(16)), '+/', '-_'), '=');

        return "{$this->key}:{$topicId}:{$partition}";
    }

    /**
     * Looks the share coordinator of a key up until the node has created `__share_group_state` and answers it
     */
    private function shareCoordinatorOf(string $key): FindCoordinatorResponseCoordinator
    {
        $stream   = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);
        $deadline = microtime(true) + self::REQUEST_TIMEOUT_MS / 1000;

        while (true) {
            GroupCoordinatorRequest::forKeys(
                [$key],
                GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE,
                self::CLIENT_ID,
                4020
            )->writeTo($stream);
            $entry = GroupCoordinatorResponse::unpack($stream)->coordinatorOf($key);
            if ($entry->errorCode !== KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE || microtime(true) > $deadline) {
                return $entry;
            }
            usleep(250000);
        }
    }

    /**
     * Sends one lookup of the given classes and returns the entry of {@see self::$key}
     *
     * @param class-string<GroupCoordinatorRequest>  $requestClass
     * @param class-string<GroupCoordinatorResponse> $responseClass
     */
    private function lookup(
        Stream $stream,
        string $requestClass,
        string $responseClass,
        int $correlationId,
        int $coordinatorType = GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE
    ): FindCoordinatorResponseCoordinator {
        $requestClass::forKeys([$this->key], $coordinatorType, self::CLIENT_ID, $correlationId)->writeTo($stream);

        return $responseClass::unpack($stream)->coordinatorOf($this->key);
    }

    /**
     * A connection of the one SASL user the authorizer of the node has no acl for
     */
    private function unprivileged(): Stream
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM    => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME     => self::UNPRIVILEGED_USER,
            ClientConfig::SASL_PASSWORD     => self::UNPRIVILEGED_PASSWORD,
        ] + $this->configuration();

        foreach (Cluster::bootstrap($configuration)->nodes() as $node) {
            return $node->getConnection($configuration);
        }

        throw new RuntimeException('the SASL listener advertised no broker');
    }

    /**
     * Client configuration pointing at the broker under test
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => self::REQUEST_TIMEOUT_MS,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS,
            ClientConfig::RETRY_BACKOFF_MS          => 100,
        ];
    }
}

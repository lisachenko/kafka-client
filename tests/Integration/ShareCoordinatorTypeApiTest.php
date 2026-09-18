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
use Protocol\Kafka\Common\Errors\GroupCoordinatorNotAvailableException;
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
 * What Kafka 3.9 added to the group apis, against the 3.9.2 KRaft node: **FindCoordinator v6** (KIP-932).
 *
 * The version adds no field to either half of the api - *"Version 6 adds support for share groups (KIP-932)"*
 * stands over `FindCoordinatorRequest.json` and `FindCoordinatorResponse.json` @ 3.9.2 and the field lists below
 * it are the ones of version 4. What it adds is a value the `coordinator_type` byte may take: `CoordinatorType`
 * @ 3.9.2 is `GROUP(0)`, `TRANSACTION(1)`, `SHARE(2)`, and the type 2 is refused with the error code 42 while
 * `apiVersion < 6`.
 *
 * Share groups themselves are **out of this line** by the owner's decision, so
 * {@see GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE} is a constant and this class is what it is for: the
 * measurement of what the node really answers a legal share lookup, which is not what reading the KIP would
 * suggest. Three things are driven here:
 *
 * * the version gate, at the versions 4, 5 and 6 of the same frame, which puts it exactly at the 6;
 * * the **15** `CoordinatorNotAvailable` a 3.9.2 node answers a type 2 lookup it is allowed to serve - a
 *   *retriable* code that means "never", because the share coordinator arrives with Kafka 4;
 * * the three resources the three types are authorized against, measured with the SASL user `acltest`, the one
 *   principal of the image that `super.users` does not name.
 *
 * Every key of this class is named `t3-39-share-…`; not one of them is ever created, because a coordinator
 * lookup registers nothing - the broker only hashes the key onto a partition of an internal topic.
 *
 * @see docs/protocol/3.9.md, section "GroupCoordinator API (key 10, v0 to v6)"
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
     * At the version the type exists for, the node answers the 15 of a coordinator that is not there at all
     */
    public function testVersionSixAnswersAShareLookupWithTheCoordinatorNotAvailable(): void
    {
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);

        $entry = $this->lookup($stream, GroupCoordinatorRequest::class, GroupCoordinatorResponse::class, 4002);

        self::assertSame(
            KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE,
            $entry->errorCode,
            'the `CoordinatorType.SHARE` branch of `getCoordinator` @ 3.9.2 returns the 15 and nothing else: '
            . 'the share coordinator of KIP-932 does not exist on a 3.9.2 node'
        );
        self::assertSame('', $entry->errorMessage);
        self::assertSame(-1, $entry->nodeId, 'with the placeholder coordinator of an error');
        self::assertSame('', $entry->host);
        self::assertSame(-1, $entry->port);
    }

    /**
     * The type is one field in front of the batch, so every key of a share lookup gets the same treatment
     */
    public function testEveryKeyOfAShareBatchIsAnsweredSeparately(): void
    {
        $stream = $this->connect([ClientConfig::REQUEST_TIMEOUT_MS => self::REQUEST_TIMEOUT_MS]);
        $keys   = [$this->key, $this->key . '-second'];

        GroupCoordinatorRequest::forKeys($keys, GroupCoordinatorRequest::COORDINATOR_TYPE_SHARE, self::CLIENT_ID, 4003)
            ->writeTo($stream);
        $answer = GroupCoordinatorResponse::unpack($stream);

        self::assertSame($keys, array_keys($answer->coordinators), 'every key of the batch is answered');
        foreach ($answer->coordinators as $entry) {
            self::assertSame(KafkaException::GROUP_COORDINATOR_NOT_AVAILABLE, $entry->errorCode);
        }
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
     * The 15 of a share lookup is retriable, which is what makes it a trap: {@see CoordinatorLookup} repeats the
     * request until the given timeout runs out and then reports a coordinator that never became available
     */
    public function testTheRetriableFifteenOfAShareLookupRunsIntoTheTimeout(): void
    {
        $lookup = new CoordinatorLookup(
            Cluster::bootstrap($this->configuration()),
            $this->configuration()
        );

        $this->expectException(GroupCoordinatorNotAvailableException::class);

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

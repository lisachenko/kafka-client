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

use Closure;
use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Admin\RaftVoterEndpoint;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\DuplicateVoterException;
use Protocol\Kafka\Common\Errors\InconsistentClusterIdException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\RequestTimedOutException;
use Protocol\Kafka\Common\Errors\VoterNotFoundException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\Data\AddRaftVoterRequestListener;
use Protocol\Kafka\Protocol\Request\AddRaftVoterRequest;
use Protocol\Kafka\Protocol\Request\AddRaftVoterRequestV0;
use Protocol\Kafka\Protocol\Request\AddRaftVoterResponse;
use Protocol\Kafka\Protocol\Request\AddRaftVoterResponseV0;
use Protocol\Kafka\Protocol\Request\RemoveRaftVoterRequest;
use Protocol\Kafka\Protocol\Request\RemoveRaftVoterResponse;

/**
 * Exercises AddRaftVoter (key 80, v0 and the v1 of Kafka 4.2) and RemoveRaftVoter (key 81, v0) of KIP-853 against the
 * 4.3.1 KRaft node.
 *
 * The node runs the **dynamic** quorum of KIP-853 - formatted `--standalone`, `kraft.version` finalized at 1 - with
 * one voter, the node 1 itself. A test of this class may therefore change nothing: it never removes the voter 1
 * and never adds a voter that could answer, so every request below is one the controller refuses before it writes
 * a voter set - the cluster id, the voter key, the listener, the voter id the set already has, a key the set does
 * not hold, and a new voter whose CONTROLLER endpoint is a closed port. Every test reads the quorum back and
 * demands the one voter it found.
 *
 * @see docs/protocol/4.3.md, sections "AddRaftVoter API (key 80, v0 and v1)" and "RemoveRaftVoter API (key 81, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(AddRaftVoterRequest::class)]
#[CoversClass(AddRaftVoterResponse::class)]
#[CoversClass(AddRaftVoterRequestV0::class)]
#[CoversClass(AddRaftVoterResponseV0::class)]
#[CoversClass(AddRaftVoterRequestListener::class)]
#[CoversClass(RemoveRaftVoterRequest::class)]
#[CoversClass(RemoveRaftVoterResponse::class)]
final class RaftVoterApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-40-voters';

    /**
     * A replica id no node of this repository has, for the voter that is added or removed
     */
    private const int ABSENT_VOTER_ID = 4243;

    /**
     * The message of an operation that met another one in flight, which this class waits out
     */
    private const string OPERATION_PENDING = 'Request timed out waiting for leader to handle previous voter change';

    /**
     * The end of the message of an add whose new voter id the raft client of the leader still backs off
     *
     * `DefaultRequestSender` @ 4.3.1 refuses to send to a replica id whose last request failed for
     * `controller.quorum.retry.backoff.ms` (20 ms by default), and `AddVoterHandler` answers that refusal with the 7
     * `New voter ReplicaKey(...) is not ready to receive requests`: two adds of the same unreachable voter in a row
     * meet it. This class waits it out, like an operation in flight.
     */
    private const string VOTER_BACKING_OFF = 'is not ready to receive requests';

    private AdminClient $admin;

    /**
     * The 16 raw bytes of the directory id of the voter 1, read from the quorum
     */
    private string $voterDirectoryId;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);

        $quorum = $this->admin->describeMetadataQuorum();

        self::assertSame([1], array_map(static fn($voter): int => $voter->replicaId, $quorum->voters));
        $this->voterDirectoryId = $quorum->voters[0]->replicaDirectoryId;
        self::assertNotSame(Uuid::ZERO, $this->voterDirectoryId, 'a kraft.version 1 quorum has directory ids');
    }

    /**
     * No test of this class may change the voter set of a node four agents share
     */
    protected function tearDown(): void
    {
        $quorum = $this->admin->describeMetadataQuorum();

        self::assertSame(
            [[1, $this->voterDirectoryId]],
            array_map(static fn($voter): array => [$voter->replicaId, $voter->replicaDirectoryId], $quorum->voters),
            'the one voter of the node, with its own key'
        );
        self::assertSame([], $quorum->observers);

        parent::tearDown();
    }

    /**
     * An add of the voter id the quorum has is the 126, whatever the directory id - the id alone is compared
     */
    public function testAnAddOfTheVoterIdTheQuorumHasIsTheDuplicateVoter(): void
    {
        $key = Uuid::toString($this->voterDirectoryId);

        foreach ([$this->voterDirectoryId, random_bytes(Uuid::SIZE)] as $directoryId) {
            $refused = $this->refusal(fn() => $this->admin->addRaftVoter(1, $directoryId, $this->closedEndpoint()));

            self::assertInstanceOf(DuplicateVoterException::class, $refused);
            self::assertSame(KafkaException::DUPLICATE_VOTER, $refused->getCode());
            self::assertSame(
                'The voter id for ReplicaKey(id=1, directoryId=' . Uuid::toString($directoryId) . ') is already'
                . ' part of the set of voters [ReplicaKey(id=1, directoryId=' . $key . ')].',
                $refused->getContext()['error']
            );
        }
    }

    /**
     * The real cluster id passes the first check, and the request goes on to the one after it
     */
    public function testTheClusterIdOfTheNodeIsAccepted(): void
    {
        $clusterId = $this->admin->describeCluster()->clusterId;
        $refused   = $this->refusal(
            fn() => $this->admin->addRaftVoter(1, $this->voterDirectoryId, $this->closedEndpoint(), $clusterId)
        );

        self::assertInstanceOf(DuplicateVoterException::class, $refused, 'the 126 of the voter id, not the 104');
    }

    /**
     * A remove of a key the voter set does not hold is the 127, and its message lists the keys it does hold
     */
    public function testARemoveOfAnUnknownVoterIsTheVoterNotFound(): void
    {
        $directoryId = random_bytes(Uuid::SIZE);
        $refused     = $this->refusal(fn() => $this->admin->removeRaftVoter(self::ABSENT_VOTER_ID, $directoryId));

        self::assertInstanceOf(VoterNotFoundException::class, $refused);
        self::assertSame(KafkaException::VOTER_NOT_FOUND, $refused->getCode());
        self::assertSame(
            'Cannot remove voter ReplicaKey(id=' . self::ABSENT_VOTER_ID . ', directoryId='
            . Uuid::toString($directoryId) . ') from the set of voters [ReplicaKey(id=1, directoryId='
            . Uuid::toString($this->voterDirectoryId) . ')]',
            $refused->getContext()['error']
        );
    }

    /**
     * A foreign cluster id is the 104 of both apis, before anything else is looked at
     */
    public function testAForeignClusterIdIsTheInconsistentClusterId(): void
    {
        $clusterId = $this->admin->describeCluster()->clusterId;
        $expected  = 'The given id "not-this-cluster" doesn\'t match the cluster id "' . $clusterId . '"';

        $add = $this->refusal(fn() => $this->admin->addRaftVoter(
            self::ABSENT_VOTER_ID,
            random_bytes(Uuid::SIZE),
            $this->closedEndpoint(),
            'not-this-cluster'
        ));

        self::assertInstanceOf(InconsistentClusterIdException::class, $add);
        self::assertSame($expected, $add->getContext()['error']);

        $remove = $this->refusal(
            fn() => $this->admin->removeRaftVoter(self::ABSENT_VOTER_ID, random_bytes(Uuid::SIZE), 'not-this-cluster')
        );

        self::assertInstanceOf(InconsistentClusterIdException::class, $remove);
        self::assertSame($expected, $remove->getContext()['error']);
    }

    /**
     * A voter key without a directory id is the 42 of both apis: a voter of a dynamic quorum is always a whole key
     */
    public function testTheZeroDirectoryIdIsAnInvalidVoter(): void
    {
        $add = $this->refusal(
            fn() => $this->admin->addRaftVoter(self::ABSENT_VOTER_ID, Uuid::ZERO, $this->closedEndpoint())
        );

        self::assertInstanceOf(InvalidRequestException::class, $add);
        self::assertSame('Add voter request didn\'t include a valid voter', $add->getContext()['error']);

        $remove = $this->refusal(fn() => $this->admin->removeRaftVoter(self::ABSENT_VOTER_ID, Uuid::ZERO));

        self::assertInstanceOf(InvalidRequestException::class, $remove);
        self::assertSame('Remove voter request didn\'t include a valid voter', $remove->getContext()['error']);
    }

    /**
     * A new voter without an endpoint on the listener of the controllers is the 42 as well
     */
    public function testAnAddWithoutTheControllerListenerIsInvalidRequest(): void
    {
        $refused = $this->refusal(fn() => $this->admin->addRaftVoter(
            self::ABSENT_VOTER_ID,
            random_bytes(Uuid::SIZE),
            [new RaftVoterEndpoint('PLAINTEXT', '127.0.0.1', 1)]
        ));

        self::assertInstanceOf(InvalidRequestException::class, $refused);
        self::assertSame(
            'Add voter request didn\'t include the endpoint (Endpoints(endpoints={ListenerName(PLAINTEXT)='
            . '127.0.0.1/<unresolved>:1})) for the default listener ListenerName(CONTROLLER)',
            $refused->getContext()['error']
        );
    }

    /**
     * A new voter the controller cannot reach is never added: its ApiVersions fails and the operation is aborted
     *
     * `AddVoterHandler` @ 4.0.0 asks the new voter for its ApiVersions before it appends anything - the answer has
     * to support the finalized `kraft.version`, and the new voter has to have caught up with the log. A closed port
     * answers neither: the raft client reports the failed request as `BROKER_NOT_AVAILABLE`, and the add is
     * answered the **7** (`RequestTimedOut`) of an aborted operation within milliseconds. The quorum is unchanged
     * (`tearDown`).
     */
    public function testAnUnreachableVoterIsNeverAdded(): void
    {
        $refused = $this->refusal(fn() => $this->admin->addRaftVoter(
            self::ABSENT_VOTER_ID,
            random_bytes(Uuid::SIZE),
            $this->closedEndpoint()
        ));

        self::assertInstanceOf(RequestTimedOutException::class, $refused);
        self::assertSame(KafkaException::REQUEST_TIMED_OUT, $refused->getCode());
        self::assertSame(
            'Aborted add voter operation for since API_VERSIONS returned an error BROKER_NOT_AVAILABLE',
            $refused->getContext()['error'],
            'the missing word is the sentence of AddVoterHandler'
        );
    }

    /**
     * The `ack_when_committed` of AddRaftVoter v1 (Kafka 4.2) changes no refusal: the 126 and the aborted 7 alike
     *
     * `AddVoterHandler` @ 4.2.0 reads the flag only once the new voter answered its ApiVersions and the leader
     * appended the new voter set - false answers right there, true once the set is committed. Every check before
     * that is the same with either value, so the voter id the quorum has is the 126 and an unreachable voter the
     * aborted operation, and nothing is added (`tearDown`).
     */
    public function testTheAcknowledgementModeOfVersionOneChangesNoRefusal(): void
    {
        foreach ([true, false] as $ackWhenCommitted) {
            $duplicate = $this->refusal(fn() => $this->admin->addRaftVoter(
                1,
                $this->voterDirectoryId,
                $this->closedEndpoint(),
                null,
                30000,
                $ackWhenCommitted
            ));

            self::assertInstanceOf(DuplicateVoterException::class, $duplicate);

            $unreachable = $this->refusal(fn() => $this->admin->addRaftVoter(
                self::ABSENT_VOTER_ID,
                random_bytes(Uuid::SIZE),
                $this->closedEndpoint(),
                null,
                30000,
                $ackWhenCommitted
            ));

            self::assertInstanceOf(RequestTimedOutException::class, $unreachable);
            self::assertSame(
                'Aborted add voter operation for since API_VERSIONS returned an error BROKER_NOT_AVAILABLE',
                $unreachable->getContext()['error']
            );
        }
    }

    /**
     * The keep-behind v0 frame is still served: the node answers it the 126 in the frame of v1
     */
    public function testTheVersionZeroFrameIsStillAnswered(): void
    {
        $stream = $this->connect();
        new AddRaftVoterRequestV0(
            null,
            30000,
            1,
            $this->voterDirectoryId,
            [new AddRaftVoterRequestListener('CONTROLLER', '127.0.0.1', 1)],
            self::CLIENT_ID,
            4220
        )->writeTo($stream);
        $answer = AddRaftVoterResponseV0::unpack($stream);

        self::assertSame(4220, $answer->getCorrelationId());
        self::assertSame(KafkaException::DUPLICATE_VOTER, $answer->errorCode);
        self::assertStringEndsWith('is already part of the set of voters [ReplicaKey(id=1, directoryId='
            . Uuid::toString($this->voterDirectoryId) . ')].', (string) $answer->errorMessage);
    }

    /**
     * A directory id is 16 bytes, and the admin api refuses anything else before it sends a frame
     */
    public function testADirectoryIdThatIsNotAUuidIsRefusedBeforeTheRequest(): void
    {
        $this->expectException(\UnexpectedValueException::class);

        $this->admin->removeRaftVoter(self::ABSENT_VOTER_ID, 'not-a-uuid');
    }

    /**
     * Runs a call the controller has to refuse, and returns the exception it was refused with
     *
     * A refusal of "another voter change is in flight" is waited out: the add of an unreachable voter keeps the
     * one operation slot of the leader for the few milliseconds its ApiVersions takes to fail. So is the backoff of
     * a voter id whose last ApiVersions failed ({@see self::VOTER_BACKING_OFF}).
     */
    private function refusal(Closure $call): KafkaException
    {
        for ($attempt = 0; ; ++$attempt) {
            try {
                $call();
            } catch (KafkaException $exception) {
                $message = (string) ($exception->getContext()['error'] ?? '');
                $pending = $exception instanceof RequestTimedOutException
                    && (str_starts_with($message, self::OPERATION_PENDING) || str_ends_with($message, self::VOTER_BACKING_OFF));
                if (!$pending || $attempt >= 20) {
                    return $exception;
                }
                usleep(100000);

                continue;
            }

            self::fail('the controller accepted a voter change this test expects it to refuse');
        }
    }

    /**
     * A CONTROLLER endpoint that answers nothing: the port 1 inside the container is closed
     *
     * @return list<RaftVoterEndpoint>
     */
    private function closedEndpoint(): array
    {
        return [new RaftVoterEndpoint('CONTROLLER', '127.0.0.1', 1)];
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 30000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}

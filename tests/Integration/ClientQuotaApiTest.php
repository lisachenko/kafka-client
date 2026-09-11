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
use Protocol\Kafka\Admin\ClientQuotaAlteration;
use Protocol\Kafka\Admin\ClientQuotaAlterationOp;
use Protocol\Kafka\Admin\ClientQuotaEntity;
use Protocol\Kafka\Admin\ClientQuotaFilter;
use Protocol\Kafka\Admin\ClientQuotaFilterComponent;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnsupportedVersionException;
use Protocol\Kafka\IO\StringStream;
use Protocol\Kafka\Protocol\Data\ClientQuotaComponentData;
use Protocol\Kafka\Protocol\Request\AlterClientQuotasRequest;
use Protocol\Kafka\Protocol\Request\AlterClientQuotasResponse;
use Protocol\Kafka\Protocol\Request\DescribeClientQuotasRequest;
use Protocol\Kafka\Protocol\Request\DescribeClientQuotasRequestV0;
use Protocol\Kafka\Protocol\Request\DescribeClientQuotasResponse;
use Protocol\Kafka\Protocol\Request\DescribeClientQuotasResponseV0;
use Throwable;

/**
 * Exercises the two client-quota apis of KIP-546 - DescribeClientQuotas (48) and AlterClientQuotas (49) - against
 * a real Kafka 2.8.2 broker.
 *
 * Until Kafka 2.6 a client quota could only be written through **ZooKeeper**, which is why `tests/Fixture/ClientQuota`
 * shells `kafka-configs.sh` into the container and why the suites that came up the cascade still use it. This class
 * is the same thing over the protocol, in both the plain version 0 of Kafka 2.6 and the flexible version 1 of
 * Kafka 2.8.
 *
 * **Every quota this class writes is attached to a `client-id` of its own**, generated per run, and every one of
 * them is removed again in {@see self::tearDownAfterClass()}. No test writes a `<default>` quota: it would apply to
 * every principal or client of the shared container that has none of its own, which is the same restraint the
 * reassignment suite shows towards a null topic array.
 *
 * @see docs/protocol/2.8.md, sections "DescribeClientQuotas API (key 48, v0 and v1)" and
 *      "AlterClientQuotas API (key 49, v0 and v1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeClientQuotasRequest::class)]
#[CoversClass(DescribeClientQuotasResponse::class)]
#[CoversClass(AlterClientQuotasRequest::class)]
#[CoversClass(AlterClientQuotasResponse::class)]
#[CoversClass(ClientQuotaEntity::class)]
#[CoversClass(ClientQuotaFilter::class)]
#[CoversClass(ClientQuotaAlteration::class)]
final class ClientQuotaApiTest extends IntegrationTestCase
{
    /**
     * Every entity this class attached a quota to, removed again when it is done
     *
     * @var list<ClientQuotaEntity>
     */
    private static array $entities = [];

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * Removes every quota this class wrote, so that the shared container keeps nothing of it
     */
    public static function tearDownAfterClass(): void
    {
        $entities       = self::$entities;
        self::$entities = [];

        if (self::bootstrapServers() === [] || $entities === []) {
            return;
        }

        try {
            $configuration = [
                ClientConfig::BOOTSTRAP_SERVERS  => ['tcp://' . self::firstBootstrapServer()],
                ClientConfig::CLIENT_ID          => 'kafka-client-t1-quota',
                ClientConfig::REQUEST_TIMEOUT_MS => 20000,
            ];
            $admin = new AdminClient(Cluster::bootstrap($configuration), $configuration);

            $admin->alterClientQuotas(array_map(
                static fn(ClientQuotaEntity $entity) => new ClientQuotaAlteration($entity, self::removeAll()),
                $entities
            ));
        } catch (Throwable) {
            // A broker that is gone or busy is not a failure of these tests - every name is unique per run
        }
    }

    /**
     * A quota that is set with the api is read back by it, and the values are doubles
     */
    public function testAQuotaIsWrittenAndReadBackWithTwoRequests(): void
    {
        $clientId = $this->clientId('roundtrip');

        $result = $this->admin->alterClientQuotas([
            new ClientQuotaAlteration(ClientQuotaEntity::forClientId($clientId), [
                ClientQuotaAlterationOp::set(ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE, 1048576.0),
                ClientQuotaAlterationOp::set(ClientQuotaAlterationOp::KEY_REQUEST_PERCENTAGE, 12.5),
            ]),
        ]);

        self::assertSame(["client-id={$clientId}" => null], $result, 'the broker accepted the entity');

        $quotas = $this->admin->describeClientQuotas(ClientQuotaFilter::contains([
            ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_CLIENT_ID, $clientId),
        ]));

        self::assertSame(
            ["client-id={$clientId}" => ['request_percentage' => 12.5, 'producer_byte_rate' => 1048576.0]],
            $quotas,
            'a float64 survives the round trip exactly, and the entries come back in the order of the broker'
        );
    }

    /**
     * The plain version 0 of Kafka 2.6 reads the very same quota as the flexible version 1
     */
    public function testBothVersionsOfTheApiAnswerTheSameQuota(): void
    {
        $clientId = $this->clientId('versions');
        $this->setQuota($clientId, ClientQuotaAlterationOp::KEY_CONSUMER_BYTE_RATE, 2048.0);

        $filter = [
            ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_CLIENT_ID, $clientId)->toData(),
        ];

        $plain    = $this->send(new DescribeClientQuotasRequestV0($filter, false, 'kafka-client-t1-quota', 7001));
        $flexible = $this->send(new DescribeClientQuotasRequest($filter, false, 'kafka-client-t1-quota', 7002));

        self::assertInstanceOf(DescribeClientQuotasResponseV0::class, $plain);
        self::assertInstanceOf(DescribeClientQuotasResponse::class, $flexible);
        self::assertCount(1, (array) $plain->entries);
        self::assertCount(1, (array) $flexible->entries);
        self::assertEquals(
            $plain->entries[0]->values,
            $flexible->entries[0]->values,
            'the plain and the compact encoding of the same quota carry the same values'
        );
    }

    /**
     * `strict` excludes an entity that also carries a part of a type the filter does not name
     */
    public function testStrictExcludesACombinedEntity(): void
    {
        $clientId = $this->clientId('strict');
        $user     = $this->user('strict');

        $this->admin->alterClientQuotas([
            new ClientQuotaAlteration(ClientQuotaEntity::forClientId($clientId), [
                ClientQuotaAlterationOp::set(ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE, 1024.0),
            ]),
            new ClientQuotaAlteration(
                $this->remember(new ClientQuotaEntity([
                    ClientQuotaEntity::TYPE_USER      => $user,
                    ClientQuotaEntity::TYPE_CLIENT_ID => $clientId,
                ])),
                [ClientQuotaAlterationOp::set(ClientQuotaAlterationOp::KEY_CONTROLLER_MUTATION_RATE, 7.5)]
            ),
        ]);

        $component = ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_CLIENT_ID, $clientId);

        $lenient = $this->admin->describeClientQuotas(ClientQuotaFilter::contains([$component]));
        $strict  = $this->admin->describeClientQuotas(ClientQuotaFilter::containsOnly([$component]));

        self::assertSame(
            ["client-id={$clientId}", "client-id={$clientId},user={$user}"],
            array_keys($lenient),
            'a non-strict filter also answers the entity that carries a user part'
        );
        self::assertSame(
            ["client-id={$clientId}"],
            array_keys($strict),
            'and a strict one answers only the entity whose types are exactly the ones it named'
        );
    }

    /**
     * A removal really removes, and an entity without a quota left disappears from the answer
     */
    public function testRemovingTheLastQuotaRemovesTheEntity(): void
    {
        $clientId = $this->clientId('removal');
        $this->setQuota($clientId, ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE, 4096.0);

        $result = $this->admin->alterClientQuotas([
            new ClientQuotaAlteration(ClientQuotaEntity::forClientId($clientId), [
                ClientQuotaAlterationOp::remove(ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE),
            ]),
        ]);

        self::assertSame(["client-id={$clientId}" => null], $result);
        self::assertSame([], $this->quotasOf($clientId), 'the entity is gone from the answer with its last quota');

        $again = $this->admin->alterClientQuotas([
            new ClientQuotaAlteration(ClientQuotaEntity::forClientId($clientId), [
                ClientQuotaAlterationOp::remove(ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE),
            ]),
        ]);

        self::assertSame(
            ["client-id={$clientId}" => null],
            $again,
            'and removing a quota that is not there is not an error'
        );
    }

    /**
     * `validate_only` answers what the change would answer and writes nothing
     */
    public function testAValidateOnlyRequestChangesNothing(): void
    {
        $clientId = $this->clientId('validate');
        $this->setQuota($clientId, ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE, 1024.0);

        $result = $this->admin->alterClientQuotas([
            new ClientQuotaAlteration(ClientQuotaEntity::forClientId($clientId), [
                ClientQuotaAlterationOp::set(ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE, 999999.0),
            ]),
        ], true);

        self::assertSame(["client-id={$clientId}" => null], $result, 'the broker says it would have worked');
        self::assertSame(
            ['producer_byte_rate' => 1024.0],
            $this->quotasOf($clientId),
            'and the quota is exactly the one that was there'
        );
    }

    /**
     * A quota name the broker does not know is 42, per entity and not at the top level
     */
    public function testAnUnknownQuotaKeyIsRefusedWithInvalidRequest(): void
    {
        $clientId = $this->clientId('badkey');

        $result = $this->admin->alterClientQuotas([
            new ClientQuotaAlteration(ClientQuotaEntity::forClientId($clientId), [
                ClientQuotaAlterationOp::set('t1_nonsense_rate', 1.0),
            ]),
        ]);

        $error = $result["client-id={$clientId}"];

        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertSame(KafkaException::INVALID_REQUEST, $error->getCode());
        self::assertStringContainsString('Invalid configuration key t1_nonsense_rate', $error->getMessage());
    }

    /**
     * An entity type the broker does not know is the top-level 35, and the entry array comes back null
     */
    public function testAnUnknownEntityTypeIsRefusedWithUnsupportedVersion(): void
    {
        $response = $this->send(new DescribeClientQuotasRequest(
            [new ClientQuotaComponentData('t1-nonsense', ClientQuotaComponentData::MATCH_TYPE_EXACT, 'x')],
            false,
            'kafka-client-t1-quota',
            7003
        ));

        self::assertSame(KafkaException::UNSUPPORTED_VERSION, $response->errorCode, 'the broker answers 35, not 42');
        self::assertStringContainsString("Custom entity type 't1-nonsense' not supported", (string) $response->errorMessage);
        self::assertNull($response->entries, 'an error answer carries the null array, never the empty one');

        $this->expectException(UnsupportedVersionException::class);
        $this->admin->describeClientQuotas(ClientQuotaFilter::contains([
            ClientQuotaFilterComponent::ofEntity('t1-nonsense', 'x'),
        ]));
    }

    /**
     * A match type the broker does not know escapes as an IllegalArgumentException and becomes the code -1
     *
     * There is no way to ask for this through {@see AdminClient::describeClientQuotas()} - the three match types
     * are the three factory methods of {@see ClientQuotaFilterComponent}, which has no other constructor - so the
     * quirk is measured on a hand-built request.
     */
    public function testAnUnknownMatchTypeIsAnsweredWithTheCodeMinusOne(): void
    {
        $response = $this->send(new DescribeClientQuotasRequest(
            [new ClientQuotaComponentData(ClientQuotaEntity::TYPE_CLIENT_ID, 7, 'x')],
            false,
            'kafka-client-t1-quota',
            7004
        ));

        self::assertSame(KafkaException::UNKNOWN, $response->errorCode);
        self::assertNull($response->errorMessage, 'the generic handler of KafkaApis sends no message at all');
        self::assertNull($response->entries);
    }

    /**
     * A filter that matches nothing answers the empty list, and the `<default>` entity is only ever read
     */
    public function testAFilterThatMatchesNothingAnswersTheEmptyList(): void
    {
        $unknown = $this->uniqueName('nothing');

        self::assertSame([], $this->quotasOf($unknown), 'a client id without a quota is simply absent');
        self::assertSame(
            [],
            $this->admin->describeClientQuotas(ClientQuotaFilter::contains([
                ClientQuotaFilterComponent::ofDefaultEntity(ClientQuotaEntity::TYPE_IP),
            ])),
            'and the <default> ip entity of this container has no quota either - no test of this class writes one'
        );
    }

    /**
     * Returns the quotas of one client id, or an empty array when it has none
     *
     * @return array<string, float>
     */
    private function quotasOf(string $clientId): array
    {
        $quotas = $this->admin->describeClientQuotas(ClientQuotaFilter::containsOnly([
            ClientQuotaFilterComponent::ofEntity(ClientQuotaEntity::TYPE_CLIENT_ID, $clientId),
        ]));

        return $quotas["client-id={$clientId}"] ?? [];
    }

    /**
     * Sets one quota of one client id
     */
    private function setQuota(string $clientId, string $key, float $value): void
    {
        $this->admin->alterClientQuotas([
            new ClientQuotaAlteration(
                ClientQuotaEntity::forClientId($clientId),
                [ClientQuotaAlterationOp::set($key, $value)]
            ),
        ]);
    }

    /**
     * Sends one request to the first broker and reads the answer that belongs to it
     */
    private function send(DescribeClientQuotasRequest $request): DescribeClientQuotasResponse
    {
        $stream = $this->connect();
        $request->writeTo($stream);
        $size  = $stream->read('NmessageSize')['messageSize'];
        $body  = $stream->read("a{$size}data")['data'];
        $class = $request::VERSION === 0 ? DescribeClientQuotasResponseV0::class : DescribeClientQuotasResponse::class;

        return $class::unpack(new StringStream(pack('N', $size) . $body));
    }

    /**
     * Every change of {@see self::tearDownAfterClass()}: remove every quota this class can have written
     *
     * @return list<ClientQuotaAlterationOp>
     */
    private static function removeAll(): array
    {
        return [
            ClientQuotaAlterationOp::remove(ClientQuotaAlterationOp::KEY_PRODUCER_BYTE_RATE),
            ClientQuotaAlterationOp::remove(ClientQuotaAlterationOp::KEY_CONSUMER_BYTE_RATE),
            ClientQuotaAlterationOp::remove(ClientQuotaAlterationOp::KEY_REQUEST_PERCENTAGE),
            ClientQuotaAlterationOp::remove(ClientQuotaAlterationOp::KEY_CONTROLLER_MUTATION_RATE),
        ];
    }

    /**
     * Names a client id of this run and remembers its entity for the cleanup
     */
    private function clientId(string $kind): string
    {
        $clientId         = $this->uniqueName($kind);
        self::$entities[] = ClientQuotaEntity::forClientId($clientId);

        return $clientId;
    }

    /**
     * Names a user of this run and remembers its entity for the cleanup
     */
    private function user(string $kind): string
    {
        $user             = $this->uniqueName('user-' . $kind);
        self::$entities[] = ClientQuotaEntity::forUser($user);

        return $user;
    }

    /**
     * Remembers an entity this class wrote a quota to, so that the cleanup finds it as well
     */
    private function remember(ClientQuotaEntity $entity): ClientQuotaEntity
    {
        self::$entities[] = $entity;

        return $entity;
    }

    private function uniqueName(string $kind): string
    {
        return 't1-quota-' . $kind . '-' . bin2hex(random_bytes(4));
    }

    /**
     * @return array<string, mixed> Client configuration for this test class
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => 'kafka-client-t1-quota',
            ClientConfig::REQUEST_TIMEOUT_MS        => 20000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }
}

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
use Protocol\Kafka\Common\AccessControlEntry;
use Protocol\Kafka\Common\AclBinding;
use Protocol\Kafka\Common\AclBindingFilter;
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\ClusterAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\ResourcePattern;
use Protocol\Kafka\Common\ResourceType;
use Protocol\Kafka\Protocol\Request\CreateAclsRequest;
use Protocol\Kafka\Protocol\Request\DeleteAclsRequest;
use Protocol\Kafka\Protocol\Request\DescribeAclsRequest;
use Protocol\Kafka\Tests\Compliance\VectorFile;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * The three ACL methods of the admin client, replayed against the answers the 3.9.2 KRaft node really sent.
 *
 * @see docs/protocol/4.3.md, sections "DescribeAcls API (key 29, v0 to v3)", "CreateAcls API (key 30, v0 to v3)"
 *      and "DeleteAcls API (key 31, v0 to v3)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(AclBinding::class)]
#[CoversClass(AclBindingFilter::class)]
final class AclAdminApiTest extends TestCase
{
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string BROKER_ADDRESS = 'tcp://127.0.0.1:9092';

    private const string TOPIC = 't4-33-acl-topic';

    private const string GROUP_PREFIX = 't4-33-acl-';

    private const string PRINCIPAL = 'User:acltest';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testDescribeAclsFlattensTheResourceGroupsOfTheAnswer(): void
    {
        $broker = $this->scriptBroker(self::vector('describe-acls', 'describeacls.response.v3'));

        $bindings = $this->adminClient()->describeAcls();

        self::assertCount(2, $bindings, 'two resources with one acl each become two bindings');
        self::assertSame(ResourceType::GROUP, $bindings[0]->pattern->resourceType);
        self::assertSame(ResourceType::TOPIC, $bindings[1]->pattern->resourceType);
        self::assertSame(self::PRINCIPAL, $bindings[1]->entry->principal);

        self::assertSame(
            [self::requestFrame(new DescribeAclsRequest(null, 't4-acl', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames(),
            'no argument is the filter that matches every acl of the cluster'
        );
    }

    public function testDescribeAclsRaisesTheRefusalOfTheWholeRequest(): void
    {
        $this->scriptBroker(self::vector('describe-acls', 'describeacls.response.v3.unauthorized'));

        $this->expectException(ClusterAuthorizationFailedException::class);

        $this->adminClient()->describeAcls();
    }

    public function testCreateAclsReportsEveryAclOfTheCallAndThrowsNothing(): void
    {
        $broker = $this->scriptBroker(self::vector('create-acls', 'createacls.response.v3'));

        $bindings = [
            AclBinding::allow(ResourceType::TOPIC, self::TOPIC, self::PRINCIPAL, AclOperation::READ),
            new AclBinding(
                ResourcePattern::prefixed(ResourceType::GROUP, self::GROUP_PREFIX),
                AccessControlEntry::allow(self::PRINCIPAL, AclOperation::READ)
            ),
        ];

        self::assertSame([null, null], $this->adminClient()->createAcls($bindings));
        self::assertSame(
            [self::requestFrame(new CreateAclsRequest($bindings, 't4-acl', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames()
        );
    }

    public function testARefusedCreationBecomesTheExceptionOfItsOwnEntry(): void
    {
        $this->scriptBroker(self::vector('create-acls', 'createacls.response.v3.invalid'));

        $result = $this->adminClient()->createAcls([
            new AclBinding(
                new ResourcePattern(ResourceType::TOPIC, ''),
                AccessControlEntry::allow(self::PRINCIPAL, AclOperation::READ)
            ),
        ]);

        self::assertInstanceOf(InvalidRequestException::class, $result[0]);
        self::assertStringContainsString('Invalid empty resource name', $result[0]->getMessage());
    }

    public function testCreateAclsSendsNothingForAnEmptyList(): void
    {
        $broker = $this->scriptBroker(self::vector('create-acls', 'createacls.response.v3'));

        self::assertSame([], $this->adminClient()->createAcls([]));
        self::assertSame([], $broker->getReceivedFrames(), 'an empty call is answered without asking the broker');
    }

    public function testDeleteAclsAnswersTheAclsEveryFilterRemoved(): void
    {
        $broker  = $this->scriptBroker(self::vector('delete-acls', 'deleteacls.response.v3'));
        $filters = [
            AclBindingFilter::ofResourceType(ResourceType::TOPIC),
            AclBindingFilter::ofResourceType(ResourceType::TRANSACTIONAL_ID),
        ];

        $result = $this->adminClient()->deleteAcls($filters);

        self::assertCount(1, $result[0], 'the first filter removed the literal topic acl');
        self::assertSame(self::TOPIC, $result[0][0]->pattern->resourceName);
        self::assertSame([], $result[1], 'and the second one matched nothing, which is not an error');

        self::assertSame(
            [self::requestFrame(new DeleteAclsRequest($filters, 't4-acl', $broker->getReceivedCorrelationIds()[0]))],
            $broker->getReceivedFrames()
        );
    }

    public function testARefusedFilterBecomesTheExceptionInItsPlace(): void
    {
        $this->scriptBroker(self::vector('delete-acls', 'deleteacls.response.v3.unauthorized'));

        $result = $this->adminClient()->deleteAcls([
            AclBindingFilter::of(
                AclBinding::allow(ResourceType::TOPIC, self::TOPIC, self::PRINCIPAL, AclOperation::READ)
            ),
        ]);

        self::assertInstanceOf(ClusterAuthorizationFailedException::class, $result[0]);
        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $result[0]->getCode());
    }

    public function testDeleteAclsSendsNothingForAnEmptyList(): void
    {
        $broker = $this->scriptBroker(self::vector('delete-acls', 'deleteacls.response.v3'));

        self::assertSame([], $this->adminClient()->deleteAcls([]));
        self::assertSame([], $broker->getReceivedFrames());
    }

    /**
     * Installs a bootstrap broker with the metadata of the one-broker cluster and a broker with the given answers
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

    private function adminClient(): AdminClient
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't4-acl',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    private static function clusterMetadata(): string
    {
        return ResponseFrame::metadata(0, [[0, '127.0.0.1', 9092]]);
    }

    /**
     * Returns the frame of a request as a scripted broker records it, i.e. without the leading Size field
     */
    private static function requestFrame(DescribeAclsRequest|CreateAclsRequest|DeleteAclsRequest $request): string
    {
        return substr((string) $request, 4);
    }

    /**
     * Returns the raw frame of a documented wire vector
     */
    private static function vector(string $api, string $id): string
    {
        foreach (VectorFile::read($api)['vectors'] as $vector) {
            if ($vector['id'] === $id) {
                return (string) hex2bin((string) $vector['hex']);
            }
        }

        self::fail("There is no wire vector {$id} in docs/protocol/vectors/{$api}.json");
    }
}

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
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\AccessControlEntry;
use Protocol\Kafka\Common\AccessControlEntryFilter;
use Protocol\Kafka\Common\AclBinding;
use Protocol\Kafka\Common\AclBindingFilter;
use Protocol\Kafka\Common\AclOperation;
use Protocol\Kafka\Common\AclPermissionType;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\ClusterAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Common\PatternType;
use Protocol\Kafka\Common\ResourcePattern;
use Protocol\Kafka\Common\ResourcePatternFilter;
use Protocol\Kafka\Common\ResourceType;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Network\ConnectionFactory;
use Protocol\Kafka\Protocol\Request\CreateAclsRequest;
use Protocol\Kafka\Protocol\Request\CreateAclsResponse;
use Protocol\Kafka\Protocol\Request\DeleteAclsRequest;
use Protocol\Kafka\Protocol\Request\DeleteAclsResponse;
use Protocol\Kafka\Protocol\Request\DescribeAclsRequest;
use Protocol\Kafka\Protocol\Request\DescribeAclsResponse;
use Protocol\Kafka\Protocol\Request\MetadataRequest;
use Protocol\Kafka\Protocol\Request\MetadataResponse;

/**
 * The three ACL apis (keys 29, 30 and 31) at the version 3 of Kafka 3.3, against the `StandardAuthorizer` of the node.
 *
 * This is the first line of the repository that implements them, and the only reason it can is the authorizer of
 * `docker/kafka-4.4.0`: `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer` with
 * `super.users=User:ANONYMOUS;User:admin;User:kafkatest` and `allow.everyone.if.no.acl.found=false`. The PLAINTEXT
 * listener every other suite uses is therefore a super user, and the SASL user `acltest` is the one principal an
 * acl can be written for - which is what makes the last test of this class possible: an acl that really changes
 * what a principal may do.
 *
 * **The node is shared, so this class leaves no acl behind.** Every test writes acls of its own resources
 * (`t4-33-*`) and the tear down removes every acl that names them, whatever the test did with them.
 *
 * @see docs/protocol/4.4.md, sections "DescribeAcls API (key 29, v0 to v3)", "CreateAcls API (key 30, v0 to v3)"
 *      and "DeleteAcls API (key 31, v0 to v3)"
 * @see docs/protocol/4.4.md, section "CIDR host patterns (KIP-1276, metadata.version 4.4-IV1)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(DescribeAclsRequest::class)]
#[CoversClass(DescribeAclsResponse::class)]
#[CoversClass(CreateAclsRequest::class)]
#[CoversClass(CreateAclsResponse::class)]
#[CoversClass(DeleteAclsRequest::class)]
#[CoversClass(DeleteAclsResponse::class)]
#[CoversClass(AclBinding::class)]
#[CoversClass(AclBindingFilter::class)]
#[CoversClass(AccessControlEntry::class)]
#[CoversClass(AccessControlEntryFilter::class)]
#[CoversClass(ResourcePattern::class)]
#[CoversClass(ResourcePatternFilter::class)]
final class AclApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t4-33-acl';

    /**
     * The one SASL user of the image that `super.users` does not name
     */
    private const string UNPRIVILEGED_USER = 'acltest';

    private const string UNPRIVILEGED_PASSWORD = 'acltest-secret';

    /**
     * The principal of that user, as the authorizer stores it
     */
    private const string PRINCIPAL = 'User:acltest';

    /**
     * Prefix of every resource this class writes an acl for; the tear down removes the acls of these resources
     */
    private const string PREFIX = 't4-33-acl';

    private AdminClient $admin;

    protected function setUp(): void
    {
        parent::setUp();

        $configuration = $this->configuration();
        $this->admin   = new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    protected function tearDown(): void
    {
        // Whatever a test did, the node keeps no acl of this class: the two resource types it ever writes for are
        // removed by name, and the emptiness of the authorizer is what the next suite relies on
        $this->admin->deleteAcls([
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType::ANY, null, PatternType::ANY),
                AccessControlEntryFilter::ofPrincipal(self::PRINCIPAL)
            ),
        ]);

        ConnectionFactory::closeAll();

        parent::tearDown();
    }

    /**
     * The whole life of an acl: write it, find it under five filters, and delete it again
     */
    public function testAnAclIsWrittenFoundAndDeletedAgain(): void
    {
        $topic    = self::uniqueTopicName(self::PREFIX . '-topic');
        $group    = self::PREFIX . '-group-' . bin2hex(random_bytes(4));
        $topicAcl = AclBinding::allow(ResourceType::TOPIC, $topic, self::PRINCIPAL, AclOperation::READ);
        $groupAcl = new AclBinding(
            ResourcePattern::prefixed(ResourceType::GROUP, $group),
            AccessControlEntry::allow(self::PRINCIPAL, AclOperation::READ)
        );

        self::assertSame([null, null], $this->admin->createAcls([$topicAcl, $groupAcl]), 'both acls were written');

        // A write of an acl is a controller write, and the authorizer of the broker applies it when it replays
        // the record - so the read-back is polled, never slept on
        $written = $this->awaitAcls(AclBindingFilter::ofPrincipal(self::PRINCIPAL), 2);

        self::assertSame(
            [
                'GROUP:PREFIXED:' . $group . ' ' . self::PRINCIPAL . ' * READ ALLOW',
                'TOPIC:LITERAL:' . $topic . ' ' . self::PRINCIPAL . ' * READ ALLOW',
            ],
            self::sorted($written),
            'the answer is grouped by resource pattern and carries the principal as the `User:name` string'
        );

        self::assertSame(
            ['TOPIC:LITERAL:' . $topic . ' ' . self::PRINCIPAL . ' * READ ALLOW'],
            self::sorted($this->admin->describeAcls(AclBindingFilter::ofResourceType(ResourceType::TOPIC))),
            'a filter by resource type answers the topic acl alone'
        );

        self::assertSame(
            ['GROUP:PREFIXED:' . $group . ' ' . self::PRINCIPAL . ' * READ ALLOW'],
            self::sorted($this->admin->describeAcls(AclBindingFilter::matching(ResourceType::GROUP, $group . '-one'))),
            'the pattern type MATCH finds the PREFIXED acl through a group name that merely starts with it'
        );

        self::assertSame(
            [],
            $this->admin->describeAcls(AclBindingFilter::ofResourceType(ResourceType::TRANSACTIONAL_ID)),
            'and a filter that matches nothing is the code 0 with an empty answer, not an error'
        );

        // A delete names a filter, and its answer is the only place that says what it removed
        $deleted = $this->admin->deleteAcls([AclBindingFilter::of($topicAcl), AclBindingFilter::of($groupAcl)]);

        self::assertSame(
            [
                ['TOPIC:LITERAL:' . $topic . ' ' . self::PRINCIPAL . ' * READ ALLOW'],
                ['GROUP:PREFIXED:' . $group . ' ' . self::PRINCIPAL . ' * READ ALLOW'],
            ],
            array_map(self::sorted(...), $deleted),
            'one group per filter, in the order of the request, with every matched acl in full'
        );

        self::assertSame([], $this->awaitAcls(AclBindingFilter::ofPrincipal(self::PRINCIPAL), 0));
    }

    public function testAFilterThatMatchesNothingRemovesNothingAndIsNoError(): void
    {
        $result = $this->admin->deleteAcls([
            AclBindingFilter::matching(ResourceType::TRANSACTIONAL_ID, self::PREFIX . '-no-such-id'),
        ]);

        self::assertSame([[]], $result, 'the filter was applied and matched no acl at all');
    }

    public function testWritingTheSameAclTwiceIsNotAnError(): void
    {
        $topic = self::uniqueTopicName(self::PREFIX . '-twice');
        $acl   = AclBinding::allow(ResourceType::TOPIC, $topic, self::PRINCIPAL, AclOperation::READ);

        self::assertSame([null], $this->admin->createAcls([$acl]), 'the first write');
        self::assertSame([null], $this->admin->createAcls([$acl]), 'and the second one, of the very same acl');

        $written = $this->awaitAcls(AclBindingFilter::matching(ResourceType::TOPIC, $topic), 1);

        self::assertCount(1, $written, 'the authorizer keeps a set, so the acl is there exactly once');
    }

    public function testACreationTheBrokerRefusesIsReportedInItsOwnEntry(): void
    {
        $topic = self::uniqueTopicName(self::PREFIX . '-refused');
        $valid = AclBinding::allow(ResourceType::TOPIC, $topic, self::PRINCIPAL, AclOperation::READ);
        $empty = new AclBinding(
            new ResourcePattern(ResourceType::TOPIC, ''),
            AccessControlEntry::allow(self::PRINCIPAL, AclOperation::READ)
        );

        $result = $this->admin->createAcls([$valid, $empty]);

        self::assertNull($result[0], 'the valid creation was written, and the refused one did not stop it');
        self::assertInstanceOf(InvalidRequestException::class, $result[1]);
        self::assertStringContainsString(
            'Invalid empty resource name',
            $result[1]->getMessage(),
            'the message `AclApis.handleCreateAcls` @ 3.9.2 words itself'
        );
    }

    public function testAClusterAclUnderTheWrongNameIsRefusedAsWell(): void
    {
        $result = $this->admin->createAcls([
            AclBinding::allow(ResourceType::CLUSTER, self::PREFIX . '-not-the-cluster', self::PRINCIPAL, AclOperation::ALTER),
        ]);

        self::assertInstanceOf(InvalidRequestException::class, $result[0]);
        self::assertStringContainsString(ResourceType::CLUSTER_NAME, $result[0]->getMessage());
    }

    /**
     * A pattern type that only a filter may carry is answered -1, because the broker cannot even read the entry
     */
    public function testACreationWithAFilterPatternTypeIsAnUnknownServerError(): void
    {
        $topic = self::uniqueTopicName(self::PREFIX . '-pattern');

        $result = $this->admin->createAcls([
            new AclBinding(
                new ResourcePattern(ResourceType::TOPIC, $topic, PatternType::ANY),
                AccessControlEntry::allow(self::PRINCIPAL, AclOperation::READ)
            ),
        ]);

        self::assertInstanceOf(UnknownErrorException::class, $result[0]);
        self::assertFalse(
            PatternType::isSpecific(PatternType::ANY),
            '`CreateAclsRequest.aclBinding` @ 3.9.2 throws an IllegalArgumentException before any validation of'
            . ' the api runs, which ApiError can only map to the unknown error - so a client checks it itself'
        );
    }

    /**
     * The `USER` resource type and the `CREATE_TOKENS` operation are the two values Kafka 3.3 added (KIP-373)
     */
    public function testTheUserResourceTypeOfTheVersionThreeIsStoredLikeAnyOtherResource(): void
    {
        $acl = AclBinding::allow(
            ResourceType::USER,
            self::UNPRIVILEGED_USER,
            self::PRINCIPAL,
            AclOperation::CREATE_TOKENS
        );

        self::assertSame([null], $this->admin->createAcls([$acl]));

        $written = $this->awaitAcls(AclBindingFilter::ofResourceType(ResourceType::USER), 1);

        self::assertSame(
            ['USER:LITERAL:' . self::UNPRIVILEGED_USER . ' ' . self::PRINCIPAL . ' * CREATE_TOKENS ALLOW'],
            self::sorted($written),
            'the resource type 7 and the operation 13 that no version below 3 may send'
        );
        self::assertSame(ResourceType::USER, $written[0]->pattern->resourceType);
        self::assertSame(AclOperation::CREATE_TOKENS, $written[0]->entry->operation);
        self::assertSame(AclPermissionType::ALLOW, $written[0]->entry->permissionType);
    }

    /**
     * Every one of the three apis refuses the one principal outside `super.users`, each in its own field
     */
    public function testThePrincipalOutsideTheSuperUsersIsRefusedByAllThreeApis(): void
    {
        $unprivileged = $this->unprivilegedAdminClient();
        $acl          = AclBinding::allow(
            ResourceType::TOPIC,
            self::uniqueTopicName(self::PREFIX . '-unauthorized'),
            self::PRINCIPAL,
            AclOperation::READ
        );

        try {
            $unprivileged->describeAcls();
            self::fail('The unprivileged principal was allowed to describe the acls of the cluster');
        } catch (ClusterAuthorizationFailedException $refused) {
            // `AuthHelper.authorizeClusterOperation` @ 4.1.0 names the operation the principal lacks; 3.9.2 and
            // 4.0.x said "Request … is not authorized."
            self::assertStringContainsString(
                'needs DESCRIBE permission.',
                $refused->getMessage(),
                'the message is the whole request object of the broker, which the code 31 is the truth of'
            );
        }

        $created = $unprivileged->createAcls([$acl]);
        self::assertInstanceOf(ClusterAuthorizationFailedException::class, $created[0]);
        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $created[0]->getCode());

        $deleted = $unprivileged->deleteAcls([AclBindingFilter::of($acl)]);
        self::assertInstanceOf(ClusterAuthorizationFailedException::class, $deleted[0]);
        self::assertSame(
            KafkaException::CLUSTER_AUTHORIZATION_FAILED,
            $deleted[0]->getCode(),
            'CreateAcls and DeleteAcls have no top-level error code, so the 31 stands in every entry'
        );
    }

    /**
     * The point of the whole api: an acl changes what a principal may do
     *
     * `acltest` is refused the metadata of a topic it has no acl for - `KafkaApis.handleTopicMetadataRequest` @
     * 3.9.2 authorizes `DESCRIBE` on every topic of the request and answers **29** per topic - and is answered the
     * very same request once a `DESCRIBE` acl for that topic exists.
     */
    public function testAnAclChangesWhatThePrincipalMaySee(): void
    {
        $topic = self::uniqueTopicName(self::PREFIX . '-metadata');
        $this->createTopic($topic);

        $unprivileged = $this->unprivilegedStream();
        new MetadataRequest([$topic], false, self::CLIENT_ID, 6100)->writeTo($unprivileged);
        $refused = MetadataResponse::unpack($unprivileged);

        self::assertSame(
            KafkaException::TOPIC_AUTHORIZATION_FAILED,
            $refused->topics[$topic]->topicErrorCode,
            'a topic the principal may not DESCRIBE is answered 29, with no partition of its own'
        );
        self::assertSame([], $refused->topics[$topic]->partitions);

        self::assertSame(
            [null],
            $this->admin->createAcls([
                AclBinding::allow(ResourceType::TOPIC, $topic, self::PRINCIPAL, AclOperation::DESCRIBE),
            ])
        );
        $this->awaitAcls(AclBindingFilter::matching(ResourceType::TOPIC, $topic), 1);

        $allowed = $this->awaitMetadata($topic);

        self::assertSame(
            KafkaException::NO_ERROR,
            $allowed->topics[$topic]->topicErrorCode,
            'and with the acl the same principal is answered the topic'
        );
        self::assertNotSame([], $allowed->topics[$topic]->partitions, 'with its partitions');
    }

    /**
     * KIP-1276 (`metadata.version` 4.4-IV1): an IPv4 or IPv6 CIDR block is a host of an acl, and is kept verbatim
     *
     * `AclControlManager.validateHostPattern` @ 4.4.0 parses a host with a `/` as a CIDR block and stores the string
     * as it was written - `10.0.0.1/24` is not normalized to its network address -, and a host filter of
     * DescribeAcls compares the string, so the filter `10.0.0.0/8` finds the acl and an address inside the block
     * does not.
     */
    public function testACidrBlockIsTheHostOfAnAclAndIsDescribedBackVerbatim(): void
    {
        $topic = self::uniqueTopicName(self::PREFIX . '-cidr');
        $hosts = ['10.0.0.0/8', '2001:db8::/32', '10.0.0.1/24'];

        self::assertSame(
            [null, null, null],
            $this->admin->createAcls(array_map(
                static fn(string $host): AclBinding => AclBinding::allow(
                    ResourceType::TOPIC,
                    $topic,
                    self::PRINCIPAL,
                    AclOperation::READ,
                    PatternType::LITERAL,
                    $host
                ),
                $hosts
            )),
            'an IPv4 block, an IPv6 block and a block written with a host address in it'
        );

        $written = $this->awaitAcls(AclBindingFilter::matching(ResourceType::TOPIC, $topic), 3);
        $described = array_map(static fn(AclBinding $acl): string => $acl->entry->host, $written);
        sort($described);
        $expected = $hosts;
        sort($expected);

        self::assertSame($expected, $described, 'every block comes back as the string it was written as');

        $byHost = static fn(string $host): AclBindingFilter => new AclBindingFilter(
            new ResourcePatternFilter(ResourceType::TOPIC, $topic, PatternType::LITERAL),
            new AccessControlEntryFilter(self::PRINCIPAL, $host)
        );
        self::assertCount(1, $this->admin->describeAcls($byHost('10.0.0.0/8')), 'the filter compares the string');
        self::assertCount(0, $this->admin->describeAcls($byHost('10.1.2.3')), 'and does not match an address in it');
    }

    /**
     * A host with a `/` that is no CIDR block is the 42 of the creation, with the message of the controller
     */
    public function testAnInvalidCidrBlockIsRefusedWithTheMessageOfTheController(): void
    {
        $topic    = self::uniqueTopicName(self::PREFIX . '-cidr-invalid');
        $creation = static fn(string $host): AclBinding => AclBinding::allow(
            ResourceType::TOPIC,
            $topic,
            self::PRINCIPAL,
            AclOperation::READ,
            PatternType::LITERAL,
            $host
        );

        $result = $this->admin->createAcls([
            $creation('10.0.0.0/33'),
            $creation('2001:db8::/129'),
            $creation('300.0.0.0/8'),
            $creation('10.0.0.0/24'),
        ]);

        $messages = [];
        foreach (array_slice($result, 0, 3) as $refused) {
            self::assertInstanceOf(InvalidRequestException::class, $refused);
            self::assertSame(KafkaException::INVALID_REQUEST, $refused->getCode());
            $messages[] = $refused->getContext()['error'];
        }
        self::assertSame(
            [
                "Invalid CIDR notation '10.0.0.0/33': Value [33] not in range [0,32]",
                "Invalid CIDR notation '2001:db8::/129': Could not parse [2001:db8::/129] - prefix length must be"
                    . ' between 0 and 128',
                "Invalid CIDR notation '300.0.0.0/8': Value [300] not in range [0,255]",
            ],
            $messages
        );
        self::assertNull($result[3], 'the valid block of the same request was written');
        self::assertCount(1, $this->awaitAcls(AclBindingFilter::matching(ResourceType::TOPIC, $topic), 1));
    }

    /**
     * The authorizer matches the address the node sees for the client against the block of the acl
     *
     * The node sees a client of the host through the gateway of its Docker network, not as 127.0.0.1: the address is
     * read from the authorizer log of the node, which names the host of every refusal. An ALLOW on a block without
     * that address changes nothing, an ALLOW on its /24 opens the topic, and a DENY on its /32 closes it again - a
     * DENY wins over an ALLOW, CIDR or not (`StandardAuthorizerData.findResult` @ 4.4.0, `CidrUtils.isInRange`).
     */
    public function testTheAuthorizationOfThePrincipalFollowsTheBlockOfItsAddress(): void
    {
        $topic = self::uniqueTopicName(self::PREFIX . '-cidr-auth');
        $this->createTopic($topic);

        try {
            self::assertSame(KafkaException::TOPIC_AUTHORIZATION_FAILED, $this->unprivilegedMetadataCode($topic, 6110));
            $address = $this->addressTheNodeSeesFor($topic);

            self::assertNotFalse(ip2long($address), "the node sees the client as the IPv4 address {$address}");
            $network = long2ip(ip2long($address) & 0xFFFFFF00) . '/24';
            self::assertStringStartsNotWith('192.0.2.', $address, 'TEST-NET-1 is never the address of a client');

            $describe = fn(string $host, bool $allow): ?KafkaException => $this->admin->createAcls([
                $allow
                    ? AclBinding::allow(ResourceType::TOPIC, $topic, self::PRINCIPAL, AclOperation::DESCRIBE, PatternType::LITERAL, $host)
                    : AclBinding::deny(ResourceType::TOPIC, $topic, self::PRINCIPAL, AclOperation::DESCRIBE, PatternType::LITERAL, $host),
            ])[0];

            self::assertNull($describe('192.0.2.0/24', true));
            $this->awaitAcls(AclBindingFilter::matching(ResourceType::TOPIC, $topic), 1);
            self::assertSame(
                KafkaException::TOPIC_AUTHORIZATION_FAILED,
                $this->unprivilegedMetadataCode($topic, 6111),
                'an ALLOW on a block without the address of the client changes nothing'
            );

            self::assertNull($describe($network, true));
            $this->awaitAcls(AclBindingFilter::matching(ResourceType::TOPIC, $topic), 2);
            self::assertSame(
                KafkaException::NO_ERROR,
                $this->awaitMetadata($topic)->topics[$topic]->topicErrorCode,
                "an ALLOW on {$network} opens the topic to the client at {$address}"
            );

            self::assertNull($describe($address . '/32', false));
            $this->awaitAcls(AclBindingFilter::matching(ResourceType::TOPIC, $topic), 3);
            self::assertSame(
                KafkaException::TOPIC_AUTHORIZATION_FAILED,
                $this->unprivilegedMetadataCode($topic, 6112),
                "a DENY on {$address}/32 closes it again"
            );
        } finally {
            $this->admin->deleteTopics([$topic]);
        }
    }

    /**
     * Asks for the metadata of a topic as the unprivileged principal and returns the error code of the topic
     */
    private function unprivilegedMetadataCode(string $topic, int $correlationId): int
    {
        $stream = $this->unprivilegedStream();
        new MetadataRequest([$topic], false, self::CLIENT_ID, $correlationId)->writeTo($stream);

        return MetadataResponse::unpack($stream)->topics[$topic]->topicErrorCode;
    }

    /**
     * Reads the address the node saw for the refused request on a topic out of the authorizer log of the node
     *
     * `kafka.authorizer.logger` writes every refusal as "Principal = User:acltest is Denied operation = DESCRIBE
     * from host = 172.18.0.1 on resource = Topic:LITERAL:<topic> …" into `logs/kafka-authorizer.log`.
     */
    private function addressTheNodeSeesFor(string $topic): string
    {
        $container = getenv('KAFKA_CONTAINER');
        $container = $container === false || trim($container) === '' ? 'kafka-4-4-0' : trim($container);
        $pattern   = '/Principal = ' . preg_quote(self::PRINCIPAL, '/') . ' is Denied operation = DESCRIBE from host = (\S+)'
            . ' on resource = Topic:LITERAL:' . preg_quote($topic, '/') . ' /';

        $deadline = microtime(true) + 15.0;
        do {
            $output = [];
            exec(
                sprintf(
                    'docker exec %s grep -hF %s /opt/kafka/logs/kafka-authorizer.log 2>&1',
                    escapeshellarg($container),
                    escapeshellarg('resource = Topic:LITERAL:' . $topic . ' ')
                ),
                $output
            );
            foreach ($output as $line) {
                if (preg_match($pattern, $line, $match) === 1) {
                    return $match[1];
                }
            }
            usleep(200000);
        } while (microtime(true) < $deadline);

        self::fail("The authorizer log of {$container} names no refusal of {$topic}: " . implode("\n", $output));
    }

    /**
     * Polls the acls of a filter until the authorizer of the broker has replayed what the controller wrote
     *
     * @return list<AclBinding>
     */
    private function awaitAcls(AclBindingFilter $filter, int $expected): array
    {
        $deadline = microtime(true) + 15.0;
        do {
            $bindings = $this->admin->describeAcls($filter);
            if (count($bindings) === $expected) {
                return $bindings;
            }
            usleep(20000);
        } while (microtime(true) < $deadline);

        self::fail("The node did not report {$expected} acl(s) of the filter in time");
    }

    /**
     * Polls the metadata of a topic as the unprivileged principal until it is no longer refused
     */
    private function awaitMetadata(string $topic): MetadataResponse
    {
        $deadline = microtime(true) + 15.0;
        do {
            $stream = $this->unprivilegedStream();
            new MetadataRequest([$topic], false, self::CLIENT_ID, 6101)->writeTo($stream);
            $answer = MetadataResponse::unpack($stream);
            if ($answer->topics[$topic]->topicErrorCode === KafkaException::NO_ERROR) {
                return $answer;
            }
            usleep(20000);
        } while (microtime(true) < $deadline);

        self::fail("The acl did not reach the authorizer of the broker in time for {$topic}");
    }

    /**
     * Creates a topic of this class and waits until the node serves it
     */
    private function createTopic(string $topic): void
    {
        $this->admin->createTopics([new NewTopic($topic, 1, 1)]);

        $deadline = microtime(true) + 30.0;
        do {
            $stream = $this->connect();
            new MetadataRequest([$topic], false, self::CLIENT_ID, 6102)->writeTo($stream);
            $answer = MetadataResponse::unpack($stream);
            $entry  = $answer->topics[$topic] ?? null;
            if ($entry !== null && $entry->topicErrorCode === KafkaException::NO_ERROR && $entry->partitions !== []) {
                return;
            }
            usleep(50000);
        } while (microtime(true) < $deadline);

        self::fail("The topic {$topic} did not become servable in time");
    }

    /**
     * An admin client that authenticates as the one principal the authorizer of the node applies to
     */
    private function unprivilegedAdminClient(): AdminClient
    {
        $configuration = $this->unprivilegedConfiguration();

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }

    /**
     * A raw stream of that principal, for the apis this class sends by hand
     */
    private function unprivilegedStream(): SocketStream
    {
        return new SocketStream(
            'tcp://' . $this->saslServer(),
            $this->unprivilegedConfiguration(),
            5.0
        );
    }

    /**
     * @return array<string, mixed>
     */
    private function unprivilegedConfiguration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . $this->saslServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::SECURITY_PROTOCOL         => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM            => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME             => self::UNPRIVILEGED_USER,
            ClientConfig::SASL_PASSWORD             => self::UNPRIVILEGED_PASSWORD,
        ];
    }

    /**
     * The SASL listener of the node, without which no principal but the anonymous one exists
     */
    private function saslServer(): string
    {
        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, an acl needs a principal');
        }

        return self::saslBootstrapServer();
    }

    /**
     * The configuration of the super user, i.e. the anonymous principal of the PLAINTEXT listener
     *
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }

    /**
     * Returns the string form of every acl of a list, sorted, so that the order of the broker does not matter
     *
     * @param list<AclBinding> $bindings
     *
     * @return list<string>
     */
    private static function sorted(array $bindings): array
    {
        $texts = array_map(strval(...), $bindings);
        sort($texts);

        return $texts;
    }
}

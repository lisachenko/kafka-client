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
use Protocol\Kafka\Admin\AlterConfigOp;
use Protocol\Kafka\Admin\ConfigResource;
use Protocol\Kafka\Admin\NewTopic;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\InvalidConfigException;
use Protocol\Kafka\Common\Errors\InvalidRequestException;
use Protocol\Kafka\Common\Errors\UnknownErrorException;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestAlterableConfig;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsResponseResource;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsRequest;
use Protocol\Kafka\Protocol\Request\IncrementalAlterConfigsResponse;

/**
 * Exercises the IncrementalAlterConfigs api (key 44, v0) against a real Kafka 2.8.2 broker.
 *
 * KIP-339 added the api in Kafka 2.3 to replace the AlterConfigs of KIP-133, whose request carries the WHOLE
 * configuration a resource should have afterwards. Every test of this class works on a topic of its own, so that
 * nothing it changes can reach another suite of the shared container; the only broker resource it names is asked
 * with `validate_only`, which validates the change and writes nothing.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(AlterConfigOp::class)]
#[CoversClass(IncrementalAlterConfigsRequest::class)]
#[CoversClass(IncrementalAlterConfigsResponse::class)]
#[CoversClass(IncrementalAlterConfigsRequestResource::class)]
#[CoversClass(IncrementalAlterConfigsRequestAlterableConfig::class)]
#[CoversClass(IncrementalAlterConfigsResponseResource::class)]
final class IncrementalConfigsApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 't4-incremental';

    private Cluster $cluster;

    private AdminClient $admin;

    /** @var list<string> */
    private array $createdTopics = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cluster = Cluster::bootstrap($this->configuration());
        $this->admin   = new AdminClient($this->cluster, $this->configuration());
    }

    protected function tearDown(): void
    {
        if ($this->createdTopics !== []) {
            $this->admin->deleteTopics($this->createdTopics);
            $this->createdTopics = [];
        }
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
            ClientConfig::REQUEST_TIMEOUT_MS        => 40000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];
    }

    /**
     * Creates a topic of one partition for this test
     */
    private function topic(string $purpose): string
    {
        $topic                 = self::uniqueTopicName("t4-incremental-{$purpose}");
        $this->createdTopics[] = $topic;

        self::assertSame([$topic => null], $this->admin->createTopics([new NewTopic($topic, 1, 1)]));

        return $topic;
    }

    /**
     * The three operations a topic accepts, in one call, and everything they did not name stays as it was
     */
    public function testSetAppendAndDeleteChangeOnlyTheOptionsTheyName(): void
    {
        $topic = $this->topic('operations');
        $key   = ConfigResource::topic($topic)->key();
        $before = $this->valueOf($topic, 'max.message.bytes');

        $result = $this->admin->incrementalAlterConfigs([
            $key => [
                AlterConfigOp::set('retention.ms', '3600000'),
                AlterConfigOp::append('cleanup.policy', 'compact'),
                AlterConfigOp::delete('segment.bytes'),
            ],
        ]);

        self::assertSame([$key => null], $result, 'the whole resource was applied');
        self::assertSame('3600000', $this->valueOf($topic, 'retention.ms'), 'the SET');
        self::assertSame(
            'delete,compact',
            $this->valueOf($topic, 'cleanup.policy'),
            'the APPEND started from the default of the option, which is `delete`'
        );
        self::assertSame(
            $before,
            $this->valueOf($topic, 'max.message.bytes'),
            'and an option the request never named kept the value it had'
        );
    }

    /**
     * SUBTRACT removes values from a list option and leaves the rest of the list alone
     */
    public function testSubtractRemovesAValueFromAListOption(): void
    {
        $topic = $this->topic('subtract');
        $key   = ConfigResource::topic($topic)->key();

        $this->admin->incrementalAlterConfigs([$key => [AlterConfigOp::append('cleanup.policy', 'compact')]]);
        self::assertSame('delete,compact', $this->valueOf($topic, 'cleanup.policy'));

        $result = $this->admin->incrementalAlterConfigs([$key => [AlterConfigOp::subtract('cleanup.policy', 'compact')]]);

        self::assertSame([$key => null], $result);
        self::assertSame('delete', $this->valueOf($topic, 'cleanup.policy'));
    }

    /**
     * A DELETE puts the option back to the value it inherits, which is not always the documented default
     */
    public function testDeleteGivesTheOptionBackToTheValueItInherits(): void
    {
        $topic    = $this->topic('delete');
        $key      = ConfigResource::topic($topic)->key();
        $inherited = $this->valueOf($topic, 'retention.ms');

        $this->admin->incrementalAlterConfigs([$key => [AlterConfigOp::set('retention.ms', '7200000')]]);
        self::assertSame('7200000', $this->valueOf($topic, 'retention.ms'));

        $result = $this->admin->incrementalAlterConfigs([$key => [AlterConfigOp::delete('retention.ms')]]);

        self::assertSame([$key => null], $result);
        self::assertSame($inherited, $this->valueOf($topic, 'retention.ms'));
    }

    /**
     * An APPEND is only allowed for an option whose `ConfigDef.Type` is LIST - `retention.ms` is a long
     */
    public function testAnAppendToAnOptionThatIsNotAListIsRefusedWithFortyTwo(): void
    {
        $topic = $this->topic('append-scalar');
        $key   = ConfigResource::topic($topic)->key();
        $before = $this->valueOf($topic, 'retention.ms');

        $error = $this->admin->incrementalAlterConfigs([$key => [AlterConfigOp::append('retention.ms', '1000')]])[$key];

        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertSame(
            'Config value append is not allowed for config key: retention.ms',
            $error->getContext()['error']
        );
        self::assertSame($before, $this->valueOf($topic, 'retention.ms'), 'and nothing of the resource was applied');
    }

    /**
     * The changes of one resource are validated together: naming an option twice refuses all of them
     */
    public function testTheSameOptionTwiceRefusesTheWholeResource(): void
    {
        $topic = $this->topic('duplicate');
        $key   = ConfigResource::topic($topic)->key();

        $error = $this->admin->incrementalAlterConfigs([
            $key => [
                AlterConfigOp::set('retention.ms', '3600000'),
                AlterConfigOp::set('retention.ms', '7200000'),
            ],
        ])[$key];

        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertSame('Error due to duplicate config keys : retention.ms', $error->getContext()['error']);
    }

    /**
     * A null value is only allowed for a DELETE, and `AlterConfigOp::set()` cannot even build one
     */
    public function testANullValueOutsideADeleteIsRefusedByTheBroker(): void
    {
        $topic = $this->topic('null-value');
        $key   = ConfigResource::topic($topic)->key();

        $error = $this->admin->incrementalAlterConfigs([
            $key => [new AlterConfigOp('retention.ms', null, AlterConfigOp::SET)],
        ])[$key];

        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertSame('Null value not supported for : SET:retention.ms', $error->getContext()['error']);
    }

    /**
     * An unknown option name is 40 for a SET - and the **unknown server error** for an APPEND, see the document
     */
    public function testAnUnknownOptionNameIsFortyForASetAndMinusOneForAnAppend(): void
    {
        $topic = $this->topic('unknown-option');
        $key   = ConfigResource::topic($topic)->key();

        $set = $this->admin->incrementalAlterConfigs([$key => [AlterConfigOp::set('not.an.option', '1')]])[$key];

        self::assertInstanceOf(InvalidConfigException::class, $set);
        self::assertSame('Unknown topic config name: not.an.option', $set->getContext()['error']);

        $append = $this->admin->incrementalAlterConfigs([$key => [AlterConfigOp::append('not.an.option', '1')]])[$key];

        self::assertInstanceOf(UnknownErrorException::class, $append);
        self::assertArrayNotHasKey(
            'error',
            $append->getContext(),
            'the NoSuchElementException of `listType()` reaches the client without a message at all'
        );
    }

    /**
     * `validate_only` answers what the change would be and writes nothing
     */
    public function testValidateOnlyChangesNothing(): void
    {
        $topic  = $this->topic('validate-only');
        $key    = ConfigResource::topic($topic)->key();
        $before = $this->valueOf($topic, 'retention.ms');

        $result = $this->admin->incrementalAlterConfigs(
            [$key => [AlterConfigOp::set('retention.ms', '999000')]],
            true
        );

        self::assertSame([$key => null], $result, 'the change is valid');
        self::assertSame($before, $this->valueOf($topic, 'retention.ms'), 'and it was not written');
    }

    /**
     * The broker resource of KIP-226, asked with `validate_only` so that the shared container is never changed
     */
    public function testABrokerOptionThatIsNotDynamicIsRefusedForTheWholeResource(): void
    {
        $key = ConfigResource::defaultBroker()->key();

        $error = $this->admin->incrementalAlterConfigs(
            [$key => [AlterConfigOp::set('log.dirs', '/tmp/kafka-logs')]],
            true
        )[$key];

        self::assertInstanceOf(InvalidRequestException::class, $error);
        self::assertStringContainsString(
            'Cannot update these configs dynamically: Set(log.dirs)',
            (string) $error->getContext()['error']
        );

        $dynamic = $this->admin->incrementalAlterConfigs(
            [$key => [AlterConfigOp::set('log.retention.ms', '604800000')]],
            true
        )[$key];

        self::assertNull($dynamic, 'a dynamic option of the cluster-wide default validates');
    }

    /**
     * Reads one option of a topic back through DescribeConfigs
     */
    private function valueOf(string $topic, string $option): ?string
    {
        $key = ConfigResource::topic($topic)->key();

        return $this->admin->describeConfigs([ConfigResource::topic($topic)])[$key]->get($option)?->value;
    }
}

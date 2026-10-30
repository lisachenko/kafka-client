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
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\ClusterAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\ControllerIdNotRegisteredException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Tests\Fixture\BrokerConnection;
use Protocol\Kafka\Tests\Fixture\ResponseFrame;
use Protocol\Kafka\Tests\Fixture\ScriptedConnections;

/**
 * `AdminClient::unregisterController()` (key 94, Kafka 4.4, KIP-1312) against scripted brokers
 *
 * @see docs/protocol/4.4.md, section "UnregisterController API (key 94, v0)"
 */
#[CoversClass(AdminClient::class)]
final class UnregisterControllerAdminTest extends TestCase
{
    private const string BOOTSTRAP_ADDRESS = 'tcp://bootstrap:9092';

    private const string BROKER = 'tcp://kafka-1:9092';

    private ScriptedConnections $brokers;

    protected function setUp(): void
    {
        $this->brokers = new ScriptedConnections();
    }

    protected function tearDown(): void
    {
        ScriptedConnections::uninstall();
    }

    public function testAnUnregisteredControllerIsRemovedWithOneRequestToABroker(): void
    {
        $broker = new BrokerConnection(self::answer(KafkaException::NO_ERROR, null));
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::BROKER, $broker)
            ->install();

        $this->adminClient()->unregisterController(4242);

        $frames = $broker->getReceivedFrames();
        self::assertCount(1, $frames);
        self::assertSame(ApiKeys::UNREGISTER_CONTROLLER, unpack('n', substr($frames[0], 0, 2))[1]);
        self::assertSame(0, unpack('n', substr($frames[0], 2, 2))[1], 'the version 0 of Kafka 4.4');
        self::assertStringEndsWith('00001092' . '00', bin2hex($frames[0]), 'the id 4242 and the tag buffer');
    }

    public function testTheCodeOfTheControllerIsThrownWithItsMessage(): void
    {
        $message = 'Controller ID 4242 is not currently registered.';
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::BROKER, new BrokerConnection(
                self::answer(KafkaException::CONTROLLER_ID_NOT_REGISTERED, $message)
            ))
            ->install();

        try {
            $this->adminClient()->unregisterController(4242);
            self::fail('The 136 is expected to be thrown');
        } catch (ControllerIdNotRegisteredException $exception) {
            self::assertSame(KafkaException::CONTROLLER_ID_NOT_REGISTERED, $exception->getCode());
            self::assertStringContainsString($message, $exception->getMessage());
            self::assertStringContainsString('4242', $exception->getMessage());
        }
    }

    public function testAPrincipalWithoutAlterOnTheClusterIsRefusedThe31(): void
    {
        $this->brokers
            ->on(self::BOOTSTRAP_ADDRESS, new BrokerConnection($this->clusterMetadata()))
            ->on(self::BROKER, new BrokerConnection(
                self::answer(KafkaException::CLUSTER_AUTHORIZATION_FAILED, 'Request … needs ALTER permission.')
            ))
            ->install();

        $this->expectException(ClusterAuthorizationFailedException::class);
        $this->adminClient()->unregisterController(4242);
    }

    /**
     * An UnregisterController v0 answer, flexible: throttle time, error code, compact nullable message
     */
    private static function answer(int $errorCode, ?string $message): string
    {
        return ResponseFrame::flexible(
            0,
            pack('N', 0) . pack('n', $errorCode)
                . ($message === null ? "\x00" : chr(strlen($message) + 1) . $message)
        );
    }

    private function clusterMetadata(): string
    {
        return ResponseFrame::metadata(0, [[1, 'kafka-1', 9092]], []);
    }

    private function adminClient(): AdminClient
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => [self::BOOTSTRAP_ADDRESS],
            ClientConfig::CLIENT_ID                 => 't4-44',
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 1000,
            ClientConfig::RETRY_BACKOFF_MS          => 1,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }
}

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

use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use Protocol\Kafka\Admin\AdminClient;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\ClusterAuthorizationFailedException;
use Protocol\Kafka\Common\Errors\ControllerIdNotRegisteredException;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\Protocol\Request\UnregisterControllerRequest;
use Protocol\Kafka\Protocol\Request\UnregisterControllerResponse;

/**
 * UnregisterController v0 (key 94, Kafka 4.4, KIP-1312) on the 4.4.0 node
 *
 * The node is one combined KRaft node whose id 1 is the one controller and the one voter of its quorum, and the
 * controller writes the `UnregisterControllerRecord` of a registered id without asking whether it is still a voter
 * or its own: **no request of this class ever names the id 1**, {@see self::unregister()} refuses it before a byte
 * is written. What is measured is everything around it - the 136 of an id the controller holds no registration
 * for, the absent range check of the id, and the 31 of a principal without `ALTER` on the cluster.
 *
 * @see docs/protocol/overview.md, section "UnregisterController API (key 94, v0)"
 */
#[CoversClass(AdminClient::class)]
#[CoversClass(UnregisterControllerRequest::class)]
#[CoversClass(UnregisterControllerResponse::class)]
final class UnregisterControllerApiTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t4-44-controller';

    /**
     * A controller id no node of this repository has
     */
    private const int ABSENT_CONTROLLER_ID = 4242;

    /**
     * The id of the one controller of the node, which no test may unregister
     */
    private const int NODE_CONTROLLER_ID = 1;

    public function testAnIdWithoutARegistrationIsTheCode136WithTheMessageOfTheController(): void
    {
        $refused = $this->unregister($this->admin(), self::ABSENT_CONTROLLER_ID);

        self::assertInstanceOf(ControllerIdNotRegisteredException::class, $refused);
        self::assertSame(KafkaException::CONTROLLER_ID_NOT_REGISTERED, $refused->getCode());
        self::assertSame('Controller ID 4242 is not currently registered.', $refused->getContext()['error']);
        self::assertSame(self::ABSENT_CONTROLLER_ID, $refused->getContext()['controllerId']);
    }

    public function testTheControllerDoesNotCheckTheRangeOfTheId(): void
    {
        $refused = $this->unregister($this->admin(), -1);

        self::assertInstanceOf(ControllerIdNotRegisteredException::class, $refused);
        self::assertSame('Controller ID -1 is not currently registered.', $refused->getContext()['error']);
    }

    /**
     * The frame of the answer is the one of the vector `unregistercontroller.response.v0.unknown`, byte for byte
     */
    public function testTheAnswerIsTheFlexibleFrameOfTheCodeAndTheMessage(): void
    {
        $stream  = $this->connect();
        $request = new UnregisterControllerRequest(self::ABSENT_CONTROLLER_ID, self::CLIENT_ID, 4410);
        $request->writeTo($stream);
        $response = UnregisterControllerResponse::unpack($stream);

        self::assertSame(
            '0000003c0000113a0000000000008830' . bin2hex('Controller ID 4242 is not currently registered.') . '00',
            bin2hex((string) $response)
        );
    }

    /**
     * `acltest` holds no acl on the cluster, and the controller authorizes `ALTER` on it before it looks at the id
     */
    public function testAPrincipalWithoutAlterOnTheClusterIsRefusedTheCode31(): void
    {
        if (self::saslBootstrapServer() === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set, the refusal needs a principal');
        }
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::saslBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::SECURITY_PROTOCOL         => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM            => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME             => 'acltest',
            ClientConfig::SASL_PASSWORD             => 'acltest-secret',
        ];

        $refused = $this->unregister(
            new AdminClient(Cluster::bootstrap($configuration), $configuration),
            self::ABSENT_CONTROLLER_ID
        );

        self::assertInstanceOf(ClusterAuthorizationFailedException::class, $refused);
        self::assertSame(KafkaException::CLUSTER_AUTHORIZATION_FAILED, $refused->getCode());

        $message = (string) $refused->getContext()['error'];
        self::assertStringEndsWith(' needs ALTER permission.', $message);
        self::assertStringStartsWith('Request Request(', $message, 'the message is the toString() of the request');
        self::assertStringContainsString('listenerName=ListenerName(CONTROLLER)', $message, 'as the controller got it');
        self::assertStringContainsString('envelope=Optional[Request(', $message, 'forwarded by the broker');
    }

    /**
     * Sends the request through the admin client and returns its refusal - never for the id of the node itself
     */
    private function unregister(AdminClient $admin, int $controllerId): KafkaException
    {
        if ($controllerId === self::NODE_CONTROLLER_ID) {
            throw new LogicException('The controller 1 is the one controller and the one voter of the node');
        }

        try {
            $admin->unregisterController($controllerId);
        } catch (KafkaException $refused) {
            return $refused;
        }

        self::fail("The controller id {$controllerId} was unregistered");
    }

    private function admin(): AdminClient
    {
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
        ];

        return new AdminClient(Cluster::bootstrap($configuration), $configuration);
    }
}

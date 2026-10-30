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
use Protocol\Kafka\Client;
use Protocol\Kafka\Common\ClientConfig;
use Protocol\Kafka\Common\Cluster;
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Errors\RebootstrapRequiredException;
use Protocol\Kafka\Common\Node;
use Protocol\Kafka\Common\Security\SaslMechanism;
use Protocol\Kafka\Common\Security\SecurityProtocol;
use Protocol\Kafka\IO\SocketStream;
use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequest;
use Protocol\Kafka\Protocol\Request\ApiVersionsRequestV4;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponse;
use Protocol\Kafka\Protocol\Request\ApiVersionsResponseV4;

/**
 * The misrouted-connection check of ApiVersions v5 against the 4.4.0 node (Kafka 4.4, KIP-1242)
 *
 * A version 5 request names the cluster and the node it means to reach, and `KafkaApis.handleApiVersionsRequest`
 * @ 4.4.0 answers a pair that does not name the broker itself with the 129 `REBOOTSTRAP_REQUIRED`, while
 * `ApiVersionsRequest.isValid()` refuses half a pair with the 42. The node is node 1 of one cluster, so "another
 * node" is any other id at its address and "another cluster" any other id; nothing here changes the node.
 *
 * @see docs/protocol/overview.md, section "ApiVersions API (key 18, v0 to v5)"
 */
#[CoversClass(ApiVersionsRequest::class)]
#[CoversClass(ApiVersionsRequestV4::class)]
#[CoversClass(ApiVersionsResponse::class)]
#[CoversClass(ApiVersionsResponseV4::class)]
#[CoversClass(Client::class)]
#[CoversClass(AdminClient::class)]
#[CoversClass(Cluster::class)]
final class MisroutedConnectionTest extends IntegrationTestCase
{
    private const string CLIENT_ID = 'kafka-client-t1-44';

    /**
     * A cluster id of the right length that no cluster has: 22 characters of the url-safe base64 of 16 zero bits
     */
    private const string FOREIGN_CLUSTER_ID = 'AAAAAAAAAAAAAAAAAAAAAA';

    /**
     * Metadata cache file of the test that needs one, removed in tearDown
     */
    private ?string $cacheFile = null;

    protected function tearDown(): void
    {
        if ($this->cacheFile !== null && is_file($this->cacheFile)) {
            unlink($this->cacheFile);
        }
        parent::tearDown();
    }

    public function testAVersionFiveThatNamesThisNodeOfThisClusterIsAnswered(): void
    {
        $cluster  = Cluster::bootstrap($this->configuration());
        $node     = $this->onlyNode($cluster);
        $response = $this->ask(new ApiVersionsRequest(self::CLIENT_ID, 4410, $cluster->clusterId(), $node->nodeId));

        self::assertNotNull($cluster->clusterId(), 'a KRaft cluster always has an id');
        self::assertSame(22, strlen((string) $cluster->clusterId()));
        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertCount(77, $response->apiVersions, 'the full table of the client listener');
        self::assertSame(5, $response->maxVersionOf(ApiKeys::API_VERSIONS), 'Kafka 4.4 serves ApiVersions v0 to v5');
    }

    public function testAVersionFiveWithNeitherIdIsNotChecked(): void
    {
        $response = $this->ask(new ApiVersionsRequest(self::CLIENT_ID, 4411));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode, 'the null cluster id and -1 turn the check off');
        self::assertCount(77, $response->apiVersions);
    }

    /**
     * Another cluster id is the 129, in the layout of the version 5 and with an empty api array
     */
    public function testAForeignClusterIdIsTheRebootstrapRequired(): void
    {
        $stream   = $this->connect();
        $response = $this->askOn($stream, new ApiVersionsRequest(self::CLIENT_ID, 4412, self::FOREIGN_CLUSTER_ID, 1));

        self::assertSame(KafkaException::REBOOTSTRAP_REQUIRED, $response->errorCode);
        self::assertSame([], $response->apiVersions, 'only the 35 of KIP-511 carries a row of its own');
        self::assertSame(0, $response->throttleTimeMs);
        self::assertSame(-1, $response->finalizedFeaturesEpoch, 'no tagged field at all');
        self::assertSame(12, $response->getMessageSize(), 'the correlation id, 129, the compact 0, the throttle, a tag buffer');

        // The broker does not close the connection: the same socket is answered again at once
        $again = $this->askOn($stream, new ApiVersionsRequest(self::CLIENT_ID, 4413));
        $stream->disconnect();

        self::assertSame(4413, $again->getCorrelationId());
        self::assertSame(KafkaException::NO_ERROR, $again->errorCode);
    }

    public function testTheRightClusterIdWithAnotherNodeIdIsTheRebootstrapRequired(): void
    {
        $cluster  = Cluster::bootstrap($this->configuration());
        $node     = $this->onlyNode($cluster);
        $response = $this->ask(new ApiVersionsRequest(self::CLIENT_ID, 4414, $cluster->clusterId(), $node->nodeId + 1));

        self::assertSame(KafkaException::REBOOTSTRAP_REQUIRED, $response->errorCode);
        self::assertSame([], $response->apiVersions);
    }

    /**
     * Half a pair is refused by `ApiVersionsRequest.isValid()` @ 4.4.0 before the ids are compared: the 42
     */
    public function testHalfAPairIsAnInvalidRequest(): void
    {
        $cluster      = Cluster::bootstrap($this->configuration());
        $withoutNode  = $this->ask(new ApiVersionsRequest(self::CLIENT_ID, 4415, $cluster->clusterId(), -1));
        $withoutId    = $this->ask(new ApiVersionsRequest(self::CLIENT_ID, 4416, null, 1));
        $foreignNoId  = $this->ask(new ApiVersionsRequest(self::CLIENT_ID, 4417, self::FOREIGN_CLUSTER_ID, -1));

        self::assertSame(KafkaException::INVALID_REQUEST, $withoutNode->errorCode, 'the right cluster id, the node -1');
        self::assertSame(KafkaException::INVALID_REQUEST, $withoutId->errorCode, 'a node id, the null cluster id');
        self::assertSame(
            KafkaException::INVALID_REQUEST,
            $foreignNoId->errorCode,
            'the validity comes first: a foreign cluster id next to -1 is the 42, not the 129'
        );
        self::assertSame([], $withoutNode->apiVersions);
    }

    /**
     * A version 4 frame has no place for the two ids: the check cannot apply to it
     */
    public function testTheVersionFourIsNeverAnsweredTheRebootstrapRequired(): void
    {
        $stream = $this->connect();
        new ApiVersionsRequestV4(self::CLIENT_ID, 4418, self::FOREIGN_CLUSTER_ID, 1)->writeTo($stream);
        $response = ApiVersionsResponseV4::unpack($stream);
        $stream->disconnect();

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertCount(77, $response->apiVersions);
    }

    /**
     * An unauthenticated SASL connection is answered without the check: only the 42 of half a pair is made there
     *
     * Before the authentication the ApiVersions of a SASL listener never reaches `KafkaApis`: the
     * `SaslServerAuthenticator.handleApiVersionsRequest` @ 4.4.0 answers it itself, with the 35 of an unknown version,
     * the 42 of `isValid()` - which includes the half pair of KIP-1242 - and otherwise the full answer. The ids of a
     * valid pair are not compared there, so a foreign cluster id is answered 0 on 9094 and 129 on 9092. A connection
     * of this client asks ApiVersions only after it authenticated (`Client::apiVersions()` on a connection of
     * {@see \Protocol\Kafka\Network\ConnectionFactory}), where `KafkaApis` answers it and the check is made.
     */
    public function testAnUnauthenticatedSaslConnectionIsNotChecked(): void
    {
        $address = self::saslBootstrapServer();
        if ($address === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set');
        }

        // One connection each: an unauthenticated SASL connection takes one ApiVersions and then a SaslHandshake,
        // and the node closes it when a second ApiVersions arrives instead ("Unexpected ApiVersions request
        // received during SASL authentication state HANDSHAKE_REQUEST" in the log of the node)
        $foreign  = $this->askOnce($address, new ApiVersionsRequest(self::CLIENT_ID, 4419, self::FOREIGN_CLUSTER_ID, 1));
        $halfPair = $this->askOnce($address, new ApiVersionsRequest(self::CLIENT_ID, 4420, self::FOREIGN_CLUSTER_ID, -1));

        self::assertSame(KafkaException::NO_ERROR, $foreign->errorCode, 'the authenticator does not compare the ids');
        self::assertCount(77, $foreign->apiVersions);
        self::assertSame(KafkaException::INVALID_REQUEST, $halfPair->errorCode, 'but isValid() refuses half a pair');
    }

    /**
     * The SASL client of this package authenticates first and asks ApiVersions afterwards: there the check is made
     */
    public function testAnAuthenticatedSaslConnectionIsChecked(): void
    {
        $address = self::saslBootstrapServer();
        if ($address === '') {
            self::markTestSkipped(self::SASL_BOOTSTRAP_SERVERS_ENV . ' is not set');
        }
        $configuration = [
            ClientConfig::BOOTSTRAP_SERVERS => ['tcp://' . $address],
            ClientConfig::SECURITY_PROTOCOL => SecurityProtocol::SASL_PLAINTEXT,
            ClientConfig::SASL_MECHANISM    => SaslMechanism::PLAIN,
            ClientConfig::SASL_USERNAME     => 'kafkatest',
            ClientConfig::SASL_PASSWORD     => 'kafkatest-secret',
        ] + $this->configuration();
        $cluster = Cluster::bootstrap($configuration);
        $client  = new Client($cluster, $configuration);
        $node    = $this->onlyNode($cluster);

        $stale         = new Node();
        $stale->nodeId = $node->nodeId + 1;
        $stale->host   = $node->host;
        $stale->port   = $node->port;

        self::assertSame(KafkaException::NO_ERROR, $client->apiVersions($node)->errorCode);
        $this->expectException(RebootstrapRequiredException::class);
        $client->apiVersions($stale);
    }

    public function testTheClientNamesTheNodeAndPassesTheCheck(): void
    {
        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration);
        $client        = new Client($cluster, $configuration);

        $response = $client->apiVersions($this->onlyNode($cluster));

        self::assertSame(KafkaException::NO_ERROR, $response->errorCode);
        self::assertCount(77, $response->apiVersions);
        self::assertSame(
            array_keys($response->apiVersions),
            array_keys(new AdminClient($cluster, $configuration)->getApiVersions($this->onlyNode($cluster))),
            'and so does the admin client'
        );
        self::assertArrayHasKey('metadata.version', new AdminClient($cluster, $configuration)->describeFeatures()->finalizedFeatures);
    }

    /**
     * A node id that is not the one of the broker at the address is the 129, and the client recovers from it
     */
    public function testTheClientMapsAWrongNodeToTheRebootstrapRequiredAndRecovers(): void
    {
        $configuration = $this->configuration();
        $cluster       = Cluster::bootstrap($configuration);
        $client        = new Client($cluster, $configuration);
        $node          = $this->onlyNode($cluster);

        // The metadata of a client that still believes node 2 lives at the address of node 1
        $stale         = new Node();
        $stale->nodeId = $node->nodeId + 1;
        $stale->host   = $node->host;
        $stale->port   = $node->port;

        try {
            $client->apiVersions($stale);
            self::fail('the broker at the address is not node ' . $stale->nodeId);
        } catch (RebootstrapRequiredException $expected) {
            self::assertSame(KafkaException::REBOOTSTRAP_REQUIRED, $expected->getCode());
            self::assertSame($stale->nodeId, $expected->getContext()['node']);
            self::assertSame($cluster->clusterId(), $expected->getContext()['clusterId']);
        }

        $recovered = $client->apiVersions($cluster->nodeById($node->nodeId) ?? self::fail('the node is gone'));
        self::assertSame(KafkaException::NO_ERROR, $recovered->errorCode);

        $this->expectException(RebootstrapRequiredException::class);
        new AdminClient($cluster, $configuration)->getApiVersions($stale);
    }

    /**
     * Metadata of another cluster - a stale cache file - is the 129, and the rebootstrap replaces it
     */
    public function testAStaleClusterIdIsRebootstrappedAwayByTheClient(): void
    {
        $this->cacheFile = sys_get_temp_dir() . '/t1-misrouted-cache-' . bin2hex(random_bytes(6)) . '.php';
        $configuration   = [
            ClientConfig::METADATA_CACHE_FILE => $this->cacheFile,
            ClientConfig::METADATA_MAX_AGE_MS => 300000,
        ] + $this->configuration();

        $clusterId = (string) Cluster::bootstrap($configuration)->clusterId();
        $cache     = (string) file_get_contents($this->cacheFile);
        self::assertStringContainsString("'{$clusterId}'", $cache);
        file_put_contents($this->cacheFile, str_replace("'{$clusterId}'", "'" . self::FOREIGN_CLUSTER_ID . "'", $cache));

        // The cluster restores the metadata of the file without asking the node
        $cluster = Cluster::bootstrap($configuration);
        $client  = new Client($cluster, $configuration);
        self::assertSame(self::FOREIGN_CLUSTER_ID, $cluster->clusterId());

        try {
            $client->apiVersions($this->onlyNode($cluster));
            self::fail('the node does not belong to the cluster of the cache file');
        } catch (RebootstrapRequiredException $expected) {
            self::assertSame(self::FOREIGN_CLUSTER_ID, $expected->getContext()['clusterId']);
        }

        self::assertSame($clusterId, $cluster->clusterId(), 'the rebootstrap asked the bootstrap servers');
        self::assertStringContainsString("'{$clusterId}'", (string) file_get_contents($this->cacheFile));
        self::assertSame(KafkaException::NO_ERROR, $client->apiVersions($this->onlyNode($cluster))->errorCode);
    }

    /**
     * Sends one request on a connection of its own and reads the answer with the class of the version 5
     */
    private function ask(ApiVersionsRequest $request): ApiVersionsResponse
    {
        $stream   = $this->connect();
        $response = $this->askOn($stream, $request);
        $stream->disconnect();

        return $response;
    }

    /**
     * Sends one request on a new connection to the given `host:port` and closes it
     */
    private function askOnce(string $address, ApiVersionsRequest $request): ApiVersionsResponse
    {
        $stream   = new SocketStream('tcp://' . $address, [ClientConfig::REQUEST_TIMEOUT_MS => 10000], 5.0);
        $response = $this->askOn($stream, $request);
        $stream->disconnect();

        return $response;
    }

    private function askOn(SocketStream $stream, ApiVersionsRequest $request): ApiVersionsResponse
    {
        $request->writeTo($stream);
        $response = ApiVersionsResponse::unpack($stream);
        self::assertSame(
            (int) unpack('N', substr((string) $request, 8, 4))[1],
            $response->getCorrelationId()
        );

        return $response;
    }

    /**
     * The one node of the 4.4.0 single-node cluster
     */
    private function onlyNode(Cluster $cluster): Node
    {
        $nodes = $cluster->nodes();
        self::assertNotEmpty($nodes, 'The readiness probe must have resolved the cluster before the tests run');

        return reset($nodes);
    }

    /**
     * @return array<string, mixed>
     */
    private function configuration(): array
    {
        return [
            ClientConfig::BOOTSTRAP_SERVERS         => ['tcp://' . self::firstBootstrapServer()],
            ClientConfig::CLIENT_ID                 => self::CLIENT_ID,
            ClientConfig::METADATA_FETCH_TIMEOUT_MS => 30000,
            ClientConfig::RETRY_BACKOFF_MS          => 250,
            ClientConfig::REQUEST_TIMEOUT_MS        => 10000,
        ] + ClientConfig::getDefaultConfiguration();
    }
}

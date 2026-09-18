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
use Protocol\Kafka\Common\Errors\KafkaException;
use Protocol\Kafka\Common\Uuid;
use Protocol\Kafka\Protocol\Data\ClientMetricsResource;
use Protocol\Kafka\Protocol\Request\GetTelemetrySubscriptionsRequest;
use Protocol\Kafka\Protocol\Request\GetTelemetrySubscriptionsResponse;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesRequest;
use Protocol\Kafka\Protocol\Request\ListClientMetricsResourcesResponse;
use Protocol\Kafka\Protocol\Request\PushTelemetryRequest;
use Protocol\Kafka\Protocol\Request\PushTelemetryResponse;

/**
 * Exercises the three client-metrics apis of KIP-714 (keys 71, 72 and 74) against the 3.9.2 KRaft node.
 *
 * They are **wire only** on this line - classes and vectors, no client method and no telemetry emitter - so this
 * class sends every frame by hand on a raw stream and asserts what a node **without a client-metrics receiver
 * plugin** answers. That is the whole point of it: the keys 71 and 72 are missing from the ApiVersions answer of
 * such a node (`ApiVersionManager` @ 3.9.2 filters them while `isTelemetryReceiverConfigured` is false) and are
 * answered all the same, which is a thing only a request can establish.
 *
 * It writes nothing to the broker. A client instance is an entry of an LRU cache that expires after 60 seconds
 * without traffic, so there is nothing to clean up; the `client-metrics` configuration resource that the vectors
 * of these apis were captured against was written and removed by hand and is not recreated here.
 *
 * @see docs/protocol/3.9.md, section "Client metrics (KIP-714) — wire only"
 */
#[CoversClass(GetTelemetrySubscriptionsRequest::class)]
#[CoversClass(GetTelemetrySubscriptionsResponse::class)]
#[CoversClass(PushTelemetryRequest::class)]
#[CoversClass(PushTelemetryResponse::class)]
#[CoversClass(ListClientMetricsResourcesRequest::class)]
#[CoversClass(ListClientMetricsResourcesResponse::class)]
#[CoversClass(ClientMetricsResource::class)]
final class ClientMetricsApiTest extends IntegrationTestCase
{
    /**
     * Client id of this class, so that the frames of the other agents on this node are never mistaken for these
     */
    private const string CLIENT_ID = 'kafka-client-t1-37-it';

    /**
     * Prefix of everything this class would ever create on the node, asserted to be absent from the resource list
     */
    private const string RESOURCE_PREFIX = 't1-37-';

    /**
     * The ids of `CompressionType` a 3.9.2 broker accepts for a pushed metrics blob: zstd, lz4, gzip, snappy
     */
    private const array ACCEPTED_CODECS = [4, 3, 1, 2];

    /**
     * `ClientMetricsConfigs.DEFAULT_INTERVAL_MS` @ 3.9.2, the interval of an instance no subscription matches
     */
    private const int DEFAULT_PUSH_INTERVAL_MS = 300000;

    /**
     * A first GetTelemetrySubscriptions answers the defaults of the broker and asks for no metric at all
     */
    public function testAFirstRequestIsAnsweredAGeneratedInstanceIdAndAnEmptySubscription(): void
    {
        $subscription = $this->subscribe(7101);

        self::assertSame(KafkaException::NO_ERROR, $subscription->errorCode);
        self::assertNotSame(Uuid::ZERO, $subscription->clientInstanceId, 'the broker generates the id');
        self::assertSame(Uuid::SIZE, strlen($subscription->clientInstanceId));
        self::assertSame(self::ACCEPTED_CODECS, $subscription->acceptedCompressionTypes);
        self::assertSame(self::DEFAULT_PUSH_INTERVAL_MS, $subscription->pushIntervalMs);
        self::assertGreaterThan(0, $subscription->telemetryMaxBytes);
        self::assertTrue($subscription->deltaTemporality, 'a 3.9.2 node always asks for deltas');
        self::assertSame(
            [],
            $subscription->requestedMetrics,
            'no client-metrics resource of this node matches this client id, and an empty array is "no metric"'
        );
    }

    /**
     * The same instance asking again inside its push interval is throttled, and told nothing else
     */
    public function testAskingAgainInsideThePushIntervalIsThrottled(): void
    {
        $first = $this->subscribe(7102);
        $again = $this->subscribe(7103, $first->clientInstanceId);

        self::assertSame(KafkaException::THROTTLING_QUOTA_EXCEEDED, $again->errorCode);
        self::assertSame(Uuid::ZERO, $again->clientInstanceId, 'a refusal is every field of the schema default');
        self::assertSame(0, $again->subscriptionId);
        self::assertSame([], $again->acceptedCompressionTypes);
        self::assertSame(0, $again->pushIntervalMs);
        self::assertSame(0, $again->telemetryMaxBytes);
        self::assertFalse($again->deltaTemporality);
    }

    /**
     * A node without a receiver plugin takes the metrics of a known instance and drops them
     */
    public function testAPushOfAKnownInstanceIsAccepted(): void
    {
        $subscription = $this->subscribe(7104);

        $empty = $this->push(7105, $subscription->clientInstanceId, $subscription->subscriptionId);

        self::assertSame(KafkaException::NO_ERROR, $empty->errorCode, 'an empty blob never reaches a plugin');
        self::assertSame(0, $empty->throttleTimeMs);
        self::assertSame(12, $empty->getMessageSize(), 'a throttle time, an error code and the two tag buffers');

        $second = $this->subscribe(7106);
        $blob   = $this->push(7107, $second->clientInstanceId, $second->subscriptionId, "\x0a\x00");

        self::assertSame(
            KafkaException::NO_ERROR,
            $blob->errorCode,
            'a node with an empty ClientMetricsReceiverPlugin accepts a blob and exports it nowhere'
        );
    }

    /**
     * A subscription id that is not the one of the instance is the 117 that Kafka 3.7 added for it
     */
    public function testAPushWithAnotherSubscriptionIdIsUnknownSubscriptionId(): void
    {
        $subscription = $this->subscribe(7108);
        $answer       = $this->push(7109, $subscription->clientInstanceId, $subscription->subscriptionId + 1);

        self::assertSame(KafkaException::UNKNOWN_SUBSCRIPTION_ID, $answer->errorCode);
    }

    /**
     * The zero client instance id is refused before the subscription is even looked up
     */
    public function testAPushWithTheZeroInstanceIdIsInvalidRequest(): void
    {
        $answer = $this->push(7110, Uuid::ZERO, 0);

        self::assertSame(
            KafkaException::INVALID_REQUEST,
            $answer->errorCode,
            'Uuid.RESERVED is checked before anything else, so this is never the 117'
        );
    }

    /**
     * A codec that is no `CompressionType` is the 76, the code the zstd logs of KIP-110 introduced
     */
    public function testAPushWithACodecTheBrokerDoesNotKnowIsUnsupportedCompressionType(): void
    {
        $subscription = $this->subscribe(7111);
        $answer       = $this->push(7112, $subscription->clientInstanceId, $subscription->subscriptionId, "\x01\x02", 7);

        self::assertSame(KafkaException::UNSUPPORTED_COMPRESSION_TYPE, $answer->errorCode);
    }

    /**
     * One byte more than the limit the subscription announced is the 118 of Kafka 3.7
     */
    public function testABlobAboveTheAnnouncedLimitIsTelemetryTooLarge(): void
    {
        $subscription = $this->subscribe(7113);
        $answer       = $this->push(
            7114,
            $subscription->clientInstanceId,
            $subscription->subscriptionId,
            str_repeat("\x00", $subscription->telemetryMaxBytes + 1)
        );

        self::assertSame(KafkaException::TELEMETRY_TOO_LARGE, $answer->errorCode);
    }

    /**
     * A terminating push is accepted outside the interval, and ends the instance for good
     */
    public function testATerminatingPushEndsTheInstance(): void
    {
        $subscription = $this->subscribe(7115);

        $last = $this->push(7116, $subscription->clientInstanceId, $subscription->subscriptionId, '', 0, true);

        self::assertSame(KafkaException::NO_ERROR, $last->errorCode);

        $after = $this->push(7117, $subscription->clientInstanceId, $subscription->subscriptionId);

        self::assertSame(
            KafkaException::INVALID_REQUEST,
            $after->errorCode,
            'a terminated instance accepts nothing else as long as the broker remembers it'
        );
    }

    /**
     * The resource list is answered on a node that has no client-metrics resource, and this class leaves none
     */
    public function testTheResourceListIsAnsweredAndHoldsNothingOfThisClass(): void
    {
        $stream = $this->connect();
        new ListClientMetricsResourcesRequest(self::CLIENT_ID, 7118)->writeTo($stream);
        $answer = ListClientMetricsResourcesResponse::unpack($stream);

        self::assertSame(KafkaException::NO_ERROR, $answer->errorCode);

        foreach ($answer->clientMetricsResources as $name => $resource) {
            self::assertSame($name, $resource->name, 'the array is keyed by the one field a resource has');
            self::assertStringStartsNotWith(
                self::RESOURCE_PREFIX,
                $resource->name,
                'this suite writes no client-metrics resource, and the ones of the vectors were removed'
            );
        }
    }

    /**
     * Asks the node for the subscription of a client instance, or for a new instance with the zero uuid
     */
    private function subscribe(int $correlationId, string $clientInstanceId = Uuid::ZERO): GetTelemetrySubscriptionsResponse
    {
        $stream = $this->connect();
        new GetTelemetrySubscriptionsRequest($clientInstanceId, self::CLIENT_ID, $correlationId)->writeTo($stream);

        return GetTelemetrySubscriptionsResponse::unpack($stream);
    }

    /**
     * Pushes a metrics blob of a client instance
     */
    private function push(
        int $correlationId,
        string $clientInstanceId,
        int $subscriptionId,
        string $metrics = '',
        int $compressionType = PushTelemetryRequest::COMPRESSION_NONE,
        bool $terminating = false
    ): PushTelemetryResponse {
        $stream = $this->connect();
        new PushTelemetryRequest(
            $clientInstanceId,
            $subscriptionId,
            $metrics,
            $terminating,
            $compressionType,
            self::CLIENT_ID,
            $correlationId
        )->writeTo($stream);

        return PushTelemetryResponse::unpack($stream);
    }
}

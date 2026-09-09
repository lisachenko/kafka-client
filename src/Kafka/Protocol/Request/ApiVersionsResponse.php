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

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\ApiVersionsResponseMetadata;

/**
 * The api keys and versions one broker serves
 *
 * <pre>
 *   ApiVersionsResponse => ErrorCode [ApiKey MinVersion MaxVersion]
 *     ErrorCode  => int16
 *     ApiKey     => int16
 *     MinVersion => int16
 *     MaxVersion => int16
 * </pre>
 *
 * The array is indexed by the api key, so that a caller can ask for one api directly
 * ({@see self::supports()}, {@see self::maxVersionOf()}); the broker sends the keys in ascending order and reports
 * every key it knows, including the broker-to-broker apis 4, 5 and 6 that no client ever sends.
 *
 * `ErrorCode` belongs to the whole answer and is 0 or **35** (UnsupportedVersion, `Errors.java` @ 0.10.2.2). The
 * second case is the answer to an ApiVersions request of a version the broker does not serve: the array is then
 * empty, and the frame is the version 0 layout no matter which version was asked for
 * (`KafkaApis.handleApiVersionsRequest` builds `ApiVersionsResponse.fromError(UNSUPPORTED_VERSION)` for it). The
 * connection survives it, which no other unknown key or version of a 0.10.2.2 broker does.
 *
 * A 0.10.2.2 broker answers with the 21 keys 0 to 20; a client must not assume that, though - the whole point of
 * the api is that the set is whatever the broker on the other side reports, and a later broker reports more.
 *
 * @see docs/protocol/0.11.0.md, section "ApiVersions API (key 18, v0)"
 */
class ApiVersionsResponse extends AbstractResponse
{
    /**
     * Error code of the whole request, 0 or 35 (UnsupportedVersion)
     */
    public int $errorCode = 0;

    /**
     * Version range of every api the broker serves, indexed by the api key
     *
     * @var array<int, ApiVersionsResponseMetadata>
     */
    public array $apiVersions = [];

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode'   => BinarySchema::TYPE_INT16,
            'apiVersions' => ['apiKey' => ApiVersionsResponseMetadata::class],
        ];
    }

    /**
     * Checks whether the broker serves the given version of an api
     *
     * @param int $apiKey  One of the constants of {@see \Protocol\Kafka\Protocol\ApiKeys}
     * @param int $version Version to look for
     */
    public function supports(int $apiKey, int $version): bool
    {
        return isset($this->apiVersions[$apiKey]) && $this->apiVersions[$apiKey]->supports($version);
    }

    /**
     * Returns the highest version of an api the broker serves, or null when it does not serve that api at all
     *
     * @param int $apiKey One of the constants of {@see \Protocol\Kafka\Protocol\ApiKeys}
     */
    public function maxVersionOf(int $apiKey): ?int
    {
        return $this->apiVersions[$apiKey]->maxVersion ?? null;
    }
}

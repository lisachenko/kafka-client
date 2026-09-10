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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * The range of versions the broker serves for one api key
 *
 * <pre>
 *   ApiVersionsResponseMetadata => ApiKey MinVersion MaxVersion
 *     ApiKey     => int16
 *     MinVersion => int16
 *     MaxVersion => int16
 * </pre>
 *
 * Both bounds are inclusive, and both of them matter: a 0.11.0.3 broker reports `MinVersion = 1` for
 * ControlledShutdown (key 7), because the version 0 of that request uses a header without a client id that the Java
 * client cannot build. The class is named after the `main` branch; the Kafka sources call the structure
 * `API_VERSIONS_V0` / `ApiVersionsResponse.ApiVersion` (`clients/.../requests/ApiVersionsResponse.java` @ 0.11.0.3).
 *
 * The entry itself has no version of its own: `API_VERSIONS_RESPONSE_V1` reuses the `API_VERSIONS_V0` schema of the
 * version 0 answer, so the same class reads both versions of the api.
 *
 * @see docs/protocol/1.1.md, section "ApiVersions API (key 18, v0 and v1)"
 */
class ApiVersionsResponseMetadata implements BinarySchemaInterface
{
    /**
     * Numerical code of the api, one of the constants of {@see \Protocol\Kafka\Protocol\ApiKeys}
     */
    public int $apiKey;

    /**
     * Lowest version of that api the broker still serves
     */
    public int $minVersion;

    /**
     * Highest version of that api the broker serves
     */
    public int $maxVersion;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'apiKey'     => BinarySchema::TYPE_INT16,
            'minVersion' => BinarySchema::TYPE_INT16,
            'maxVersion' => BinarySchema::TYPE_INT16,
        ];
    }

    /**
     * Checks whether the broker serves the given version of this api
     */
    public function supports(int $version): bool
    {
        return $version >= $this->minVersion && $version <= $this->maxVersion;
    }
}

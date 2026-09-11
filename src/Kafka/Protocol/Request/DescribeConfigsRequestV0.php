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

use Protocol\Kafka\Protocol\Data\DescribeConfigsRequestResource;

/**
 * DescribeConfigs, version 0: the request of Kafka 0.11, without the `include_synonyms` flag
 *
 * <pre>
 *   DescribeConfigs Request (Version: 0) => [resources]
 *     resources => resource_type resource_name [config_names]
 * </pre>
 *
 * The resource entries are the ones of version 1 (`DESCRIBE_CONFIGS_REQUEST_RESOURCE_V0` is the element schema of
 * both versions in `DescribeConfigsRequest.java` @ 1.1.1); only the trailing boolean is missing, and the answer to
 * this version carries an `is_default` boolean instead of the config source and the synonyms, see
 * {@see DescribeConfigsResponseV0}. A 1.1.1 broker still serves it, which is what
 * `tests/Integration/ConfigsApiTest.php` checks.
 *
 * @see docs/protocol/2.8.md, section "DescribeConfigs API (key 32, v0, v1 and v2)"
 */
final class DescribeConfigsRequestV0 extends DescribeConfigsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * @param list<DescribeConfigsRequestResource> $resources     Resources to describe
     * @param string                               $clientId      A user specified identifier for the client
     * @param int                                  $correlationId A user-supplied value the broker passes back
     */
    public function __construct(array $resources, string $clientId = '', int $correlationId = 0)
    {
        parent::__construct($resources, false, $clientId, $correlationId);
    }
}

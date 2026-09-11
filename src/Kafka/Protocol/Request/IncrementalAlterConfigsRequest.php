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

use Protocol\Kafka\Protocol\ApiKeys;
use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Data\IncrementalAlterConfigsRequestResource;

/**
 * IncrementalAlterConfigs, version 0: changes single options of a resource (ApiKey 44, Kafka 2.3, KIP-339)
 *
 * <pre>
 *   IncrementalAlterConfigs Request (Version: 0) => [resources] validate_only
 *     resources => resource_type resource_name [configs]
 *       resource_type => INT8
 *       resource_name => STRING
 *       configs       => name config_operation value
 *         name             => STRING
 *         config_operation => INT8
 *         value            => NULLABLE_STRING
 *     validate_only => BOOLEAN
 * </pre>
 *
 * The api KIP-339 added to repair the one real defect of {@see AlterConfigsRequest}: that request carries the WHOLE
 * configuration a resource should have afterwards, so a caller who wants to change one option has to read the
 * current configuration first and send it back - and loses whatever another client changed in between. This one
 * names an operation per option ({@see AlterConfigOp}) and leaves everything it does not mention alone.
 *
 * `IncrementalAlterConfigsRequest.json` @ 2.8.2 declares the versions `0-1`, of which 1 is the first flexible one
 * (Kafka 2.4); this line sends the **version 0**.
 *
 * Any broker of the cluster serves it - `KafkaApis.handleIncrementalAlterConfigsRequest` has no controller check -
 * except for a `broker:<id>` resource, which is the live configuration of that one broker and is only altered by
 * it, exactly as for the api below.
 *
 * **Kafka 2.4 added the version 1** (KIP-482), the same fields in the flexible encoding.
 * {@see IncrementalAlterConfigsRequestV0} is the frame Kafka 2.3 introduced.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0 and v1)"
 */
class IncrementalAlterConfigsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::INCREMENTAL_ALTER_CONFIGS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 1;

    /**
     * @inheritdoc
     */
    public const int FLEXIBLE_VERSION = 1;

    /**
     * Resources to alter, in the order of the request
     *
     * The array is a LIST and not a map, for the reason of {@see AlterConfigsRequest::$resources}: a resource is
     * identified by its type AND its name.
     *
     * @var list<IncrementalAlterConfigsRequestResource>
     */
    protected readonly array $resources;

    /**
     * @param list<IncrementalAlterConfigsRequestResource> $resources     Resources to alter
     * @param bool                                         $validateOnly  Validate without changing anything
     * @param string                                       $clientId      Identifier of the client
     * @param int                                          $correlationId A value the broker passes back
     */
    public function __construct(
        array $resources,
        /**
         * Whether the request should only be validated instead of altering anything
         */
        protected readonly bool $validateOnly = false,
        string $clientId = '',
        int $correlationId = 0
    ) {
        $this->resources = array_values($resources);

        parent::__construct(self::API_KEY, $clientId, $correlationId);
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'resources'    => [IncrementalAlterConfigsRequestResource::class],
            'validateOnly' => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}

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
use Protocol\Kafka\Protocol\Data\AlterConfigsRequestResource;

/**
 * AlterConfigs, version 0: replaces the configuration of a topic (ApiKey 33, Kafka 0.11, KIP-133)
 *
 * <pre>
 *   AlterConfigs Request (Version: 0) => [resources] validate_only
 *     resources => resource_type resource_name [config_entries]
 *       resource_type  => INT8
 *       resource_name  => STRING
 *       config_entries => config_name config_value
 *         config_name  => STRING
 *         config_value => NULLABLE_STRING
 *     validate_only => BOOLEAN
 * </pre>
 *
 * The counterpart of {@see DescribeConfigsRequest}, and the api that replaces `kafka-configs.sh --alter`. Two things
 * about it are easy to get wrong:
 *
 *  - **the entries are the whole configuration, not a patch.** `AdminManager.alterConfigs` @ 0.11.0.3 copies them
 *    into a `Properties` and calls `AdminUtils.changeTopicConfig`, which overwrites the ZooKeeper node of the topic;
 *    an option that was set before and is not in the request is therefore RESET to its default. A caller that wants
 *    to change one option reads the current configuration with DescribeConfigs first and sends it back with that
 *    one option changed.
 *  - **a 0.11 broker only alters topics.** Any other resource type - the broker resource of KIP-133 included - falls
 *    into the `case resourceType =>` branch of `AdminManager.alterConfigs` and is answered with the error code 42
 *    (InvalidRequest) and the message `AlterConfigs is only supported for topics, but resource type is BROKER`.
 *    Dynamic broker configuration is Kafka 1.1 (KIP-226).
 *
 * Any broker of the cluster serves the request - `KafkaApis.handleAlterConfigsRequest` has no controller check, and
 * the change travels through ZooKeeper - and `validateOnly` runs the validation without writing anything.
 *
 * @see docs/protocol/0.11.0.md, section "AlterConfigs API (key 33, v0)"
 */
class AlterConfigsRequest extends AbstractRequest
{
    /**
     * @inheritdoc
     */
    public const int API_KEY = ApiKeys::ALTER_CONFIGS;

    /**
     * @inheritdoc
     */
    public const int VERSION = 0;

    /**
     * Resources to alter, in the order of the request
     *
     * The array is a LIST and not a map, for the reason of {@see DescribeConfigsRequest::$resources}: a resource is
     * identified by its type AND its name.
     *
     * @var list<AlterConfigsRequestResource>
     */
    protected readonly array $resources;

    /**
     * @param list<AlterConfigsRequestResource> $resources     Resources to alter
     * @param bool                              $validateOnly  Validate the request without changing anything
     * @param string                            $clientId      A user specified identifier for the client
     * @param int                               $correlationId A user-supplied value the broker passes back
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
            'resources'    => [AlterConfigsRequestResource::class],
            'validateOnly' => BinarySchema::TYPE_BOOLEAN,
        ];
    }
}

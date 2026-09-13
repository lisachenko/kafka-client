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

/**
 * IncrementalAlterConfigs request of version 0 (Kafka 2.3), the body of version 1 before KIP-482
 *
 * <pre>
 *   IncrementalAlterConfigs Request (Version: 0) => [resources] validate_only
 * </pre>
 *
 * Kafka 2.4 made the version 1 the first flexible one of this api and added no field, so this class only lowers
 * the version constant: the version 0 is what a broker below Kafka 2.4 speaks.
 *
 * @see docs/protocol/2.8.md, section "IncrementalAlterConfigs API (key 44, v0 and v1)"
 */
final class IncrementalAlterConfigsRequestV0 extends IncrementalAlterConfigsRequest
{
    /**
     * @inheritdoc
     */
    public const int VERSION = 0;
}

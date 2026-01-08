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

/**
 * @author Alexander.Lisachenko
 * @date 14.07.2014
 */

namespace Protocol\Kafka\Protocol\Request;

use Protocol\Kafka\Protocol\ApiKeys;

/**
 * This request queries the broker about supported API versions for each command
 */
class ApiVersionsRequest extends AbstractRequest
{
    public function __construct($clientId = '', $correlationId = 0)
    {
        parent::__construct(ApiKeys::API_VERSIONS, $clientId, $correlationId);
    }
}

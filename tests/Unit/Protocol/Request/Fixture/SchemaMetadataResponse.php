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

namespace Protocol\Kafka\Tests\Unit\Protocol\Request\Fixture;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\Request\AbstractResponse;
use Protocol\Kafka\Tests\Fixture\BrokerRecord;

/**
 * Response declared with a scheme: the broker array of a Metadata response v0, indexed by node id
 */
final class SchemaMetadataResponse extends AbstractResponse
{
    /**
     * @var array<int, BrokerRecord>
     */
    public array $brokers = [];

    public int $errorCode = 0;

    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'brokers'   => ['nodeId' => BrokerRecord::class],
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}

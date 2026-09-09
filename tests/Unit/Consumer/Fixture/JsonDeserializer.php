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

namespace Protocol\Kafka\Tests\Unit\Consumer\Fixture;

use Protocol\Kafka\Common\Serialization\Deserializer;

/**
 * Turns the bytes of a record into the value the JSON in them describes, which is not a string
 */
final class JsonDeserializer implements Deserializer
{
    public function deserialize(string $topic, string $data): mixed
    {
        return json_decode($data, true, 512, JSON_THROW_ON_ERROR);
    }
}

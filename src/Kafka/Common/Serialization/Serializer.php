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

namespace Protocol\Kafka\Common\Serialization;

/**
 * Converts an application-level value into the raw bytes stored in a record's key or value.
 */
interface Serializer
{
    public function serialize(string $topic, mixed $data): string;
}

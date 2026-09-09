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
 * Passes strings through unchanged; the default when no deserializer is configured.
 */
final class StringDeserializer implements Deserializer
{
    public function deserialize(string $topic, string $data): mixed
    {
        return $data;
    }
}

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
 * Copyright
 *
 * @author Alexander.Lisachenko
 * @date   29.06.2018
 */

namespace Protocol\Kafka\Protocol;

interface BinarySchemaInterface
{
    /**
     * Returns definition of binary packet for the class or object
     */
    public static function getScheme(): array;
}

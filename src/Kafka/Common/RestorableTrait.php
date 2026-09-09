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
 * @date 09.08.2016
 */

namespace Protocol\Kafka\Common;

/**
 * Simple implementation for classes that can be restored after var_export
 */
trait RestorableTrait
{
    /**
     * @inheritDoc
     */
    public static function __set_state(array $cachedData)
    {
        $self = new static();
        foreach ($cachedData as $key => $value) {
            $self->$key = $value;
        }

        return $self;
    }
}

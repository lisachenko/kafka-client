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

/**
 * Smallest possible response with a body: the response header followed by a single error code
 */
final class ErrorCodeResponse extends AbstractResponse
{
    public int $errorCode = 0;

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        $header = parent::getScheme();

        return $header + [
            'errorCode' => BinarySchema::TYPE_INT16,
        ];
    }
}

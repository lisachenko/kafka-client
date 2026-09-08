<?php

/*
 * This file is part of the lisachenko/kafka-client package.
 *
 * (c) Alexander Lisachenko <lisachenko.it@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare (strict_types=1);

namespace Protocol\Kafka\Common\Errors;

use RuntimeException;

/**
 * Class InvalidConfigurationException
 */
class InvalidConfigurationException extends RuntimeException implements ClientExceptionInterface {}

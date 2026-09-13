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

namespace Protocol\Kafka\Protocol\Data;

use Protocol\Kafka\Protocol\BinarySchema;
use Protocol\Kafka\Protocol\BinarySchemaInterface;

/**
 * One credential an AlterUserScramCredentials request removes (key 51, Kafka 2.7, KIP-554)
 *
 * <pre>
 *   ScramCredentialDeletion => Name Mechanism
 *     Name      => COMPACT_STRING
 *     Mechanism => INT8
 * </pre>
 *
 * A deletion names the user **and** the mechanism: a user may have a SCRAM-SHA-256 and a SCRAM-SHA-512 credential
 * at the same time, and removing one leaves the other alone.
 *
 * @see docs/protocol/2.8.md, section "AlterUserScramCredentials API (key 51, v0)"
 */
class ScramCredentialDeletion implements BinarySchemaInterface
{
    /**
     * Name of the user whose credential is removed
     */
    public string $name;

    /**
     * Mechanism of the credential, one of the MECHANISM_* constants of {@see ScramCredentialInfo}
     */
    public int $mechanism;

    public function __construct(string $name = '', int $mechanism = 0)
    {
        $this->name      = $name;
        $this->mechanism = $mechanism;
    }

    /**
     * @inheritdoc
     */
    public static function getScheme(): array
    {
        return [
            'name'      => BinarySchema::TYPE_STRING,
            'mechanism' => BinarySchema::TYPE_INT8,
        ];
    }
}

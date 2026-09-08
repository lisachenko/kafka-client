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

namespace Protocol\Kafka\Tests\Compliance;

use RuntimeException;

/**
 * Reader of the wire vectors that live next to the protocol document.
 *
 * The vectors in `docs/protocol/vectors/*.json` are the machine-readable half of `docs/protocol/0.9.0.md`: the
 * document shows every one of them as an annotated hex dump, the JSON files carry the same bytes together with the
 * values that the message decodes into. Both halves are kept in step by {@see DocumentationSyncTest}.
 */
final class VectorFile
{
    /**
     * Directory that holds the vector files and the protocol document
     */
    public const string VECTORS_DIRECTORY = __DIR__ . '/../../docs/protocol/vectors';

    /**
     * Location of the protocol document that the vectors are documented in
     */
    public const string PROTOCOL_DOCUMENT = __DIR__ . '/../../docs/protocol/0.9.0.md';

    /**
     * Returns the vectors of one api as a PHPUnit data provider, indexed by the vector id
     *
     * @param string $api Base name of the vector file, e.g. `metadata`
     *
     * @return iterable<string, array{0: array<string, mixed>}>
     */
    public static function provide(string $api): iterable
    {
        $file = self::read($api);
        foreach ($file['vectors'] as $vector) {
            yield $vector['id'] => [$vector + ['api' => $api, 'apiKey' => $file['apiKey']]];
        }
    }

    /**
     * Returns the whole content of one vector file
     *
     * @return array{api: string, apiKey: int, section: string, vectors: list<array<string, mixed>>}
     */
    public static function read(string $api): array
    {
        $fileName = self::VECTORS_DIRECTORY . "/{$api}.json";
        $content  = file_get_contents($fileName);
        if ($content === false) {
            throw new RuntimeException("Vector file {$fileName} can not be read");
        }

        return json_decode($content, true, flags: JSON_THROW_ON_ERROR);
    }

    /**
     * Returns the base names of every vector file, in alphabetical order
     *
     * @return list<string>
     */
    public static function names(): array
    {
        $files = glob(self::VECTORS_DIRECTORY . '/*.json');
        if ($files === false) {
            throw new RuntimeException('Vector directory ' . self::VECTORS_DIRECTORY . ' can not be listed');
        }
        sort($files);

        return array_map(static fn(string $file): string => basename($file, '.json'), $files);
    }

    /**
     * Returns every vector of every file, indexed by its id
     *
     * @return array<string, array<string, mixed>>
     */
    public static function all(): array
    {
        $vectors = [];
        foreach (self::names() as $api) {
            $file = self::read($api);
            foreach ($file['vectors'] as $vector) {
                $vectors[$vector['id']] = $vector + ['api' => $api, 'apiKey' => $file['apiKey']];
            }
        }

        return $vectors;
    }
}

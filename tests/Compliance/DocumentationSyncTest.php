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

use PHPUnit\Framework\TestCase;

/**
 * Keeps `docs/protocol/0.8.2.md` and `docs/protocol/vectors/*.json` from drifting apart.
 *
 * Every vector is documented twice: as an annotated hex dump in the protocol document, introduced by an
 * `<!-- vector: <id> -->` marker, and as raw hex plus decoded fields in the vector file that {@see ProtocolVectorTest}
 * replays. This suite checks that both halves describe the same bytes, that neither half has an entry the other one
 * does not know, and that every api section named by a vector file exists in the document.
 */
final class DocumentationSyncTest extends TestCase
{
    /**
     * Matches an annotated dump: the marker comment and the fenced block that follows it
     */
    private const string VECTOR_PATTERN = '/<!-- vector: (?P<id>[a-z0-9.\-]+) -->\R```\R(?P<dump>.*?)\R```/s';

    public function testDocumentAndVectorFilesDescribeTheSameVectors(): void
    {
        $stored     = array_keys(VectorFile::all());
        $documented = array_keys(self::documentedVectors());
        sort($stored);
        sort($documented);

        self::assertSame(
            $stored,
            $documented,
            'The protocol document and the vector files do not list the same vectors'
        );
    }

    public function testEveryDocumentedDumpHoldsTheBytesOfItsVector(): void
    {
        $documented = self::documentedVectors();
        foreach (VectorFile::all() as $id => $vector) {
            self::assertArrayHasKey($id, $documented, "The vector {$id} is not documented in the protocol document");
            self::assertSame(
                $vector['hex'],
                $documented[$id],
                "The annotated dump of {$id} in the protocol document holds other bytes than the vector file"
            );
        }
    }

    public function testEveryVectorFileNamesAnExistingSectionOfTheDocument(): void
    {
        $document = self::document();
        foreach (VectorFile::names() as $api) {
            $section = VectorFile::read($api)['section'];
            self::assertStringContainsString(
                "## {$section}",
                $document,
                "The vector file {$api}.json refers to a section that the protocol document does not have"
            );
        }
    }

    public function testEveryDocumentedVectorIdIsUnique(): void
    {
        preg_match_all(self::VECTOR_PATTERN, self::document(), $matches);

        self::assertSame(
            array_values(array_unique($matches['id'])),
            $matches['id'],
            'The protocol document documents the same vector id twice'
        );
    }

    /**
     * Returns the bytes of every annotated dump of the protocol document, indexed by the vector id
     *
     * @return array<string, string>
     */
    private static function documentedVectors(): array
    {
        preg_match_all(self::VECTOR_PATTERN, self::document(), $matches, PREG_SET_ORDER);

        $vectors = [];
        foreach ($matches as $match) {
            $vectors[$match['id']] = self::hexOf($match['dump']);
        }

        return $vectors;
    }

    /**
     * Extracts the bytes of an annotated dump: everything that is not a `#` comment is hex
     */
    private static function hexOf(string $dump): string
    {
        $withoutComments = preg_replace('/#[^\n]*/', '', $dump) ?? '';
        $hex             = strtolower((string) preg_replace('/\s+/', '', $withoutComments));

        self::assertMatchesRegularExpression('/^([0-9a-f]{2})+$/', $hex, 'A documented dump is not valid hex');

        return $hex;
    }

    /**
     * Returns the content of the protocol document
     */
    private static function document(): string
    {
        $content = file_get_contents(VectorFile::PROTOCOL_DOCUMENT);
        self::assertIsString($content, 'The protocol document can not be read');

        return $content;
    }
}

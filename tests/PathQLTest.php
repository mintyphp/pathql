<?php

namespace MintyPHP\PathQL\Tests;

use MintyPHP\PathQL\PathQL;

class PathQLTest extends PathQLTestCase
{
    /** @var PathQL|null */
    protected $pathQL;

    public function setUp(): void
    {
        parent::setUp();
        if (static::$db !== null && static::$cache !== null) {
            $this->pathQL = new PathQL(static::$db, static::$cache);
        }
    }

    /**
     * @param array<int|string,mixed> $params
     * @param array<string,string> $pathHints
     * @dataProvider pathQueryDataProvider
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('pathQueryDataProvider')]
    public function testExecute(string $sql, array $params, array $pathHints, string $expected): void
    {
        $this->assertNotNull($this->pathQL);
        $result = $this->pathQL->execute($sql, $params, $pathHints);
        $this->assertSame($expected, json_encode($result));
    }

    /**
     * @return array<string,array{0:string,1:array<int|string,mixed>,2:array<string,string>,3:string}>
     */
    public static function pathQueryDataProvider(): array
    {
        return [
            // --- No-path flat array (fast path, no "$" aliases) ---
            'single record no path' => [
                'select id, content from posts where id=?',
                [1],
                [],
                '[{"id":1,"content":"blog started"}]',
            ],
            'two records no path' => [
                'select id from posts where id<=? order by id',
                [2],
                [],
                '[{"id":1},{"id":2}]',
            ],
            'count posts grouped no path' => [
                'select categories.name, count(posts.id) as post_count from posts, categories where posts.category_id = categories.id group by categories.name order by categories.name',
                [],
                [],
                '[{"name":"announcement","post_count":11},{"name":"article","post_count":1}]',
            ],

            // --- Automatic Path Inference (from JOINs and FKs) ---
            'posts with comments properly nested' => [
                'select p.id, c.id from posts p left join comments c on c.post_id = p.id where p.id<=2 order by p.id, c.id',
                [],
                [],
                '[{"id":1,"c":[{"id":1},{"id":2}]},{"id":2,"c":[{"id":3},{"id":4},{"id":5},{"id":6}]}]',
            ],
            'comments with post properly nested' => [
                'select c.id, p.id from comments c join posts p on c.post_id = p.id where p.id<=2 order by c.id, p.id',
                [],
                [],
                '[{"id":1,"p":{"id":1}},{"id":2,"p":{"id":1}},{"id":3,"p":{"id":2}},{"id":4,"p":{"id":2}},{"id":5,"p":{"id":2}},{"id":6,"p":{"id":2}}]',
            ],

            // --- Automatic Path Inference ---
            'simple query with alias no joins' => [
                'select p.id, p.content from posts p where p.id=1',
                [],
                [],
                '[{"id":1,"content":"blog started"}]',
            ],
            'posts with comments one-to-many with content' => [
                'select p.id, p.content, c.id, c.message from posts p left join comments c on c.post_id = p.id where p.id=1 order by c.id',
                [],
                [],
                '[{"id":1,"content":"blog started","c":[{"id":1,"message":"great!"},{"id":2,"message":"nice!"}]}]',
            ],
            'multiple posts with comments with message' => [
                'select p.id, c.id, c.message from posts p left join comments c on c.post_id = p.id where p.id<=2 order by p.id, c.id',
                [],
                [],
                '[{"id":1,"c":[{"id":1,"message":"great!"},{"id":2,"message":"nice!"}]},{"id":2,"c":[{"id":3,"message":"interesting"},{"id":4,"message":"cool"},{"id":5,"message":"wow"},{"id":6,"message":"amazing"}]}]',
            ],
            'posts with category many-to-one' => [
                'select p.id, p.content, cat.id, cat.name from posts p left join categories cat on p.category_id = cat.id where p.id=1',
                [],
                [],
                '[{"id":1,"content":"blog started","cat":{"id":1,"name":"announcement"}}]',
            ],
        ];
    }

    /**
     * Test execute with explicit paths parameter
     */
    public function testExecuteWithPathsParameter(): void
    {
        $this->assertNotNull($this->pathQL);
        // Single object result with paths parameter
        $result = $this->pathQL->execute(
            'SELECT COUNT(*) as posts FROM posts p',
            [],
            ['p' => '$.statistics']
        );
        $this->assertEquals(['statistics' => ['posts' => 12]], $result);
    }

    public function testExecuteWithPathsParameterNestedArrays(): void
    {
        $this->assertNotNull($this->pathQL);
        // Posts with comments using paths parameter
        $result = $this->pathQL->execute(
            'SELECT p.id, c.id, c.message 
             FROM posts p 
             LEFT JOIN comments c ON c.post_id = p.id 
             WHERE p.id = ?
             ORDER BY c.id',
            [1],
            [
                'p' => '$',
                'c' => '$.comments[]'
            ]
        );

        $expected = [
            'id' => 1,
            'comments' => [
                ['id' => 1, 'message' => 'great!'],
                ['id' => 2, 'message' => 'nice!']
            ]
        ];

        $this->assertEquals($expected, $result);
    }

    public function testExecuteWithPathsParameterCustomPaths(): void
    {
        $this->assertNotNull($this->pathQL);
        // Test paths parameter with custom structure
        $result = $this->pathQL->execute(
            'SELECT p.id, c.id 
             FROM posts p
             LEFT JOIN comments c ON c.post_id = p.id
             WHERE p.id = ?
             ORDER BY c.id',
            [1],
            [
                'p' => '$',
                'c' => '$.comments[]'
            ]
        );

        $expected = [
            'id' => 1,
            'comments' => [
                ['id' => 1],
                ['id' => 2]
            ]
        ];

        $this->assertEquals($expected, $result);
    }

    public function testExecuteWithPathsParameterMultipleLevels(): void
    {
        $this->assertNotNull($this->pathQL);
        // Test deeply nested paths
        $result = $this->pathQL->execute(
            'SELECT p.id, c.id, c.message 
             FROM posts p 
             LEFT JOIN comments c ON c.post_id = p.id 
             WHERE p.id = ? 
             ORDER BY c.id',
            [2],
            [
                'p' => '$.data',
                'c' => '$.data.comments[]'
            ]
        );

        $this->assertArrayHasKey('data', $result);
        $this->assertIsArray($result['data']);
        $this->assertArrayHasKey('id', $result['data']);
        $this->assertEquals(2, $result['data']['id']);
        $this->assertArrayHasKey('comments', $result['data']);
        $this->assertIsArray($result['data']['comments']);
        $this->assertCount(4, $result['data']['comments']);
    }

    public function testExecuteWithPathsParameterArrayRoot(): void
    {
        $this->assertNotNull($this->pathQL);
        // Test array at root level
        $result = $this->pathQL->execute(
            'SELECT p.id, p.content FROM posts p WHERE p.id <= ? ORDER BY p.id',
            [2],
            ['p' => '$[]']
        );

        $this->assertCount(2, $result);
        $this->assertIsArray($result[0]);
        $this->assertEquals(1, $result[0]['id']);
        $this->assertIsArray($result[1]);
        $this->assertEquals(2, $result[1]['id']);
        $this->assertIsArray($result[0]);
        $this->assertEquals('blog started', $result[0]['content']);
    }

    public function testExecuteWithEmptyPathsParameter(): void
    {
        $this->assertNotNull($this->pathQL);
        // Empty paths parameter should work like normal query
        $result = $this->pathQL->execute(
            'SELECT id, content FROM posts WHERE id = ?',
            [1],
            []
        );

        $this->assertEquals([['id' => 1, 'content' => 'blog started']], $result);
    }

    public function testExecuteWithEmptyResult(): void
    {
        $this->assertNotNull($this->pathQL);
        $result = $this->pathQL->execute(
            'SELECT id FROM posts WHERE id = ?',
            [999999],
            []
        );

        $this->assertEquals([], $result);
    }
}

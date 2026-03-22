<?php

namespace MintyPHP\PathQL\Tests;

use MintyPHP\PathQL\Schema;

class SchemaTest extends PathQLTestCase
{
    public function testGetForeignKeysFromDatabase(): void
    {
        $this->assertNotNull(static::$db);
        $this->assertNotNull(static::$cache);

        $schema = new Schema(static::$cache);
        $fks = $schema->getForeignKeys(static::$db);

        $this->assertNotEmpty($fks);

        // Should have foreign keys from test database
        $commentsFk = null;
        foreach ($fks as $fk) {
            if ($fk['from_table'] === 'comments' && $fk['from_column'] === 'post_id') {
                $commentsFk = $fk;
                break;
            }
        }

        $this->assertNotNull($commentsFk);
        $this->assertEquals('posts', $commentsFk['to_table']);
        $this->assertEquals('id', $commentsFk['to_column']);
    }

    public function testClearCache(): void
    {
        $this->assertNotNull(static::$db);
        $this->assertNotNull(static::$cache);

        $schema = new Schema(static::$cache);

        // Get foreign keys to populate cache
        $fks1 = $schema->getForeignKeys(static::$db);
        $this->assertNotEmpty($fks1);

        // Clear cache
        $schema->clearCache(static::$db);

        // Get again - should work even after cache clear
        $fks2 = $schema->getForeignKeys(static::$db);
        $this->assertEquals($fks1, $fks2);
    }
}

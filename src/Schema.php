<?php

namespace MintyPHP\PathQL;

use MintyPHP\Core\DB;
use MintyPHP\Core\Cache;

class Schema
{
    private Cache $cache;
    private string $cachePrefix = 'pathql_schema_';
    private int $cacheTtl = 3600; // 1 hour cache TTL

    /**
     * Constructs a Schema instance.
     * 
     * @param Cache $cache Cache instance for storing schema metadata
     */
    public function __construct(Cache $cache)
    {
        $this->cache = $cache;
    }

    /**
     * Get foreign keys for the database, using cache when available.
     * 
     * @param DB $db Database connection
     * @return array<int,array<string,string>>
     */
    public function getForeignKeys(DB $db): array
    {
        $driver = $this->getDriver($db);
        $cacheKey = $this->cachePrefix . $driver;

        // Try to get from cache first
        $cached = $this->cache->get($cacheKey);
        if ($cached !== false && is_array($cached)) {
            return $cached;
        }

        // Not in cache, query the database
        $fks = $this->getForeignKeysFromDatabase($db, $driver);

        // Store in cache
        $this->cache->set($cacheKey, $fks, $this->cacheTtl);

        return $fks;
    }

    /**
     * Clear the cached schema information.
     * 
     * @param DB $db Database connection
     */
    public function clearCache(DB $db): void
    {
        $driver = $this->getDriver($db);
        $cacheKey = $this->cachePrefix . $driver;
        $this->cache->delete($cacheKey);
    }

    /**
     * Get the database driver name from DB connection.
     * 
     * @param DB $db Database connection
     * @return string Driver name (mysql, pgsql, sqlsrv, etc.)
     */
    private function getDriver(DB $db): string
    {
        // Execute a simple query to get connection info
        // For MySQL, we can check the version
        try {
            $result = $db->query("SELECT VERSION()");
            if (is_array($result) && count($result) > 0) {
                // This is likely MySQL or MariaDB
                return 'mysql';
            }
        } catch (\Exception $e) {
            // Try PostgreSQL syntax
            try {
                $result = $db->query("SELECT version()");
                if (is_array($result) && count($result) > 0) {
                    return 'pgsql';
                }
            } catch (\Exception $e2) {
                // Default to mysql
                return 'mysql';
            }
        }
        return 'mysql';
    }

    /**
     * Get foreign keys by querying the database.
     * 
     * @param DB $db Database connection
     * @param string $driver Database driver (mysql, pgsql, sqlsrv)
     * @return array<int,array<string,string>>
     */
    private function getForeignKeysFromDatabase(DB $db, string $driver): array
    {
        $fks = [];

        switch ($driver) {
            case 'mysql':
                $sql = "
                    SELECT 
                        TABLE_NAME AS from_table, 
                        COLUMN_NAME AS from_column, 
                        REFERENCED_TABLE_NAME AS to_table, 
                        REFERENCED_COLUMN_NAME AS to_column 
                    FROM information_schema.key_column_usage 
                    WHERE REFERENCED_TABLE_NAME IS NOT NULL 
                      AND TABLE_SCHEMA = DATABASE()
                ";
                $result = $db->query($sql);
                if (is_array($result)) {
                    foreach ($result as $row) {
                        // Get the first table's data
                        foreach ($row as $tableName => $data) {
                            if (is_array($data)) {
                                $fks[] = [
                                    'from_table' => $data['from_table'] ?? '',
                                    'from_column' => $data['from_column'] ?? '',
                                    'to_table' => $data['to_table'] ?? '',
                                    'to_column' => $data['to_column'] ?? '',
                                ];
                            }
                            break;
                        }
                    }
                }
                break;

            case 'pgsql':
                $sql = "
                    SELECT
                        tc.table_name AS from_table,
                        kcu.column_name AS from_column,
                        ccu.table_name AS to_table,
                        ccu.column_name AS to_column
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_schema = current_schema()
                ";
                $result = $db->query($sql);
                if (is_array($result)) {
                    foreach ($result as $row) {
                        foreach ($row as $tableName => $data) {
                            if (is_array($data)) {
                                $fks[] = [
                                    'from_table' => $data['from_table'] ?? '',
                                    'from_column' => $data['from_column'] ?? '',
                                    'to_table' => $data['to_table'] ?? '',
                                    'to_column' => $data['to_column'] ?? '',
                                ];
                            }
                            break;
                        }
                    }
                }
                break;

            case 'sqlsrv':
                $sql = "
                    SELECT 
                        tp.name AS from_table,
                        cp.name AS from_column,
                        tr.name AS to_table,
                        cr.name AS to_column
                    FROM sys.foreign_keys fk
                    INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
                    INNER JOIN sys.tables tr ON fk.referenced_object_id = tr.object_id
                    INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                    INNER JOIN sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
                    INNER JOIN sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
                ";
                $result = $db->query($sql);
                if (is_array($result)) {
                    foreach ($result as $row) {
                        foreach ($row as $tableName => $data) {
                            if (is_array($data)) {
                                $fks[] = [
                                    'from_table' => $data['from_table'] ?? '',
                                    'from_column' => $data['from_column'] ?? '',
                                    'to_table' => $data['to_table'] ?? '',
                                    'to_column' => $data['to_column'] ?? '',
                                ];
                            }
                            break;
                        }
                    }
                }
                break;
        }

        return $fks;
    }
}

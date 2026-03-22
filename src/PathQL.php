<?php

namespace MintyPHP\PathQL;

use MintyPHP\Core\DB;
use MintyPHP\Core\Cache;
use MintyPHP\PathQL\Error\PathError;

/**
 * PathQL query processor for MintyPHP ORM
 * 
 * Handles automatic path inference for hierarchical query results.
 */
class PathQL
{
    private Schema $schema;
    private QueryAnalyzer $queryAnalyzer;
    private PathInference $pathInference;

    /**
     * Constructor for the PathQL class.
     * 
     * @param DB $db Database instance for executing queries.
     * @param Cache $cache Cache instance for storing schema metadata.
     */
    public function __construct(private DB $db, Cache $cache)
    {
        $this->schema = new Schema($cache);
        $this->queryAnalyzer = new QueryAnalyzer();
        $this->pathInference = new PathInference($this->schema);
    }

    /**
     * Execute a query with automatic path inference for hierarchical results.
     * 
     * Automatically infers the structure of the result set based on SQL JOINs and
     * foreign key relationships, returning nested arrays/objects instead of flat rows.
     * 
     * @param string $sql The SQL query to execute
     * @param array<int|string,mixed> $params Parameters for prepared statement
     * @param array<string,string> $pathHints Optional path mappings for table aliases
     *                     Format: ['alias' => '$.path', 'other' => '$.parent.child[]']
     * @return array<int|string,mixed> Hierarchical result structure based on inferred paths
     * @throws \RuntimeException If query execution fails
     */
    public function execute(string $sql, array $params = [], array $pathHints = []): array
    {
        // Execute the query
        $result = $this->db->query($sql, ...$params);

        if (!is_array($result) || count($result) === 0) {
            return [];
        }

        // Get column information from first row
        $firstRow = $result[0];
        $columns = [];
        foreach ($firstRow as $tableName => $tableData) {
            if (is_array($tableData)) {
                foreach (array_keys($tableData) as $columnName) {
                    $columns[] = $tableName . '.' . $columnName;
                }
            }
        }

        // Analyze query and infer paths
        $this->queryAnalyzer->analyze($sql);

        // Merge provided path hints
        if (!empty($pathHints)) {
            $this->queryAnalyzer->pathHints = array_merge($this->queryAnalyzer->pathHints, $pathHints);
        }

        $inferColumns = $this->queryAnalyzer->parseSelectColumns($sql);
        if (empty($inferColumns) || count($inferColumns) !== count($columns)) {
            $inferColumns = $columns; // Fallback to detected columns if parse fails
        }

        $paths = $this->pathInference->inferPaths($this->queryAnalyzer, $inferColumns, $this->db);

        // Decide which branch to take based on the paths
        $hasArrayMarkers = false;
        $hasObjectPrefix = false;
        foreach ($paths as $path) {
            if (strpos($path, '[]') !== false) {
                $hasArrayMarkers = true;
            }
            if (strpos($path, '$.') === 0) {
                $hasObjectPrefix = true;
            }
        }

        $isObjectResult = $hasObjectPrefix && !$hasArrayMarkers;

        $records = $this->getAllRecords($result, $paths);

        if ($isObjectResult && count($records) > 0) {
            return $this->buildObject($records[0]);
        }

        if (!$hasArrayMarkers) {
            return $this->buildFlatArray($records);
        }

        $groups = $this->groupBySeparator($records, '[]');
        $hashes = $this->addHashes($groups);
        $tree   = $this->combineIntoTree($hashes, '.');
        $result = $this->removeHashes($tree, '$');
        return $result;
    }

    /**
     * @param array<int, array<string, array<string, mixed>>> $result
     * @param array<int,string> $paths
     * @return array<int, array<int|string,mixed>>
     */
    private function getAllRecords(array $result, array $paths): array
    {
        $records = [];
        foreach ($result as $row) {
            $record = [];
            $i = 0;
            foreach ($row as $tableName => $tableData) {
                if (is_array($tableData)) {
                    foreach ($tableData as $columnName => $value) {
                        if (!isset($paths[$i])) {
                            $i++;
                            continue;
                        }
                        $path = $paths[$i];
                        // Strip leading "$" if present, else keep the dot (e.g. .id)
                        $record[($path[0] === '$' ? substr($path, 1) : $path)] = $value;
                        $i++;
                    }
                }
            }
            $records[] = $record;
        }
        return $records;
    }

    /**
     * @param array<int|string,mixed> $record
     * @return array<string,mixed>
     */
    private function buildObject(array $record): array
    {
        $result = [];
        foreach ($record as $key => $value) {
            $key = ltrim((string)$key, '.');
            $parts = explode('.', $key);
            $current = &$result;
            foreach ($parts as $i => $part) {
                if ($i === count($parts) - 1) {
                    $current[$part] = $value;
                } else {
                    if (!isset($current[$part]) || !is_array($current[$part])) {
                        $current[$part] = [];
                    }
                    $current = &$current[$part];
                }
            }
            unset($current);
        }
        return $result;
    }

    /**
     * @param array<int, array<int|string,mixed>> $records
     * @return array<int, array<string,mixed>>
     */
    private function buildFlatArray(array $records): array
    {
        $results = [];
        foreach ($records as $record) {
            $obj = [];
            foreach ($record as $key => $value) {
                $obj[ltrim((string)$key, '.')] = $value;
            }
            $results[] = $obj;
        }
        return $results;
    }

    /**
     * @param array<int, array<int|string,mixed>> $records
     * @return array<int, array<string,array<string,mixed>>>
     */
    private function groupBySeparator(array $records, string $separator): array
    {
        $results = [];
        foreach ($records as $record) {
            $result = [];
            foreach ($record as $name => $value) {
                $parts   = $separator !== '' ? explode($separator, (string)$name) : [(string)$name];
                $newName = array_pop($parts);
                $path    = implode($separator, $parts);
                if ($parts) {
                    $path .= $separator;
                }
                if (!isset($result[$path])) {
                    $result[$path] = [];
                }
                $result[$path][$newName] = $value;
            }
            $results[] = $result;
        }
        return $results;
    }

    /**
     * @param array<int, array<string,array<string,mixed>>> $records
     * @return array<int, array<int|string,mixed>>
     */
    private function addHashes(array $records): array
    {
        $results = [];
        foreach ($records as $record) {
            $mapping = [];
            foreach ($record as $key => $part) {
                if (substr($key, -2) != '[]') {
                    continue;
                }
                $jsonEncoded = json_encode($part);
                if ($jsonEncoded === false) {
                    $jsonEncoded = '';
                }
                $hash             = md5($jsonEncoded);
                $mapping[$key]    = substr($key, 0, -2) . '.!' . $hash . '!';
            }
            uksort($mapping, function ($a, $b) {
                return strlen($b) - strlen($a);
            });
            $keys    = array_keys($record);
            $values  = array_values($record);
            $newKeys = str_replace(array_keys($mapping), array_values($mapping), array_map('strval', $keys));
            $results[] = array_combine($newKeys, $values);
        }
        return $results;
    }

    /**
     * @param array<int, array<int|string,mixed>> $records
     * @return array<mixed>
     */
    private function combineIntoTree(array $records, string $separator): array
    {
        /** @var array<string,mixed> $results */
        $results = [];
        foreach ($records as $record) {
            foreach ($record as $name => $value) {
                if (!is_array($value)) {
                    continue;
                }
                foreach ($value as $key => $v) {
                    $path    = $separator !== '' ? explode($separator, (string)$name . (string)$key) : [(string)$name . (string)$key];
                    $newName = array_pop($path);
                    $current = &$results;
                    foreach ($path as $p) {
                        if (!isset($current[$p])) {
                            $current[$p] = [];
                        }
                        if (is_array($current[$p])) {
                            $current = &$current[$p];
                        }
                    }
                    $current[$newName] = $v;
                }
            }
        }
        return isset($results['']) && is_array($results['']) ? $results[''] : [];
    }

    /**
     * @param array<mixed> $tree
     * @return array<int|string,mixed>
     */
    private function removeHashes(array $tree, string $path): array
    {
        $values  = [];
        $trees   = [];
        $results = [];
        foreach ($tree as $key => $value) {
            if (is_array($tree[$key])) {
                if (substr((string)$key, 0, 1) == '!' && substr((string)$key, -1, 1) == '!') {
                    $results[] = $this->removeHashes($tree[$key], $path . '[]');
                } else {
                    $trees[$key] = $this->removeHashes($tree[$key], $path . '.' . $key);
                }
            } else {
                $values[$key] = $value;
            }
        }
        if (count($results)) {
            $hidden = array_merge(array_keys($values), array_keys($trees));
            if (count($hidden) > 0) {
                throw new PathError('The path "' . $path . '.' . $hidden[0] . '" is hidden by the path "' . $path . '[]"');
            }
            return $results;
        }
        return array_merge($values, $trees);
    }
}

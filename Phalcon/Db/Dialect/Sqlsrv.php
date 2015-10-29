<?php

namespace Phalcon\Db\Dialect;

use Phalcon\Db\Column;
use Phalcon\Db\Exception;

/**
 * Phalcon\Db\Dialect\Sqlsrv
 * Generates database specific SQL for the MsSQL RDBMS.
 */
class Sqlsrv extends \Phalcon\Db\Dialect
{
    /**
     * Escape Char.
     *
     * @var string
     */
    protected $_escapeChar = '"';

    /**
     * Generates the SQL for LIMIT clause
     * <code>
     * $sql = $dialect->limit('SELECT * FROM robots', 10);
     * echo $sql; // SELECT * FROM robots LIMIT 10
     * $sql = $dialect->limit('SELECTFROM robots', [10, 50]);
     * echo $sql; // SELECT * FROM robots OFFSET 10 ROWS FETCH NEXT 50 ROWS ONLY
     * </code>.
     *
     * @param string $sqlQuery
     * @param mixed  $number
     *
     * @return string
     */
    public function limit($sqlQuery, $number)
    {
        $offset = 0;
        if (is_array($number)) {
            if (isset($number[1]) && strlen($number[1])) {
                $offset = $number[1];
            }

            $number = $number[0];
        }

        if (strpos($sqlQuery, 'ORDER BY') === false) {
            $sqlQuery .= ' ORDER BY 1';
        }

        return $sqlQuery." OFFSET {$offset} ROWS FETCH NEXT {$number} ROWS ONLY";
    }

    /**
     * Returns a SQL modified with a FOR UPDATE clause.
     *
     * <code>
     * $sql = $dialect->forUpdate('SELECT * FROM robots');
     * echo $sql; // SELECT * FROM robots WITH (UPDLOCK)
     * </code>
     */
    public function forUpdate($sqlQuery)
    {
        return $sqlQuery.' WITH (UPDLOCK) ';
    }

    /**
     * Returns a SQL modified with a LOCK IN SHARE MODE clause.
     *
     * <code>
     * $sql = $dialect->sharedLock('SELECT * FROM robots');
     * echo $sql; // SELECT * FROM robots WITH (NOLOCK)
     * </code>
     */
    public function sharedLock($sqlQuery)
    {
        return $sqlQuery.' WITH (NOLOCK) ';
    }

    /**
     * Gets the column name in MsSQL.
     *
     * @param mixed $column
     *
     * @return string
     */
    public function getColumnDefinition(\Phalcon\Db\ColumnInterface $column)
    {
        $columnSql = '';
        $type = $column->getType();
        if (is_string($type)) {
            $columnSql .= $type;
            $type = $column->getTypeReference();
        }

        switch ($type) {
            case Column::TYPE_INTEGER:
                if (empty($columnSql)) {
                    $columnSql .= 'INT';
                }

//                $columnSql .= '('.$column->getSize().')';
//                if ($column->isUnsigned()) {
//                    $columnSql .= ' UNSIGNED';
//                }
                break;

            case Column::TYPE_DATE:
                if (empty($columnSql)) {
                    $columnSql .= 'DATE';
                }
                break;

            case Column::TYPE_VARCHAR:
                if (empty($columnSql)) {
                    $columnSql .= 'NVARCHAR';
                }
                $columnSql .= '('.$column->getSize().')';
                break;

            case Column::TYPE_DECIMAL:
                if (empty($columnSql)) {
                    $columnSql .= 'DECIMAL';
                }
                $columnSql .= '('.$column->getSize().','.$column->getScale().')';
//                if ($column->isUnsigned()) {
//                    $columnSql .= ' UNSIGNED';
//                }
                break;

            case Column::TYPE_DATETIME:
                if (empty($columnSql)) {
                    $columnSql .= 'DATETIME';
                }
                break;

            case Column::TYPE_TIMESTAMP:
                if (empty($columnSql)) {
                    $columnSql .= 'TIMESTAMP';
                }
                break;

            case Column::TYPE_CHAR:
                if (empty($columnSql)) {
                    $columnSql .= 'CHAR';
                }
                $columnSql .= '('.$column->getSize().')';
                break;

            case Column::TYPE_TEXT:
                if (empty($columnSql)) {
                    $columnSql .= 'NTEXT';
                }
                break;

            case Column::TYPE_BOOLEAN:
                if (empty($columnSql)) {
                    $columnSql .= 'BIT';
                }
                break;

            case Column::TYPE_FLOAT:
                if (empty($columnSql)) {
                    $columnSql .= 'FLOAT';
                }
                $size = $column->getSize();
                if ($size) {
//                    $scale = $column->getScale();
//                    if ($scale) {
//                        $columnSql .= '('.size.','.scale.')';
//                    } else {
                        $columnSql .= '('.size.')';
//                    }
                }
//                if ($column->isUnsigned()) {
//                    $columnSql .= ' UNSIGNED';
//                }
                break;

            case Column::TYPE_DOUBLE:
                if (empty($columnSql)) {
                    $columnSql .= 'NUMERIC';
                }
                $size = $column->getSize();
                if ($size) {
                    $scale = $column->getScale();
                    $columnSql .= '('.$size;
                    if ($scale) {
                        $columnSql .= ','.$scale.')';
                    } else {
                        $columnSql .= ')';
                    }
                }
//                if ($column->isUnsigned()) {
//                    $columnSql .= ' UNSIGNED';
//                }
                break;

            case Column::TYPE_BIGINTEGER:
                if (empty($columnSql)) {
                    $columnSql .= 'BIGINT';
                }
                $size = $column->getSize();
                if ($size) {
                    $columnSql .= '('.$size.')';
                }
//                if ($column->isUnsigned()) {
//                    $columnSql .= ' UNSIGNED';
//                }
                break;

            case Column::TYPE_TINYBLOB:
                if (empty($columnSql)) {
                    $columnSql .= 'VARBINARY(255)';
                }
                break;

            case Column::TYPE_BLOB:
            case Column::TYPE_MEDIUMBLOB:
            case Column::TYPE_LONGBLOB:
                if (empty($columnSql)) {
                    $columnSql .= 'VARBINARY(MAX)';
                }
                break;

            default:
                if (empty($columnSql)) {
                    throw new Exception('Unrecognized MsSql data type at column '.$column->getName());
                }

                $typeValues = $column->getTypeValues();
                if (!empty($typeValues)) {
                    if (is_array($typeValues)) {
                        $valueSql = '';
                        foreach ($typeValues as $value) {
                            $valueSql .= '"'.addcslashes($value, '"').'", ';
                        }
                        $columnSql .= '('.substr(valueSql, 0, -2).')';
                    } else {
                        $columnSql .= '("'.addcslashes($typeValues, '"').'")';
                    }
                }
                break;
        }

        return $columnSql;
    }

    /**
     * Generates SQL to add a column to a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param mixed  $column
     *
     * @return string
     */
    public function addColumn($tableName, $schemaName, \Phalcon\Db\ColumnInterface $column)
    {
        $sql = 'ALTER TABLE '.$this->prepareTable($tableName, $schemaName).' ADD ['.$column->getName().'] '.$this->getColumnDefinition($column);

        if ($column->hasDefault()) {
            $defaultValue = $column->getDefault();
            if (strpos(strtoupper($defaultValue), 'CURRENT_TIMESTAMP') !== false) {
                $sql .= ' DEFAULT CURRENT_TIMESTAMP';
            } else {
                $sql .= ' DEFAULT "'.addcslashes($defaultValue, '"').'"';
            }
        }

        if ($column->isNotNull()) {
            $sql .= ' NOT NULL';
        }

        if ($column->isAutoIncrement()) {
            $sql .= ' IDENTITY(1,1)';
        }

        if ($column->isFirst()) {
            $sql .= ' FIRST';
        } else {
            $afterPosition = $column->getAfterPosition();
            if ($afterPosition) {
                $sql .= ' AFTER '.$afterPosition;
            }
        }

        return $sql;
    }

    /**
     * Generates SQL to modify a column in a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param mixed  $column
     * @param mixed  $currentColumn
     *
     * @return string
     */
    public function modifyColumn($tableName, $schemaName, \Phalcon\Db\ColumnInterface $column, \Phalcon\Db\ColumnInterface $currentColumn = null)
    {
        $sql = 'ALTER TABLE '.$this->prepareTable($tableName, $schemaName).' ALTER COLUMN ['.$column->getName().'] '.$this->getColumnDefinition($column);

        if ($column->hasDefault()) {
            $defaultValue = $column->getDefault();
            if (strpos(strtoupper($defaultValue), 'CURRENT_TIMESTAMP') !== false) {
                $sql .= ' DEFAULT CURRENT_TIMESTAMP';
            } else {
                $sql .= ' DEFAULT "'.addcslashes($defaultValue, '"').'"';
            }
        }

        if ($column->isNotNull()) {
            $sql .= ' NOT NULL';
        }

        if ($column->isAutoIncrement()) {
            $sql .= ' IDENTITY(1,1)';
        }

        return $sql;
    }

    /**
     * Generates SQL to delete a column from a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param string $columnName
     *
     * @return string
     */
    public function dropColumn($tableName, $schemaName, $columnName)
    {
        return 'ALTER TABLE '.$this->prepareTable($tableName, $schemaName).' DROP COLUMN ['.$columnName.']';
    }

    /**
     * Generates SQL to add an index to a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param mixed  $index
     *
     * @return string
     */
    public function addIndex($tableName, $schemaName, \Phalcon\Db\IndexInterface $index)
    {
        $indexType = $index->getType();
        if (!empty($indexType)) {
            $sql = ' CREATE '.$indexType.' INDEX ';
        } else {
            $sql = ' CREATE INDEX ';
        }

        $sql = '['.$index->getName().'] ON '.$this->prepareTable($tableName, $schemaName).' ('.$this->getColumnList($index->getColumns()).')';

        return $sql;
    }

    /**
     * Generates SQL to delete an index from a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param string $indexName
     *
     * @return string
     */
    public function dropIndex($tableName, $schemaName, $indexName)
    {
        return 'DROP INDEX ['.$indexName.'] ON '.$this->prepareTable($tableName, $schemaName);
    }

    /**
     * Generates SQL to add the primary key to a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param mixed  $index
     *
     * @return string
     */
    public function addPrimaryKey($tableName, $schemaName, \Phalcon\Db\IndexInterface $index)
    {
        return 'ALTER TABLE '.$this->prepareTable($tableName, $schemaName).' ADD PRIMARY KEY ('.$this->getColumnList($index->getColumns()).')';
    }

    /**
     * Generates SQL to delete primary key from a table.
     *
     * @param string $tableName
     * @param string $schemaName
     *
     * @return string
     */
    public function dropPrimaryKey($tableName, $schemaName)
    {
        return 'ALTER TABLE '.$this->prepareTable($tableName, $schemaName).' DROP PRIMARY KEY';
    }

    /**
     * Generates SQL to add an index to a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param mixed  $reference
     *
     * @return string
     */
    public function addForeignKey($tableName, $schemaName, \Phalcon\Db\ReferenceInterface $reference)
    {
        $sql = 'ALTER TABLE '.$this->prepareTable($tableName, $schemaName).' ADD CONSTRAINT ['.$reference->getName().'] FOREIGN KEY ('.$this->getColumnList($reference->getColumns()).') REFERENCES '.$this->prepareTable($reference->getReferencedTable(), $reference->getReferencedSchema()).'('.$this->getColumnList($reference->getReferencedColumns()).')';

        $onDelete = $reference->getOnDelete();
        if (!empty($onDelete)) {
            $sql .= ' ON DELETE '.$onDelete;
        }

        $onUpdate = $reference->getOnUpdate();
        if (!empty($onUpdate)) {
            $sql .= ' ON UPDATE '.$onUpdate;
        }

        return $sql;
    }

    /**
     * Generates SQL to delete a foreign key from a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param string $referenceName
     *
     * @return string
     */
    public function dropForeignKey($tableName, $schemaName, $referenceName)
    {
        return 'ALTER TABLE '.$this->prepareTable($tableName, $schemaName).' DROP FOREIGN KEY ['.$referenceName.']';
    }

    /**
     * Generates SQL to create a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param array  $definition
     *
     * @return string
     */
    public function createTable($tableName, $schemaName, array $definition)
    {
        if (isset($definition['columns']) === false) {
            throw new Exception("The index 'columns' is required in the definition array");
        }

        $table = $this->prepareTable($tableName, $schemaName);

        $temporary = false;
        if (isset($definition['options']) === true) {
            $temporary = (bool) $definition['options']['temporary'];
        }

        /*
         * Create a temporary o normal table
         */
        if ($temporary) {
            $sql = 'CREATE TEMPORARY TABLE '.$table." (\n\t";
        } else {
            $sql = 'CREATE TABLE '.$table." (\n\t";
        }

        $createLines = [];
        foreach ($definition['columns'] as $column) {
            $columnLine = '['.$column->getName().'] '.$this->getColumnDefinition($column);

            /*
             * Add a Default clause
             */
            if ($column->hasDefault()) {
                $defaultValue = $column->getDefault();
                if (strpos(strtoupper($defaultValue), 'CURRENT_TIMESTAMP') !== false) {
                    $columnLine .= ' DEFAULT CURRENT_TIMESTAMP';
                } else {
                    $columnLine .= ' DEFAULT "'.addcslashes($defaultValue, '"').'"';
                }
            }

            /*
             * Add a NOT NULL clause
             */
            if ($column->isNotNull()) {
                $columnLine .= ' NOT NULL';
            }

            /*
             * Add an AUTO_INCREMENT clause
             */
            if ($column->isAutoIncrement()) {
                $columnLine .= ' IDENTITY(1,1)';
            }

            /*
             * Mark the column as primary key
             */
            if ($column->isPrimary()) {
                $columnLine .= ' PRIMARY KEY';
            }

            $createLines[] = $columnLine;
        }

        /*
         * Create related indexes
         */
        if (isset($definition['indexes']) === true) {
            foreach ($definition['indexes'] as $index) {
                $indexName = $index->getName();
                $indexType = $index->getType();

                /*
                 * If the index name is primary we add a primary key
                 */
                if ($indexName == 'PRIMARY') {
                    $indexSql = 'PRIMARY KEY ('.$this->getColumnList($index->getColumns()).')';
                } else {
                    if (!empty($indexType)) {
                        $indexSql = $indexType.' KEY ['.$indexName.'] ('.$this->getColumnList($index->getColumns()).')';
                    } else {
                        $indexSql = 'KEY ['.$indexName.'] ('.$this->getColumnList($index->getColumns()).')';
                    }
                }

                $createLines[] = $indexSql;
            }
        }

        /*
         * Create related references
         */
        if (isset($definition['references']) === true) {
            foreach ($definition['references'] as $reference) {
                $referenceSql = 'CONSTRAINT ['.$reference->getName().'] FOREIGN KEY ('.$this->getColumnList($reference->getColumns()).')'
                    .' REFERENCES ['.$reference->getReferencedTable().'] ('.$this->getColumnList($reference->getReferencedColumns()).')';

                $onDelete = $reference->getOnDelete();
                if (!empty($onDelete)) {
                    $referenceSql .= ' ON DELETE '.onDelete;
                }

                $onUpdate = $reference->getOnUpdate();
                if (!empty($onUpdate)) {
                    $referenceSql .= ' ON UPDATE '.onUpdate;
                }

                $createLines[] = $referenceSql;
            }
        }

        $sql .= implode(",\n\t", $createLines)."\n)";
        if (isset($definition['options'])) {
            $sql .= ' '.$this->_getTableOptions($definition);
        }

        return $sql;
    }

    /**
     * Generates SQL to drop a table.
     *
     * @param string $tableName
     * @param string $schemaName
     * @param bool   $ifExists
     *
     * @return string
     */
    public function dropTable($tableName, $schemaName = null, $ifExists = true)
    {
        $table = $this->prepareTable($tableName, $schemaName);

        if ($ifExists) {
            $sql = 'DROP TABLE IF EXISTS '.$table;
        } else {
            $sql = 'DROP TABLE '.$table;
        }

        return $sql;
    }

    /**
     * Generates SQL to create a view.
     *
     * @param string $viewName
     * @param array  $definition
     * @param string $schemaName
     *
     * @return string
     */
    public function createView($viewName, array $definition, $schemaName = null)
    {
        if (!isset($definition['sql'])) {
            throw new Exception("The index 'sql' is required in the definition array");
        }

        return 'CREATE VIEW '.$this->prepareTable($viewName, $schemaName).' AS '.$definition['sql'];
    }

    /**
     * Generates SQL to drop a view.
     *
     * @param string $viewName
     * @param string $schemaName
     * @param bool   $ifExists
     *
     * @return string
     */
    public function dropView($viewName, $schemaName = null, $ifExists = true)
    {
        $view = $this->prepareTable($viewName, $schemaName);

        if ($ifExists) {
            $sql = 'DROP VIEW IF EXISTS '.$view;
        } else {
            $sql = 'DROP VIEW '.$view;
        }

        return $sql;
    }

    /**
     * Generates SQL checking for the existence of a schema.table
     * <code>
     * echo $dialect->tableExists("posts", "blog");
     * echo $dialect->tableExists("posts");
     * </code>.
     *
     * @param string $tableName
     * @param string $schemaName
     *
     * @return string
     */
    public function tableExists($tableName, $schemaName = null)
    {
        $sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{$tableName}'";

        if ($schemaName) {
            $sql .= " AND TABLE_SCHEMA = '{$schemaName}'";
        }

        return $sql;
    }

    /**
     * Generates SQL checking for the existence of a schema.view.
     *
     * @param string $viewName
     * @param string $schemaName
     *
     * @return string
     */
    public function viewExists($viewName, $schemaName = null)
    {
        $sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '{$viewName}'";

        if ($schemaName) {
            $sql .= " AND TABLE_SCHEMA = '{$schemaName}'";
        }

        return $sql;
    }

    /**
     * Generates SQL describing a table
     * <code>
     * print_r($dialect->describeColumns("posts"));
     * </code>.
     *
     * @param string $table
     * @param string $schema
     *
     * @return string
     */
    public function describeColumns($table, $schema = null)
    {
        $sql = "exec sp_columns @table_name = '{$table}'";
        if ($schema) {
            $sql .= ", @table_owner = '{$schema}'";
        }

        return $sql;
    }

    /**
     * List all tables in database
     * <code>
     * print_r($dialect->listTables("blog"))
     * </code>.
     *
     * @param string $schemaName
     *
     * @return string
     */
    public function listTables($schemaName = null)
    {
        $sql = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES';
        if ($schemaName) {
            $sql .= " WHERE TABLE_SCHEMA = '{$schemaName}'";
        }

        return $sql;
    }

    /**
     * Generates the SQL to list all views of a schema or user.
     *
     * @param string $schemaName
     *
     * @return string
     */
    public function listViews($schemaName = null)
    {
        $sql = 'SELECT TABLE_NAME AS view_name FROM INFORMATION_SCHEMA.VIEWS';
        if ($schemaName) {
            $sql .= " WHERE TABLE_SCHEMA = '{$schemaName}'";
        }

        return $sql.' ORDER BY view_name';
    }

    /**
     * Generates SQL to query indexes on a table.
     *
     * @param string $table
     * @param string $schema
     *
     * @return string
     */
    public function describeIndexes($table, $schema = null)
    {
        $sql = "SELECT * FROM sys.indexes ind INNER JOIN sys.tables t ON ind.object_id = t.object_id WHERE t.name = '{$table}'";
        if ($schema) {
        }

        return $sql;
    }

    /**
     * Generates SQL to query foreign keys on a table.
     *
     * @param string $table
     * @param string $schema
     *
     * @return string
     */
    public function describeReferences($table, $schema = null)
    {
        $sql = 'SELECT TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME,REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL AND ';
        if ($schema) {
            $sql .= "CONSTRAINT_SCHEMA = '".$schema."' AND TABLE_NAME = '".$table."'";
        } else {
            $sql .= "TABLE_NAME = '".$table."'";
        }

        return $sql;
    }

    /**
     * Generates the SQL to describe the table creation options.
     *
     * @param string $table
     * @param string $schema
     *
     * @return string
     */
    public function tableOptions($table, $schema = null)
    {
        $sql = 'SELECT TABLES.TABLE_TYPE AS table_type,TABLES.AUTO_INCREMENT AS auto_increment,TABLES.ENGINE AS engine,TABLES.TABLE_COLLATION AS table_collation FROM INFORMATION_SCHEMA.TABLES WHERE ';
        if ($schema) {
            $sql .= "TABLES.TABLE_SCHEMA = '".$schema."' AND TABLES.TABLE_NAME = '".$table."'";
        } else {
            $sql .= "TABLES.TABLE_NAME = '".$table."'";
        }

        return $sql;
    }

    /**
     * Generates SQL to add the table creation options.
     *
     * @param array $definition
     *
     * @return string
     */
    protected function _getTableOptions($definition)
    {
        if (isset($definition['options']) === true) {
            $tableOptions = array();
            $options = $definition['options'];

            /*
             * Check if there is an ENGINE option
             */
            if (isset($options['ENGINE']) === true &&
                $options['ENGINE'] == true) {
                $tableOptions[] = 'ENGINE='.$options['ENGINE'];
            }

            /*
             * Check if there is an AUTO_INCREMENT option
             */
            if (isset($options['AUTO_INCREMENT']) === true &&
                $options['AUTO_INCREMENT'] == true) {
                $tableOptions[] = 'AUTO_INCREMENT='.$options['AUTO_INCREMENT'];
            }

            /*
             * Check if there is a TABLE_COLLATION option
             */
            if (isset($options['TABLE_COLLATION']) === true &&
                $options['TABLE_COLLATION'] == true) {
                $collationParts = explode('_', $options['TABLE_COLLATION']);
                $tableOptions[] = 'DEFAULT CHARSET='.$collationParts[0];
                $tableOptions[] = 'COLLATE='.$options['TABLE_COLLATION'];
            }

            if (count($tableOptions) > 0) {
                return implode(' ', $tableOptions);
            }
        }

        return '';
    }

    /**
     * Generates SQL primary key a table.
     *
     * @param string $table
     * @param string $schema
     *
     * @return string
     */
    public function getPrimaryKey($table, $schema = null)
    {
        $sql = "exec sp_pkeys @table_name = '{$table}'";
        if ($schema) {
            $sql .= ", @table_owner = '{$schema}'";
        }

        return $sql;
    }
}

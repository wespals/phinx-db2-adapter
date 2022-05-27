<?php

namespace PhinxDb2Adapter;

use Phinx\Db\Adapter\PdoAdapter;
use Phinx\Db\Adapter\AdapterInterface;
use Phinx\Db\Table;
use Phinx\Db\Table\Column;
use Phinx\Db\Table\Index;
use Phinx\Db\Table\ForeignKey;

class PhinxDb2Adapter extends PdoAdapter implements AdapterInterface
{

    const INT_SMALL   = 65535;
    const INT_REGULAR = 4294967295;
    const INT_BIG     = 18446744073709551615;

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        if (null === $this->connection) {
            if (!class_exists('PDO') || !in_array('ibm', \PDO::getAvailableDrivers(), true)) {
                // @codeCoverageIgnoreStart
                throw new \RuntimeException('You need to enable the PDO_IBM extension for Phinx to run properly.');
                // @codeCoverageIgnoreEnd
            }

            $db = null;
            $dsn = $this->getDsn();
            $options = $this->getOptions();

            try {
                $db = new \PDO($dsn, $options['user'], $options['pass'], [
                    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                    \PDO::ATTR_CASE => 0,
                    'autoQuoteIdentifiers' => true,
                    'fetchMode' => 2,
                    "autocommit" => DB2_AUTOCOMMIT_ON,
                    "i5_query_optimize" => DB2_FIRST_IO,
                    "i5_naming" => DB2_I5_NAMING_OFF,
                    "i5_lib" => $options['user']
                ]);
            } catch (\PDOException $exception) {
                throw new \InvalidArgumentException(sprintf(
                    'There was a problem connecting to the database: %s',
                    $exception->getMessage()
                ));
            }

            $this->setConnection($db);
        }
    }

    /**
     * Creates a PDO data source name for the adapter from config options
     *
     * @return string
     */
    protected function getDsn()
    {
        $options = $this->getOptions();
        $this->checkRequiredOptions($options);

        // check if using full connection string
        if (array_key_exists('host', $options)) {
            $dsn = ';DATABASE=' . $options['database']
                . ';HOSTNAME=' . $options['host']
                . ';PORT=' . $options['port']
                . ';PROTOCOL=' . 'TCPIP;';
        } else {
            // catalogued connection
            $dsn = $options['database'];
        }

        return 'ibm: ' . $dsn;
    }

    /**
     * Checks required options
     *
     * @param  array $options
     * @throws \InvalidArgumentException
     * @return void
     */
    protected function checkRequiredOptions($options)
    {
        if (array_key_exists('host', $options) && !array_key_exists('port', $options)) {
            throw new \InvalidArgumentException("Configuration options must have a key for 'port' when 'host' is specified");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        $this->connection = null;
    }

    /**
     * {@inheritdoc}
     */
    public function hasTransactions()
    {
        return false; //todo transactions available, but not working as expected
    }

    /**
     * {@inheritdoc}
     *
     * http://www.ibm.com/support/knowledgecenter/ssw_ibm_i_73/db2/rbafzsettraj.htm
     */
    public function beginTransaction()
    {
        $this->execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
    }

    /**
     * {@inheritdoc}
     */
    public function commitTransaction()
    {
        $this->execute('COMMIT');
    }

    /**
     * {@inheritdoc}
     */
    public function rollbackTransaction()
    {
        $this->execute('ROLLBACK');
    }

    /**
     * Quotes a schema name for use in a query.
     *
     * @param string $schemaName Schema Name
     * @return string
     */
    public function quoteSchemaName($schemaName)
    {
        return strtoupper($this->quoteColumnName($schemaName));
    }

    /**
     * {@inheritdoc}
     */
    public function quoteTableName($tableName)
    {
        return strtoupper($this->quoteColumnName($tableName));
    }

    /**
     * {@inheritdoc}
     */
    public function quoteColumnName($columnName)
    {
        return strtoupper($columnName);
    }

    /**
     * {@inheritdoc}
     */
    public function hasTable($tableName)
    {
        $exists = $this->fetchRow(sprintf(
            "SELECT TABLE_NAME
            FROM %s.SYSTABLES
            WHERE TABLE_SCHEMA = upper('%s') AND TABLE_NAME = upper('%s')",
            $this->getSchemaName(),
            $this->getSchemaName(),
            $tableName
        ));

        return !empty($exists);
    }

    /**
     * {@inheritdoc}
     */
    public function createTable(Table $table)
    {
        $this->startCommandTimer();
        $options = $table->getOptions();

        // Add the default primary key
        $columns = $table->getPendingColumns();
        if (!isset($options['id']) || (isset($options['id']) && $options['id'] === true)) {
            $column = new Column();
            $column->setName('id')
                ->setType('integer')
                ->setIdentity(true);

            array_unshift($columns, $column);
            $options['primary_key'] = 'id';

        } elseif (isset($options['id']) && is_string($options['id'])) {
            // Handle id => "field_name" to support AUTO_INCREMENT
            $column = new Column();
            $column->setName($options['id'])
                ->setType('integer')
                ->setIdentity(true);

            array_unshift($columns, $column);
            $options['primary_key'] = $options['id'];
        }

        $sql = 'CREATE TABLE ';
        $sql .= $this->quoteSchemaName($this->getSchemaName()) . '.';
        $sql .= $this->quoteTableName($table->getName()) . ' (';

        foreach ($columns as $column) {
            //BEFORE cannot be used in table creation
            $properties = $column->getProperties();
            unset($properties['before']);
            $column->setProperties($properties);

            $sql .= $this->quoteColumnName($column->getName()) . ' ' . $this->getColumnSqlDefinition($column, $table->getName()) . ', ';

            //todo add FK constraint to column definition sql
            //i.e. USER_ID INTEGER NOT NULL CONSTRAINT "FK_401_USER_ID_1" FOREIGN KEY ("USER_ID") REFERENCES HDUSER ("USER_ID") ON DELETE CASCADE ON UPDATE NO ACTION
            //REMOVE 'FOREIGN KEY ("USER_ID")' from clause
        }

        $sql = substr(rtrim($sql), 0, -1);
        $sql .= ');';

        // execute the sql
        $this->writeCommand('createTable', array($table->getName()));
        $this->execute($sql);

        // set the foreign keys
        $foreignKeys = $table->getForeignKeys();
        if (!empty($foreignKeys)) {
            foreach ($foreignKeys as $foreignKey) {
                $this->addForeignKey($table, $foreignKey);
            }
        }

        // set the indexes
        $indexes = $table->getIndexes();
        if (!empty($indexes)) {
            foreach ($indexes as $index) {
                $this->addIndex($table, $index);
            }
        }

        // set the primary key(s)
        if (isset($options['primary_key'])) {
            $sql = sprintf(
                'ALTER TABLE %s.%s ADD PRIMARY KEY (',
                $this->quoteSchemaName($this->getSchemaName()),
                $table->getName()
            );
            if (is_string($options['primary_key'])) {       // handle primary_key => 'id'
                $sql .= $this->quoteColumnName($options['primary_key']);
            } elseif (is_array($options['primary_key'])) { // handle primary_key => array('tag_id', 'resource_id')
                // PHP 5.4 will allow access of $this, so we can call quoteColumnName() directly in the anonymous function,
                // but for now just hard-code the adapter quotes
                $sql .= implode(
                    ',',
                    array_map(
                        function ($v) {
                            return strtoupper($v);
                        },
                        $options['primary_key']
                    )
                );
            }
            $sql .= ');';
            $this->execute($sql);
        }

        // process table comments
        if (isset($options['comment'])) {
            $sql = sprintf(
                'LABEL ON TABLE %s.%s IS %s',
                $this->quoteSchemaName($this->getSchemaName()),
                $this->quoteTableName($table->getName()),
                $this->getConnection()->quote($options['comment'])
            );
            $this->execute($sql);
        }

        // process column comments
        foreach ($columns as $column) {
            if ($column->getComment()) {
                $sql = $this->getColumnCommentSqlDefinition($column, $table->getName());
                $this->execute($sql);
            }
        }

        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function renameTable($tableName, $newTableName)
    {
        $this->startCommandTimer();
        $this->writeCommand('renameTable', array($tableName, $newTableName));
        $this->execute(sprintf(
            'RENAME TABLE %s.%s TO %s',
            $this->quoteSchemaName($this->getSchemaName()),
            $this->quoteTableName($tableName),
            $this->quoteTableName($newTableName)
        ));
        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function dropTable($tableName)
    {
        $this->startCommandTimer();
        $this->writeCommand('dropTable', array($tableName));
        $this->execute(sprintf(
            'DROP TABLE %s.%s',
            $this->quoteSchemaName($this->getSchemaName()),
            $this->quoteTableName($tableName)
        ));
        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function getColumns($tableName)
    {
        $columns = array();
        $rows = $this->describeColumns($tableName);
        foreach ($rows as $columnInfo) {

            $phinxType = $this->getPhinxType($columnInfo['DATA_TYPE']);

            $column = new Column();
            $column->setName($columnInfo['COLUMN_NAME'])
                ->setNull($columnInfo['IS_NULLABLE'] !== 'N')
                ->setDefault($columnInfo['COLUMN_DEFAULT'])
                ->setType($phinxType['name'])
                ->setLimit($phinxType['limit']);

            if ($columnInfo['IS_IDENTITY'] === 'YES') {
                $column->setIdentity(true);
            }

            $columns[] = $column;
        }

        return $columns;
    }

    /**
     * {@inheritdoc}
     */
    public function hasColumn($tableName, $columnName)
    {
        $rows = $this->describeColumns($tableName);
        foreach ($rows as $column) {
            if (strcasecmp($column['COLUMN_NAME'], strtoupper($columnName)) === 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get the definition for a `DEFAULT` statement.
     *
     * @param  mixed $default
     * @return string
     */
    protected function getDefaultValueDefinition($default)
    {
        if (is_string($default) && 'CURRENT_TIMESTAMP' !== $default) {
            $default = $this->getConnection()->quote($default);
        } elseif (is_bool($default)) {
            $default = $this->castToBool($default);
        }

        return isset($default) ? ' DEFAULT ' . $default : '';
    }

    /**
     * {@inheritdoc}
     */
    public function addColumn(Table $table, Column $column)
    {
        $this->startCommandTimer();
        $sql = sprintf(
            'ALTER TABLE %s.%s ADD COLUMN %s %s',
            $this->quoteSchemaName($this->getSchemaName()),
            $this->quoteTableName($table->getName()),
            $this->quoteColumnName($column->getName()),
            $this->getColumnSqlDefinition($column, $table->getName())
        );

        $this->writeCommand('addColumn', array($table->getName(), $column->getName(), $column->getType()));
        $this->execute($sql);

        if ($column->getComment()) {
            $sql = $this->getColumnCommentSqlDefinition($column, $table->getName());
            $this->execute($sql);
        }

        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function renameColumn($tableName, $columnName, $newColumnName)
    {
        $this->startCommandTimer();
        $rows = $this->describeColumns($tableName);
        foreach ($rows as $row) {
            if (strcasecmp($row['COLUMN_NAME'], strtoupper($columnName)) === 0) {

                if ($row['IS_IDENTITY'] === 'YES' || $row['IDENTITY_GENERATION'] == 'ALWAYS') {
                    throw new \Exception(sprintf(
                        'Cannot rename the identity column \'%s\'.',
                        $columnName
                    ));
                }

                $this->writeCommand('renameColumn', array($tableName, $columnName, $newColumnName));
                $table = new Table($tableName);
                $column = new Column();
                $column->setName($newColumnName);
                $column->setType($this->getPhinxType($row['DATA_TYPE']));
                $properties = ['before' => $columnName];
                if (array_key_exists('CCSID', $row) && !empty($row['CCSID']) && $row['DATA_TYPE'] != 'TIMESTMP') {
                    $properties['ccsid'] = $row['CCSID'];
                }
                if ( $row['DATA_TYPE'] != 'TIMESTMP') {
                    $properties['limit'] = $row['LENGTH'];
                }
                $column->setOptions([
                    'default'    => (!is_null($row['COLUMN_DEFAULT']) && $row['COLUMN_DEFAULT'] != 'NULL') ? $row['COLUMN_DEFAULT'] : null,
                    'null'       => ($row['IS_NULLABLE'] === 'Y') ? true : false,
                    'identity'   => false,
                    'comment'    => $row['COLUMN_TEXT'],
                    'precision'  => $row['NUMERIC_PRECISION'],
                    'scale'      => $row['NUMERIC_SCALE'],
                    'properties' => $properties
                ]);
                $this->addColumn($table, $column);

                // copy data to new column
                $this->execute("UPDATE $tableName SET $newColumnName = $columnName;");

                if ($column->getComment()) {
                    $sql = $this->getColumnCommentSqlDefinition($column, $table->getName());
                    $this->execute($sql);
                }

                $this->dropColumn($table->getName(), $columnName);

                //todo On Db2, only one ROWID, IDENTITY, or ROW CHANGE TIMESTAMP column allowed, therefore, unable to create new column before dropping old one
                //Also, the original column can be created with NOT NULL, and no DEFAULT value specified. With ALTER TABLE, it is requiring a default value to be specified with NOT NULL definition
//                if ($row['IS_IDENTITY'] === 'YES') {
//                    $column->setIdentity(true);
//                }
//
//                if ($row['DATA_TYPE'] == 'TIMESTMP' && $row['IDENTITY_GENERATION'] == 'ALWAYS') {
//                    $column->setUpdate('CURRENT_TIMESTAMP');
//                }
//                $this->changeColumn($tableName, $newColumnName, $column);

                $this->endCommandTimer();

                return;
            }
        }

        throw new \InvalidArgumentException(sprintf(
            'The specified column doesn\'t exist: '
            . $columnName
        ));
    }

    /**
     * {@inheritdoc}
     *
     * http://www.ibm.com/support/knowledgecenter/ssw_ibm_i_71/sqlp/rbafychgcol.htm
     */
    public function changeColumn($tableName, $columnName, Column $newColumn)
    {
        $this->startCommandTimer();
        $this->writeCommand('changeColumn', array($tableName, $columnName, $newColumn->getType()));

        // change data type
        $sql = sprintf(
            'ALTER TABLE %s.%s ALTER COLUMN %s SET DATA TYPE %s',
            $this->quoteSchemaName($this->getSchemaName()),
            $this->quoteTableName($tableName),
            $this->quoteColumnName($newColumn->getName()),
            $this->getColumnSqlDefinition($newColumn, $tableName)
        );

        //re-sequence NOT NULL and DEFAULT clauses when changing column
        $sql = preg_replace('/ NOT NULL/', '', $sql);
        //If it is set, DEFAULT is the last definition
        $sql = preg_replace('/DEFAULT .*/', '', $sql);

        $sql .= sprintf(
            '%s %s',
            !is_null($newColumn->getDefault()) ? $this->getDefaultValueDefinition($newColumn->getDefault()) : ' SET DEFAULT NULL',
            $newColumn->isNull() ? 'DROP NOT NULL' : 'SET NOT NULL'
        );

        if (is_null($newColumn->getDefault()) && !$newColumn->isNull()) {
            throw new \InvalidArgumentException(sprintf(
                'Column \'%s\' is defined as NOT NULL. Cannot set the default value to NULL.',
                $newColumn->getName()
            ));
        }

        $this->execute($sql);

        // change column comment if needed
        if ($newColumn->getComment()) {
            $sql = $this->getColumnCommentSqlDefinition($newColumn, $tableName);
            $this->execute($sql);
        }

        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function dropColumn($tableName, $columnName)
    {
        $this->startCommandTimer();
        $this->writeCommand('dropColumn', array($tableName, $columnName));
        $this->execute(
            sprintf(
                'ALTER TABLE %s.%s DROP COLUMN %s',
                $this->quoteSchemaName($this->getSchemaName()),
                $this->quoteTableName($tableName),
                $this->quoteColumnName($columnName)
            )
        );
        $this->endCommandTimer();
    }

    /**
     * Gets the Column Comment Definition for a column object.
     *
     * @param Column $column Column
     * @param string $tableName Table name
     * @return string
     */
    protected function getColumnCommentSqlDefinition(Column $column, $tableName)
    {
        return sprintf(
            'LABEL ON COLUMN %s.%s (%s Text is %s)',
            $this->quoteSchemaName($this->getSchemaName()),
            $this->quoteTableName($tableName),
            $this->quoteColumnName($column->getName()),
            $this->getConnection()->quote($column->getComment())
        );
    }

    /**
     * Get an array of indexes from a particular table.
     *
     * @param string $tableName Table Name
     * @return array
     */
    protected function getIndexes($tableName)
    {
        $indexes = array();
        $rows = $this->fetchAll(sprintf(
            "SELECT
              a.*,
              b.TABLE_NAME
            FROM %s.SYSKEYS a
              LEFT JOIN %s.SYSINDEXES b ON a.INDEX_NAME = b.INDEX_NAME
            WHERE a.INDEX_SCHEMA = upper('%s')
              AND b.TABLE_NAME = upper('%s')",
            $this->getSchemaName(),
            $this->getSchemaName(),
            $this->getSchemaName(),
            $tableName
        ));
        foreach ($rows as $row) {
            if (!isset($indexes[$row['INDEX_NAME']])) {
                $indexes[$row['INDEX_NAME']] = array('columns' => array());
            }
            $indexes[$row['INDEX_NAME']]['columns'][] = $row['COLUMN_NAME'];
        }

        return $indexes;
    }

    /**
     * {@inheritdoc}
     */
    public function hasIndex($tableName, $columns)
    {
        if (is_string($columns)) {
            $columns = array($columns); // str to array
        }
        $columns = array_map('strtoupper', $columns);
        $indexes = $this->getIndexes($tableName);
        foreach ($indexes as $index) {
            if (array_diff($index['columns'], $columns) === array_diff($columns, $index['columns'])) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function hasIndexByName($tableName, $indexName)
    {
        $indexes = $this->getIndexes($tableName);
        $indexName = strtoupper($indexName);
        foreach ($indexes as $name => $index) {
            if ($name === $indexName) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function addIndex(Table $table, Index $index)
    {
        $this->startCommandTimer();
        $this->writeCommand('addIndex', array($table->getName(), $index->getColumns()));
        $sql = $this->getIndexSqlDefinition($index, $table->getName());
        $this->execute($sql);
        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function dropIndex($tableName, $columns)
    {
        $this->startCommandTimer();
        if (is_string($columns)) {
            $columns = array($columns); // str to array
        }

        $this->writeCommand('dropIndex', array($tableName, $columns));
        $indexes = $this->getIndexes($tableName);
        $columns = array_map('strtoupper', $columns);

        foreach ($indexes as $indexName => $index) {
            if (array_diff($index['columns'], $columns) === array_diff($columns, $index['columns'])) {
                $this->execute(
                    sprintf(
                        'DROP INDEX %s',
                        $this->quoteColumnName($indexName)
                    )
                );
                $this->endCommandTimer();

                return;
            }
        }
    }

    /**
     * {@inheritdoc}
     *
     * On DB2, when a column is dropped, all indexes referencing the dropped column are automatically deleted
     * Therefore, to avoid error messages, we are checking if the index exists before dropping
     */
    public function dropIndexByName($tableName, $indexName)
    {
        $this->startCommandTimer();
        $this->writeCommand('dropIndexByName', array($tableName, $indexName));
        if ($this->hasIndexByName($tableName, $indexName)) {
            $sql = sprintf(
                'DROP INDEX %s',
                $this->quoteColumnName($indexName)
            );
            $this->execute($sql);
        }
        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function hasForeignKey($tableName, $columns, $constraint = null)
    {
        if (is_string($columns)) {
            $columns = array($columns); // str to array
        }
        $columns = array_map('strtoupper', $columns);
        $foreignKeys = $this->getForeignKeys($tableName);
        if ($constraint) {
            $constraint = strtoupper($constraint);
            if (isset($foreignKeys[$constraint])) {
                return !empty($foreignKeys[$constraint]);
            }

            return false;
        } else {
            foreach ($foreignKeys as $key) {
                if (array_diff($columns, $key['columns']) === array_diff($key['columns'], $columns)) {
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Get an array of foreign keys from a particular table.
     *
     * @param string $tableName
     * @return array
     */
    protected function getForeignKeys($tableName)
    {
        $foreignKeys = array();
        $rows = $this->fetchAll(sprintf(
            "SELECT
              FK_NAME, FKTABLE_NAME, FKCOLUMN_NAME, PKTABLE_NAME, PKCOLUMN_NAME
            FROM SYSIBM.SQLFOREIGNKEYS
            WHERE PKTABLE_SCHEM = upper('%s')
              AND FKTABLE_NAME = upper('%s')",
            $this->getSchemaName(),
            $tableName
        ));
        foreach ($rows as $row) {
            $foreignKeys[$row['FK_NAME']]['table'] = $row['FKTABLE_NAME'];
            $foreignKeys[$row['FK_NAME']]['columns'][] = $row['FKCOLUMN_NAME'];
            $foreignKeys[$row['FK_NAME']]['referenced_table'] = $row['PKTABLE_NAME'];
            $foreignKeys[$row['FK_NAME']]['referenced_columns'][] = $row['PKCOLUMN_NAME'];
        }

        return $foreignKeys;
    }

    /**
     * {@inheritdoc}
     */
    public function addForeignKey(Table $table, ForeignKey $foreignKey)
    {
        $this->startCommandTimer();
        $this->writeCommand('addForeignKey', array($table->getName(), $foreignKey->getColumns()));
        $this->execute(
            sprintf(
                'ALTER TABLE %s.%s ADD %s',
                $this->quoteSchemaName($this->getSchemaName()),
                $this->quoteTableName($table->getName()),
                $this->getForeignKeySqlDefinition($foreignKey, $table->getName())
            )
        );
        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     *
     * Add hasForeignKey check
     */
    public function dropForeignKey($tableName, $columns, $constraint = null)
    {
        $this->startCommandTimer();
        if (is_string($columns)) {
            $columns = array($columns); // str to array
        }
        $this->writeCommand('dropForeignKey', array($tableName, $columns));
        if ($constraint && $this->hasForeignKey($tableName, $columns, $constraint)) {
            $this->execute(
                sprintf(
                    'ALTER TABLE %s.%s DROP FOREIGN KEY %s.%s',
                    $this->quoteSchemaName($this->getSchemaName()),
                    $this->quoteTableName($tableName),
                    $this->quoteSchemaName($this->getSchemaName()),
                    strtoupper($constraint)
                )
            );
            $this->endCommandTimer();

            return;
        } else {
            foreach ($columns as $column) {
                $rows = $this->fetchAll(sprintf(
                    "SELECT
                        CONSTRAINT_NAME
                      FROM %s.SYSKEYCST
                      WHERE TABLE_SCHEMA = upper('%s')
                        AND TABLE_NAME = upper('%s')
                        AND COLUMN_NAME = upper('%s')
                      ORDER BY COLUMN_POSITION",
                    $this->quoteSchemaName($this->getSchemaName()),
                    $this->quoteSchemaName($this->getSchemaName()),
                    $tableName,
                    $column
                ));

                foreach ($rows as $row) {
                    $this->dropForeignKey($tableName, $columns, $row['CONSTRAINT_NAME']);
                }
            }
        }
        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     *
     * Adding custom column types
     */
    public function getSqlType($type, $limit = null)
    {
        switch ($type) {
            case static::PHINX_TYPE_TIME:
            case static::PHINX_TYPE_DATE:
                return array('name' => $type);
            case static::PHINX_TYPE_DATETIME:
            case static::PHINX_TYPE_TIMESTAMP:
                return array('name' => 'timestamp');
            case static::PHINX_TYPE_CHAR:
                return array('name' => 'char', 'limit' => 255);
            case static::PHINX_TYPE_STRING:
            case static::PHINX_TYPE_TEXT:
                return array('name' => 'varchar', 'limit' => 255);
            case static::PHINX_TYPE_BINARY:
                return array('name' => 'binary');
            case static::PHINX_TYPE_VARBINARY:
                return array('name' => 'varbinary');
            case static::PHINX_TYPE_BLOB:
                return array('name' => 'blob');
            case static::PHINX_TYPE_FLOAT:
                return array('name' => 'real'); //todo need double
            case static::PHINX_TYPE_INTEGER:
                if ($limit && $limit == static::INT_SMALL) {
                    return array(
                        'name' => 'smallint',
                        'limit' => static::INT_SMALL
                    );
                }

                return array('name' => $type);
            case static::PHINX_TYPE_BIG_INTEGER:
                return array('name' => 'bigint');
            case static::PHINX_TYPE_DECIMAL:
                return array('name' => $type, 'precision' => 18, 'scale' => 0); //todo need numeric
            case static::PHINX_TYPE_BOOLEAN:
                return array('name' => 'smallint', 'limit' => 1);
            default:
                throw new \RuntimeException('The Ibm DB2 for i5 type: "' . $type . '" is not supported');
        }
    }

    /**
     * Returns Phinx type by SQL type
     * http://www.ibm.com/support/knowledgecenter/ssw_i5_54/db2/rbafzmstch2data.htm
     *
     * @param string $sqlType SQL type
     * @returns string Phinx type
     */
    public function getPhinxType($sqlType)
    {
        switch ($sqlType) {
            case 'TIME':
                return static::PHINX_TYPE_TIME;
            case 'TIMESTMP':
            case 'TIMESTAMP':
                return static::PHINX_TYPE_TIMESTAMP;
            case 'DATE':
                return static::PHINX_TYPE_DATE;
            case 'CHAR':
                return static::PHINX_TYPE_CHAR;
            case 'VARCHAR':
            case 'CLOB':
                return static::PHINX_TYPE_STRING;
            case 'BINARY':
                return static::PHINX_TYPE_BINARY;
            case 'VARBINARY':
                return static::PHINX_TYPE_VARBINARY;
            case 'BLOB':
                return static::PHINX_TYPE_BLOB;
            case 'REAL':
            case 'DOUBLE':
                return static::PHINX_TYPE_FLOAT;
            case 'SMALLINT':
                return array(
                    'name' => 'smallint',
                    'limit' => static::INT_SMALL
                );
            case 'INTEGER':
                return static::PHINX_TYPE_INTEGER;
            case 'BIGINT':
                return static::PHINX_TYPE_BIG_INTEGER;
            case 'DECIMAL':
            case 'NUMERIC':
                return static::PHINX_TYPE_DECIMAL;
            default:
                throw new \RuntimeException('The Ibm DB2 for i5 type: "' . $sqlType . '" is not supported');
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createDatabase($name, $options = array())
    {
        $this->startCommandTimer();
        $this->writeCommand('createDatabase', array($name));
        $this->execute(sprintf("CREATE SCHEMA %s", $name));
        $this->endCommandTimer();
    }

    /**
     * {@inheritdoc}
     */
    public function hasDatabase($name)
    {
        $rows = $this->fetchAll(
            sprintf(
                "SELECT SCHEMA_NAME FROM SYSSCHEMAS WHERE SCHEMA_NAME = upper('%s')",
                $name
            )
        );

        foreach ($rows as $row) {
            if (!empty($row)) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function dropDatabase($name)
    {
        $this->startCommandTimer();
        $this->writeCommand('dropDatabase', array($name));
        $this->execute(sprintf('DROP SCHEMA `%s`', $name));
        $this->endCommandTimer();
    }

    /**
     * Gets the Column Definition for a Column object.
     *
     * @param Column $column Column
     * @param string $tableName The table name
     * @return string
     */
    protected function getColumnSqlDefinition(Column $column, $tableName = null)
    {
        $buffer = array();

        $sqlType = $this->getSqlType($column->getType(), $column->getLimit());
        $buffer[] = strtoupper($sqlType['name']);
        // integers cant have limits in db2
        if (static::PHINX_TYPE_DECIMAL === $sqlType['name'] && ($column->getPrecision() || $column->getScale())) {
            $buffer[] = sprintf(
                '(%s, %s)',
                $column->getPrecision() ? $column->getPrecision() : $sqlType['precision'],
                $column->getScale() ? $column->getScale() : $sqlType['scale']
            );
        } elseif (!in_array($sqlType['name'], array('bigint', 'integer', 'smallint'))) {
            if ($column->getLimit() || isset($sqlType['limit'])) {
                $buffer[] = sprintf('(%s)', $column->getLimit() ? $column->getLimit() : $sqlType['limit']);
            }
        }

        // set ccsid
        if (array_key_exists('ccsid', $column->getProperties())) {
            $props = $column->getProperties();
            $buffer[] = 'ccsid ' . $this->quoteColumnName($props['ccsid']);
        }

        $buffer[] = $column->isNull() ? '' : 'NOT NULL';

        if (!is_null($column->getDefault())) {
            $buffer[] = $this->getDefaultValueDefinition($column->getDefault());
        }

        //replace the DEFAULT value clause if the following options are defined
        if ($column->isIdentity()) {
            array_pop($buffer);
            $buffer[] = "GENERATED ALWAYS AS IDENTITY (NO CACHE CYCLE)";
        }elseif($column->getUpdate() == "CURRENT_TIMESTAMP"){
            //Check for existing TIMESTAMPS with IDENTITY GENERATION. On Db2, only one ROWID, IDENTITY, or ROW CHANGE TIMESTAMP column allowed
            $autoGeneratedTimestampFieldExists = false;
            $existingCols = $this->describeColumns($tableName);
            foreach ($existingCols as $col) {
                if ($col['DATA_TYPE'] == 'TIMESTMP' && $col['IDENTITY_GENERATION'] == 'ALWAYS') {
                    $autoGeneratedTimestampFieldExists = true;
                    break;
                }
            }
            if (!$autoGeneratedTimestampFieldExists) {
                array_pop($buffer);
                $buffer[] = "GENERATED ALWAYS FOR EACH ROW ON UPDATE AS ROW CHANGE TIMESTAMP";
            }
        }

        // set BEFORE
        if (array_key_exists('before', $column->getProperties())) {
            $props = $column->getProperties();
            $buffer[] = 'BEFORE ' . $this->quoteColumnName($props['before']);
        }

        return implode(' ', $buffer);
    }

    /**
     * Gets the Index Definition for an Index object.
     *
     * @param Index  $index Index
     * @param string $tableName Table name
     * @return string
     */
    protected function getIndexSqlDefinition(Index $index, $tableName)
    {
        if (is_string($index->getName())) {
            $indexName = $index->getName();
        } else {
            $columnNames = $index->getColumns();
            if (is_string($columnNames)) {
                $columnNames = array($columnNames);
            }
            $indexName = sprintf('%s_%s', $tableName, implode('_', $columnNames));
        }
        $indexName = strtoupper($indexName);
        $def = sprintf(
            "CREATE %s INDEX %s ON %s.%s (%s);",
            ($index->getType() === Index::UNIQUE ? 'UNIQUE' : ''),
            $indexName,
            $this->quoteSchemaName($this->getSchemaName()),
            $this->quoteTableName($tableName),
            implode(',', $index->getColumns())
        );

        return $def;
    }

    /**
     * Gets the Foreign Key Definition for an ForeignKey object.
     *
     * @param ForeignKey $foreignKey
     * @param string $tableName
     * @return string
     */
    protected function getForeignKeySqlDefinition(ForeignKey $foreignKey, $tableName)
    {
        $constraintName = $foreignKey->getConstraint() ?: $tableName . '_' . implode('_', $foreignKey->getColumns());
        $def = ' CONSTRAINT "' . $constraintName . '" FOREIGN KEY ("' . implode('", "', $foreignKey->getColumns()) . '")';
        $def .= " REFERENCES {$this->quoteTableName($foreignKey->getReferencedTable()->getName())} (\"" . implode('", "', $foreignKey->getReferencedColumns()) . '")';
        if ($foreignKey->getOnDelete()) {
            $def .= " ON DELETE {$foreignKey->getOnDelete()}";
        }
        if ($foreignKey->getOnUpdate()) {
            $def .= " ON UPDATE {$foreignKey->getOnUpdate()}";
        }

        $def = strtoupper($def);

        return $def;
    }

    /**
     * Describes a database table.
     *
     * @param string $tableName Table name
     * @return array
     */
    public function describeTable($tableName)
    {
        $sql = sprintf(
            "SELECT * FROM %s.SYSTABLES WHERE TABLE_SCHEMA = upper('%s') AND TABLE_NAME = upper('%s')",
            $this->getSchemaName(),
            $this->getSchemaName(),
            $tableName
        );

        return $this->fetchRow($sql);
    }

    /**
     * Describes table columms
     *
     * @param string $tableName Table name
     * @return array
     */
    public function describeColumns($tableName)
    {
        $sql = sprintf(
            "SELECT * FROM %s.SYSCOLUMNS WHERE TABLE_SCHEMA = upper('%s') AND TABLE_NAME = upper('%s')",
            $this->getSchemaName(),
            $this->getSchemaName(),
            $tableName
        );

        $rowset = $this->fetchAll($sql);
        // The fetch from DB2 'stringifies' numeric data types - cast the COLUMN_DEFAULT value, so the column DATA_TYPE matches the PHP data type
        if (!empty($rowset)) {
            for ($i = 0; $i < count($rowset); $i++) {
                $row = $rowset[$i];
                if (in_array($row['DATA_TYPE'], ['INTEGER', 'BIGINT', 'SMALLINT'])) {
                    $row['COLUMN_DEFAULT'] = (int)$row['COLUMN_DEFAULT'];
                }
                if (in_array($row['DATA_TYPE'], ['DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE'])) {
                    $row['COLUMN_DEFAULT'] = (float)$row['COLUMN_DEFAULT'];
                }
                // fix escaping of COLUMN_DEFAULT value on character data types, i.e. '\'Default Value\'' -> 'Default Value'
                if (in_array($row['DATA_TYPE'], ['CHAR', 'VARCHAR', 'CLOB'])) {
                    if (("'" == substr($row['COLUMN_DEFAULT'], 0, 1)) && ("'" == substr($row['COLUMN_DEFAULT'], -1))) {
                        $row['COLUMN_DEFAULT'] = substr_replace($row['COLUMN_DEFAULT'], "", 0, 1);
                        $row['COLUMN_DEFAULT'] = substr_replace($row['COLUMN_DEFAULT'], "", -1);
                    }
                }
                $rowset[$i] = $row;
            }
        }

        return $rowset;
    }

    /**
     * Returns Ibm Db2 i5 column types (inherited and Ibm Db2 i5 specified).
     *
     * @return array
     */
    public function getColumnTypes()
    {
        return array_merge(parent::getColumnTypes(), array(
            'datalink',
            'rowid',
            'varchar',
            'clob',
            'graphic',
            'vargraphic',
            'dbclob',
            'smallint',
            'bigint',
            'numeric'
        ));
    }

    /**
     * Gets the schema name.
     *
     * @return string
     */
    public function getSchemaName()
    {
        $options = $this->getOptions();

        return strtoupper($options['name']);
    }

    /**
     * {@inheritdoc}
     *
     * Phinx expects lower case column names when parsing for command output, therefore needed to change the uppercase column names returned from the ibmi
     * Also, use specific schema name in from clause
     */
    public function getVersionLog()
    {
        $result = array();
        $rows = $this->fetchAll(sprintf(
            'SELECT * FROM %s.%s ORDER BY VERSION ASC',
            $this->getSchemaName(),
            $this->getSchemaTableName()
        ));
        foreach ($rows as $version) {
            $result[$version['VERSION']] = array_change_key_case($version, CASE_LOWER);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     *
     * Add sql logging
     */
    public function execute($sql)
    {
        error_log($sql);
        return $this->getConnection()->exec($sql);
    }

    /**
     * {@inheritdoc}
     *
     * Add processing of HD extension column logic, if extension column suffixes are present
     * Add interpolateQuery to generate sql
     * Override the PDO stmt execution; problems with inserting specific data types
     * i.e. PDO::PARAM_NULL inserting 0 on integer columns
     */
    public function insert(Table $table, $row)
    {
        $this->startCommandTimer();
        $this->writeCommand('insert', array($table->getName()));

        $sql = sprintf(
            "INSERT INTO %s ",
            $this->quoteTableName($table->getName())
        );

        $columns = array_keys($row);
        $sql .= "(". implode(', ', array_map(array($this, 'quoteColumnName'), $columns)) . ")";
        $sql .= " VALUES (" . implode(', ', array_fill(0, count($columns), '?')) . ")";

        $query = $this->interpolateQuery($sql, array_values($row));

//        $stmt = $this->getConnection()->prepare($sql);
//        $stmt->execute(array_values($row));
        $this->execute($query);
        $this->endCommandTimer();
    }

    /**
     * Replaces any parameter placeholders in a query with the value of that
     * parameter. Useful for debugging. Assumes anonymous parameters from
     * $params are are in the same order as specified in $query
     *
     * @param string $query The sql query with parameter placeholders
     * @param array $params The array of substitution parameters
     * @return string The interpolated query
     */
    public function interpolateQuery($query, $params) {
        $keys = array();
        $values = $params;

        # build a regular expression for each parameter
        foreach ($params as $key => $value) {
            if (is_string($key)) {
                $keys[] = '/:' . $key . '/';
            } else {
                $keys[] = '/[?]/';
            }

            if (is_string($value)) {
                $values[$key] = "'" . $value . "'";
            }

            if (is_array($value)) {
                $values[$key] = "'" . implode("','", $value) . "'";
            }

            if (is_null($value)) {
                $values[$key] = 'NULL';
            }
        }

        $query = preg_replace($keys, $values, $query, 1, $count);

        return $query;
    }

}

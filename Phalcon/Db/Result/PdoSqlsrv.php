<?php

namespace Phalcon\Db\Result;

/**
 * Phalcon\Db\Result\PdoSqlsrv
 * Encapsulates the resultset internals
 * <code>
 * $result = $connection->query("SELECT * FROM robots ORDER BY name");
 * $result->setFetchMode(Phalcon\Db::FETCH_NUM);
 * while ($robot = $result->fetchArray()) {
 * print_r($robot);
 * }
 * </code>.
 */
class PdoSqlsrv extends Pdo
{
    /**
     * Gets number of rows returned by a resultset
     * <code>
     * $result = $connection->query("SELECT * FROM robots ORDER BY name");
     * echo 'There are ', $result->numRows(), ' rows in the resultset';
     * </code>.
     *
     * @return int
     */
    public function numRows()
    {
        $rowCount = $this->_rowCount;
        if ($rowCount === false) {
            $rowCount = $this->_pdoStatement->rowCount();

            if ($rowCount === false) {
                parent::numRows();
            }

            $this->_rowCount = $rowCount;
        }

        return $rowCount;
    }
}

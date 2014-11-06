<?php

namespace Database;

use Database\Query\Builder;

class Connection extends \PDO
{
    public function __construct($dsn, $username = null, $password = null, array $options = array())
    {
        $options[\PDO::ATTR_ERRMODE] = \PDO::ERRMODE_SILENT;
        parent::__construct($dsn, $username, $password, $options);
    }

    /**
     * @param string $sql
     * @internal param array $bindings
     * @throw SQLException
     * @return \PDOStatement
     */
    public function query($sql /*, array $bindings*/)
    {
        $bindings = $this->prepareBindings(func_get_args(), 1);

        if ($bindings) {
            $stmt = $this->prepare($sql);
            $stmt->execute($bindings);
        } else {
            $stmt = parent::query($sql);
        }

        $this->throwExceptionIfNeed($this->errorInfo(), $sql, $bindings);
        $stmt->setFetchMode(\PDO::FETCH_ASSOC);

        return $stmt;
    }

    /**
     * @param string $sql
     * @internal param array $bindings
     * @throw SQLException
     * @return array
     */
    public function queryAll($sql /*, array $bindings*/)
    {
        $stmt = call_user_func_array(array($this, 'query'), func_get_args());
        return $stmt->fetchAll();
    }

    /**
     * @param string $sql
     * @internal param array $bindings
     * @throw SQLException
     * @return int
     */
    public function exec($sql /*, array $bindings*/)
    {
        $bindings = $this->prepareBindings(func_get_args(), 1);

        if ($bindings) {
            $stmt = $this->prepare($sql);
            $stmt->execute($bindings);
            $this->throwExceptionIfNeed($stmt->errorInfo(), $sql, $bindings);
            $res = $stmt->rowCount();
        } else {
            $res = parent::exec($sql);
        }

        $this->throwExceptionIfNeed($this->errorInfo(), $sql, $bindings);

        return $res;
    }

    public function execute(Builder $query)
    {
        $data = $query->toSql();

        if ($query->command == 'select') {
            return $this->queryAll($data[0], $data[1]);
        }

        return $this->exec($data[0], $data[1]);
    }

    public function prepare($sql, $options = array())
    {
        $sth = parent::prepare($sql, $options);
        $this->throwExceptionIfNeed($this->errorInfo(), $sql, null);
        return $sth;
    }

    protected function throwExceptionIfNeed($errorInfo, $query, $bindings)
    {
        if ($this->errorCode() == SQLException::CODE_SUCCESS) {
            return;
        }

        throw new SQLException($errorInfo, $query, $bindings);
    }

    /**
     * Flatten array of bindings if need
     *
     * @param $bindings
     * @param int $skip
     * @return array
     */
    protected function prepareBindings($bindings, $skip = 0)
    {
        $res = array();

        foreach ($bindings as $a) {

            if ($skip > 0) {
                $skip--;
                continue;
            }

            if (is_array($a)) {
                $res = array_merge($res, $a);
                continue;
            }

            $res[] = $a;
        }

        return $res;
    }

    /**
     * Executes a function in a transaction.
     *
     * The function gets passed this Connection instance as an (optional) parameter.
     *
     * If an exception occurs during execution of the function or transaction commit,
     * the transaction is rolled back and the exception re-thrown.
     *
     * @param \Closure $func The function to execute transactionally.
     * @throws \Exception
     */
    public function transaction(\Closure $func)
    {
        $this->beginTransaction();
        try {
            $func($this);
            $this->commit();
        } catch (\Exception $e) {
            $this->rollback();
            throw $e;
        }
    }

    public function execBatch($strSql)
    {
        $parts = preg_split(
            '/("(?:\\\\.|[^"])*"|\'(?:\\\\.|[^\'])*\'|`(?:\\\\.|[^`])*`|\/\*(?:.*?)\*\/|#[^\n]*$|--[^\n]*$|;)/sm',
            $strSql,
            0,
            PREG_SPLIT_DELIM_CAPTURE
        );
        $sql = '';
        foreach ($parts as $p) {
            if ((strlen($p) > 0 && $p{0} == '#') or substr($p, 0, 2) == '--' or substr($p, 0, 2) == '/*') {
                continue;
            }

            if ($p != ';') {
                $sql .= $p;
                continue;
            }

            if (strlen(trim($sql))) {
                $this->exec($sql);
                $sql = '';
            }
        }

        if (strlen(trim($sql))) {
            $this->exec($sql);
        }

        return true;
    }
}
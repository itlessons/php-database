<?php

namespace Database;

use Database\Query\Builder;
use Database\Query\Grammar;
use Database\Query\Query;

class Manager
{
    protected $config = array();
    protected $grammar;
    private $defaultConnectionName = 'default';

    /**
     * @var Connection[]
     */
    protected $connections = array();

    public function addConnection($config, $name = null)
    {
        if (!$name) {
            $name = $this->getDefaultConnectionName();
        }

        if (!isset($config['dsn'])) {
            throw new \InvalidArgumentException('Dsn is required!');
        }

        $this->config[$name] = $config;
    }

    public function getConnection($name = null)
    {
        if (!$name) {
            $name = $this->getDefaultConnectionName();
        }

        if (!isset($this->connections[$name])) {
            $this->connections[$name] = $this->makeConnection($name);
        }

        return $this->connections[$name];
    }

    public function getDefaultConnectionName()
    {
        return $this->defaultConnectionName;
    }

    public function setDefaultConnectionName($name)
    {
        $this->defaultConnectionName = $name;
    }

    protected function makeConnection($name)
    {
        if (!isset($this->config[$name])) {
            throw new \InvalidArgumentException('Config for connection with name "' . $name . '" not found');
        }

        $conf = $this->config[$name];

        $dsn = $conf['dsn'];
        $username = isset($conf['username']) ? $conf['username'] : null;
        $password = isset($conf['password']) ? $conf['password'] : null;
        $options = isset($conf['options']) ? $conf['options'] : array();

        return new Connection($dsn, $username, $password, $options);
    }

    /**
     * @param $table Table name
     * @return Builder
     */
    public function table($table)
    {
        $builder = new Builder($this->getGrammar());
        return $builder->table($table);
    }

    /**
     * @param $table Table name
     * @param $connName Connection name
     * @return Query
     */
    public function query($table, $connName)
    {
        $query = new Query($this->getConnection($connName), $this->getGrammar());
        return $query->table($table);
    }

    public function getGrammar()
    {
        if (!$this->grammar) {
            $this->grammar = new Grammar();
        }

        return $this->grammar;
    }

    public function setGrammar(Grammar $grammar)
    {
        $this->grammar = $grammar;
    }
} 
<?php

namespace Database\Query;


use Database\Connection;

class Query extends Builder
{
    /**
     * @var Connection
     */
    private $connection;

    public function __construct(Connection $connection, Grammar $grammar = null)
    {
        $this->connection = $connection;
        parent::__construct($grammar);
    }

    public function execute()
    {
        return $this->connection->execute($this);
    }

    public function setConnection(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function getConnection()
    {
        return $this->connection;
    }
} 
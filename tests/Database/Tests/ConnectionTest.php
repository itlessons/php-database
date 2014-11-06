<?php

namespace Database\Tests;

use Database\Connection;
use Database\Query\Builder;
use Database\Query\Grammar;

class ConnectionTest extends \PHPUnit_Framework_TestCase
{
    private $conn;

    /**
     * @return Connection
     */
    protected function getConnection()
    {
        if ($this->conn == null) {
            $this->conn = new Connection('sqlite::memory:');
            $this->conn->execBatch(file_get_contents(__DIR__ . '/files/users.db'));
        }

        return $this->conn;
    }

    /**
     * @expectedException \Database\SQLException
     * @expectedExceptionMessageRegExp /no such table: userz/
     */
    public function testException()
    {
        $this->getConnection()->queryAll('select * from userz');
    }

    public function testUsersCount()
    {
        $data = $this->getConnection()->queryAll('select count(*) as cnt from users');
        $this->assertEquals([['cnt' => 3]], $data);
    }

    public function testExecuteQuery()
    {
        $builder = new Builder(new Grammar());
        $builder
            ->select('name')
            ->from('users')
            ->where('id', 3);

        $data = $this->getConnection()->execute($builder);
        $this->assertEquals([['name' => 'jack']], $data);
    }

    public function testUpdate()
    {
        $this->getConnection()->exec('update users set name="mr.zero" where id=?', 1);
        $data = $this->getConnection()->queryAll('select name from users where id=?', 1);
        $this->assertEquals([['name' => 'mr.zero']], $data);
    }

    public function testBatch()
    {
        $sql = ''
            . 'begin transaction;'
            . 'delete from users where id=4;'
            . 'insert into users (id, name) values (4, \'black\'); '
            . 'commit;';


        $this->getConnection()->execBatch($sql);

        $data = $this->getConnection()->queryAll('select name from users where id=?', 4);
        $this->assertEquals([['name' => 'black']], $data);
    }
}
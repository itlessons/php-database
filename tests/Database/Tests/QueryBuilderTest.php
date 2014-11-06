<?php

namespace Database\Tests;

use Database\Query\Builder;

class QueryBuilderTest extends \PHPUnit_Framework_TestCase
{
    private function getBuilder()
    {
        return new \Database\Query\Builder(new \Database\Query\Grammar());
    }

    public function testBasicAlias()
    {
        $data = $this->getBuilder()
            ->select('foo as bar')
            ->from('users')
            ->toSql();

        $this->assertEquals('select `foo` as `bar` from `users`', $data[0]);
        $this->assertEquals([], $data[1]);


        $data = $this->getBuilder()
            ->select('x.y as foo.bar')
            ->from('baz')
            ->toSql();

        $this->assertEquals('select `x`.`y` as `foo`.`bar` from `baz`', $data[0]);
        $this->assertEquals([], $data[1]);

        $data = $this->getBuilder()
            ->table('public.users')
            ->toSql();

        $this->assertEquals('select * from `public`.`users`', $data[0]);
        $this->assertEquals([], $data[1]);
    }

    public function testWrappingQuotation()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('some`table')
            ->toSql();

        $this->assertEquals('select * from `some``table`', $data[0]);
    }

    public function testSelect()
    {
        $data = $this->getBuilder()
            ->table('users')
            ->select('name')
            ->where('id', 1)
            ->toSql();


        $this->assertEquals('select `name` from `users` where `id` = ?', $data[0]);
        $this->assertEquals([1], $data[1]);

        $data = $this->getBuilder()
            ->table('users')
            ->where('name', '=', 'John')
            ->orWhere(function ($query) {
                $query->where('votes', '>', 100)
                    ->where('title', '<>', 'Admin');
            })
            ->toSql();

        $this->assertEquals('select * from `users` where `name` = ? or (`votes` > ? and `title` <> ?)', $data[0]);
        $this->assertEquals(['John', 100, 'Admin'], $data[1]);

        $data = $this->getBuilder()
            ->table('users')
            ->count()
            ->toSql();

        $this->assertEquals('select count(*) as aggregate from `users`', $data[0]);
        $this->assertEquals([], $data[1]);
    }

    public function testWhere()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('id', '=', 3)
            ->toSql();

        $this->assertEquals('select * from `users` where `id` = ?', $data[0]);
        $this->assertEquals([3], $data[1]);
    }


    public function testWhereDayMySql()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereDay('created_at', '=', 1)
            ->toSql();

        $this->assertEquals('select * from `users` where day(`created_at`) = ?', $data[0]);
        $this->assertEquals([1], $data[1]);
    }

    public function testWhereMonthMySql()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereMonth('created_at', '=', 5)
            ->toSql();

        $this->assertEquals('select * from `users` where month(`created_at`) = ?', $data[0]);
        $this->assertEquals([5], $data[1]);
    }

    public function testWhereYearMySql()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereYear('created_at', '=', 2014)
            ->toSql();

        $this->assertEquals('select * from `users` where year(`created_at`) = ?', $data[0]);
        $this->assertEquals(array(0 => 2014), $data[1]);
    }


    public function testWhereBetweens()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereBetween('id', [1, 2])
            ->toSql();

        $this->assertEquals('select * from `users` where `id` between ? and ?', $data[0]);
        $this->assertEquals([1, 2], $data[1]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereNotBetween('id', array(1, 2))
            ->toSql();

        $this->assertEquals('select * from `users` where `id` not between ? and ?', $data[0]);
        $this->assertEquals([1, 2], $data[1]);
    }

    public function testBasicOrWheres()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('id', '=', 1)
            ->orWhere('email', '=', 'foo')
            ->toSql();

        $this->assertEquals('select * from `users` where `id` = ? or `email` = ?', $data[0]);
        $this->assertEquals([1, 'foo'], $data[1]);
    }

    public function testRawWheres()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereRaw('id = ? or email = ?', [1, 'foo'])
            ->toSql();

        $this->assertEquals('select * from `users` where id = ? or email = ?', $data[0]);
        $this->assertEquals([1, 'foo'], $data[1]);
    }

    public function testRawOrWheres()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('id', '=', 1)
            ->orWhereRaw('email = ?', ['foo'])
            ->toSql();

        $this->assertEquals('select * from `users` where `id` = ? or email = ?', $data[0]);
        $this->assertEquals([1, 'foo'], $data[1]);
    }

    public function testBasicWhereIns()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereIn('id', [1, 2, 3])
            ->toSql();

        $this->assertEquals('select * from `users` where `id` in (?, ?, ?)', $data[0]);
        $this->assertEquals([1, 2, 3], $data[1]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('id', '=', 1)
            ->orWhereIn('id', [1, 2, 3])
            ->toSql();

        $this->assertEquals('select * from `users` where `id` = ? or `id` in (?, ?, ?)', $data[0]);
        $this->assertEquals([1, 1, 2, 3], $data[1]);
    }

    public function testBasicWhereNotIns()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereNotIn('id', [1, 2, 3])
            ->toSql();

        $this->assertEquals('select * from `users` where `id` not in (?, ?, ?)', $data[0]);
        $this->assertEquals([1, 2, 3], $data[1]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('id', '=', 1)
            ->orWhereNotIn('id', [1, 2, 3])
            ->toSql();

        $this->assertEquals('select * from `users` where `id` = ? or `id` not in (?, ?, ?)', $data[0]);
        $this->assertEquals([1, 1, 2, 3], $data[1]);
    }

    public function testSubSelectWhereIns()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereIn('id', function (Builder $q) {
                $q->select('id')
                    ->from('users')
                    ->where('age', '>', 25)
                    ->limit(3);
            })
            ->toSql();

        $this->assertEquals('select * from `users` where `id` in (select `id` from `users` where `age` > ? limit 3)', $data[0]);
        $this->assertEquals([25], $data[1]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereNotIn('id', function ($q) {
                $q->select('id')->from('users')->where('age', '>', 25)->limit(3);
            })
            ->toSql();

        $this->assertEquals('select * from `users` where `id` not in (select `id` from `users` where `age` > ? limit 3)', $data[0]);
        $this->assertEquals(array(25), $data[1]);
    }


    public function testBasicWhereNulls()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereNull('id')
            ->toSql();

        $this->assertEquals('select * from `users` where `id` is null', $data[0]);
        $this->assertEquals([], $data[1]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('id', '=', 1)
            ->orWhereNull('id')
            ->toSql();

        $this->assertEquals('select * from `users` where `id` = ? or `id` is null', $data[0]);
        $this->assertEquals([1], $data[1]);
    }

    public function testBasicWhereNotNulls()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->whereNotNull('id')
            ->toSql();

        $this->assertEquals('select * from `users` where `id` is not null', $data[0]);
        $this->assertEquals([], $data[1]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('id', '>', 1)
            ->orWhereNotNull('id')
            ->toSql();

        $this->assertEquals('select * from `users` where `id` > ? or `id` is not null', $data[0]);
        $this->assertEquals([1], $data[1]);
    }

    public function testNestedWheres()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('email', '=', 'foo')
            ->orWhere(function ($q) {
                $q->where('name', '=', 'bar')
                    ->where('age', '=', 25);
            })
            ->toSql();

        $this->assertEquals('select * from `users` where `email` = ? or (`name` = ? and `age` = ?)', $data[0]);
        $this->assertEquals(['foo', 'bar', 25], $data[1]);
    }

    public function testFullSubSelects()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->where('email', '=', 'foo')
            ->orWhere('id', '=', function (Builder $q) {
                $q->select($q->raw('max(id)'))
                    ->from('users')
                    ->where('email', '=', 'bar');
            })->toSql();

        $this->assertEquals('select * from `users` where `email` = ? or `id` = (select max(id) from `users` where `email` = ?)', $data[0]);
        $this->assertEquals(['foo', 'bar'], $data[1]);
    }

    public function testGroupBy()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->groupBy('id', 'email')
            ->toSql();

        $this->assertEquals('select * from `users` group by `id`, `email`', $data[0]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->groupBy(['id', 'email'])
            ->toSql();
        $this->assertEquals('select * from `users` group by `id`, `email`', $data[0]);
    }

    public function testOrderBy()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->orderBy('email')
            ->orderBy('age', 'desc')
            ->toSql();

        $this->assertEquals('select * from `users` order by `email` asc, `age` desc', $data[0]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->orderBy('email')
            ->orderByRaw('age ? desc', 'foo')
            ->toSql();

        $this->assertEquals('select * from `users` order by `email` asc, age ? desc', $data[0]);
        $this->assertEquals(['foo'], $data[1]);
    }

    public function testHaving()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->having('email', '>', 1)
            ->toSql();

        $this->assertEquals('select * from `users` having `email` > ?', $data[0]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->groupBy('email')
            ->having('email', '>', 1)
            ->toSql();

        $this->assertEquals('select * from `users` group by `email` having `email` > ?', $data[0]);

        $data = $this->getBuilder()
            ->select('email as foo_email')
            ->from('users')
            ->having('foo_email', '>', 1)
            ->toSql();

        $this->assertEquals('select `email` as `foo_email` from `users` having `foo_email` > ?', $data[0]);
    }

    public function testRawHavings()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->havingRaw('user_foo < user_bar')
            ->toSql();

        $this->assertEquals('select * from `users` having user_foo < user_bar', $data[0]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->having('baz', '=', 1)
            ->orHavingRaw('user_foo < user_bar')
            ->toSql();

        $this->assertEquals('select * from `users` having `baz` = ? or user_foo < user_bar', $data[0]);
    }

    public function testLimitsAndOffsets()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->offset(5)
            ->limit(10)
            ->toSql();

        $this->assertEquals('select * from `users` limit 10 offset 5', $data[0]);
    }

    public function testJoin()
    {
        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->join('contacts', 'users.id', '=', 'contacts.id')
            ->leftJoin('photos', 'users.id', '=', 'photos.id')
            ->toSql();

        $this->assertEquals('select * from `users` inner join `contacts` on `users`.`id` = `contacts`.`id` left join `photos` on `users`.`id` = `photos`.`id`', $data[0]);

        $data = $this->getBuilder()
            ->select('*')
            ->from('users')
            ->leftJoinWhere('photos', 'users.id', '=', 'bar')
            ->joinWhere('photos', 'users.id', '=', 'foo')
            ->toSql();

        $this->assertEquals('select * from `users` left join `photos` on `users`.`id` = ? inner join `photos` on `users`.`id` = ?', $data[0]);
        $this->assertEquals(['bar', 'foo'], $data[1]);
    }

    public function testInsert()
    {
        $data = $this->getBuilder()
            ->table('users')
            ->insert([
                ['email' => 'john@example.com', 'votes' => 0],
                ['email' => 'john1@example.com', 'votes' => 1]
            ])->toSql();

        $this->assertEquals('insert into `users` (`email`, `votes`) values (?, ?), (?, ?)', $data[0]);
        $this->assertEquals(['john@example.com', 0, 'john1@example.com', 1], $data[1]);
    }

    public function testDelete()
    {
        $data = $this->getBuilder()
            ->table('user')
            ->delete()
            ->whereIn('id', [1, 2, 3])
            ->toSql();

        $this->assertEquals('delete from `user` where `id` in (?, ?, ?)', $data[0]);
        $this->assertEquals([1, 2, 3], $data[1]);
    }

    public function testUpdate()
    {
        $data = $this->getBuilder()
            ->table('user')
            ->update(['votes' => 2])
            ->where('id', 1)
            ->toSql();

        $this->assertEquals('update `user` set `votes` = ? where `id` = ?', $data[0]);
        $this->assertEquals([2, 1], $data[1]);
    }

    public function testAggregateFunctions()
    {
        $data = $this->getBuilder()
            ->from('users')
            ->count()
            ->toSql();

        $this->assertEquals('select count(*) as aggregate from `users`', $data[0]);

        $data = $this->getBuilder()
            ->from('users')
            ->max('id')
            ->toSql();

        $this->assertEquals('select max(`id`) as aggregate from `users`', $data[0]);

        $data = $this->getBuilder()
            ->from('users')
            ->min('id')
            ->toSql();

        $this->assertEquals('select min(`id`) as aggregate from `users`', $data[0]);

        $data = $this->getBuilder()
            ->from('users')
            ->sum('id')
            ->toSql();

        $this->assertEquals('select sum(`id`) as aggregate from `users`', $data[0]);

    }
}
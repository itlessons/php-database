## PHP Database

Very simple database toolkit for PHP, providing simple pdo wrapper connection and query builder. 
It currently supports MySQL.
Сreated for the study. But you can use it in real projects.
Сode taken from Laravel Database Layer and simplified.

### Usage

First, create a new manager instance.

```PHP
use Database\Manager;

$manager = new Manager;

$manager->addConnection([
    'dsn' => 'mysql:dbname=testdb;host=127.0.0.1',
    'username' => 'root',
    'password' => null,
    'options' => [] // pdo driver options 
]);
```

Once the Manager instance has been registered. You may use it like so:

**Using Connection**

```PHP
$conn = $manager->getConnection();
$data = $conn->queryAll('select * from users where id in (?,?,?)', 1, 2, 3);
// $data = $conn->queryAll('select * from users where id in (?,?,?)', [1, 2, 3]);
// $data => [['id' => 1, 'name' => 'joe'], ['id' => 2, 'name' => 'jack'], ['id' => 3, 'name' => 'bob']]

$pdoSth = $conn->query('select * from users where id = ?',1);

$conn->exec('insert into users (id, name) values (?, ?)', 1, 'joe'));

$conn->execBatch(file_get_contents('file_with_queries.txt'));
```

**Using The Query Builder**

```PHP
$builder = $manager
                ->table('users')
                ->select('*')
                ->whereIn('id', [1,2,3]);
                
// $builder->toSql() -> ['select * from users where id in (?,?,?)', [1,2,3]]

$data = $conn->execute($builder);
// $data -> [['id' => 1, 'name' => 'joe'], ['id' => 2, 'name' => 'jack'], ['id' => 3, 'name' => 'bob']]


$data = $manager
            ->table('user')
            ->update(['votes' => 2])
            ->where('id', 1)
            ->toSql();
// $data -> ['update `user` set `votes` = ? where `id` = ?',[2,1]]
            
$data = $manager
          ->table('user')
          ->insert([
              ['email' => 'john@example.com', 'votes' => 0],
              ['email' => 'john1@example.com', 'votes' => 1]
          ])->toSql();            
          
// $data -> [
//      'insert into `users` (`email`, `votes`) values (?, ?), (?, ?)', 
//      ['john@example.com', 0, 'john1@example.com', 1]
// ]          
            
```

More about query builder see in [QueryBuilderTest](https://github.com/itlessons/php-database/blob/master/tests/Database/QueryBuilderTest.php)

### Resources

You can run the unit tests with the following command:

    $ cd path/to/php-database/
    $ composer.phar install
    $ phpunit

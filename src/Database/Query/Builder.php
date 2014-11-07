<?php

namespace Database\Query;

class Builder
{
    protected $grammar;
    protected $bindings = array(
        'select' => [],
        'join' => [],
        'where' => [],
        'having' => [],
        'order' => [],
    );

    protected $values = [];

    public $aggregate;
    public $columns;
    public $distinct = false;
    public $table;
    public $command = 'select';
    public $joins;
    public $wheres;
    public $groups;
    public $havings;
    public $orders;
    public $limit;
    public $offset;
    public $lock;
    protected $operators = array(
        '=', '<', '>', '<=', '>=', '<>', '!=',
        'like', 'not like', 'between', 'ilike',
        '&', '|', '^', '<<', '>>',
        'rlike', 'regexp', 'not regexp',
    );

    public function __construct(Grammar $grammar = null)
    {
        $this->grammar = $grammar != null ? $grammar : new Grammar();
    }

    /**
     * Set the columns to be selected.
     *
     * @param  array|Expression $columns
     * @return $this
     */
    public function select($columns = array('*'))
    {
        $this->columns = is_array($columns) ? $columns : func_get_args();
        return $this;
    }

    /**
     * Add a new "raw" select expression to the query.
     *
     * @param  string $expression
     * @return $this
     */
    public function selectRaw($expression)
    {
        return $this->select(new Expression($expression));
    }

    /**
     * Add a new select column to the query.
     *
     * @param  mixed $column
     * @return $this
     */
    public function addSelect($column)
    {
        $column = is_array($column) ? $column : func_get_args();

        $this->columns = array_merge((array)$this->columns, $column);

        return $this;
    }

    /**
     * Force the query to only return distinct results.
     *
     * @return $this
     */
    public function distinct()
    {
        $this->distinct = true;

        return $this;
    }

    /**
     * Set the table which the query is targeting.
     *
     * @param  string $table
     * @return $this
     */
    public function table($table)
    {
        $this->table = $table;

        return $this;
    }

    public function from($table)
    {
        return $this->table($table);
    }

    /**
     * Set the sql command.
     *
     * @param  string $command
     * @return $this
     */
    protected function command($command)
    {
        $this->command = $command;

        return $this;
    }

    /**
     * Add a join clause to the query.
     *
     * @param  string $table
     * @param  string $one
     * @param  string $operator
     * @param  string $two
     * @param  string $type
     * @param  bool $where
     * @return $this
     */
    public function join($table, $one, $operator = null, $two = null, $type = 'inner', $where = false)
    {
        if ($one instanceof \Closure) {
            $this->joins[] = new JoinClause($this, $type, $table);

            call_user_func($one, end($this->joins));
        } else {
            $join = new JoinClause($this, $type, $table);

            $this->joins[] = $join->on(
                $one, $operator, $two, 'and', $where
            );
        }

        return $this;
    }

    /**
     * Add a "join where" clause to the query.
     *
     * @param  string $table
     * @param  string $one
     * @param  string $operator
     * @param  string $two
     * @param  string $type
     * @return $this
     */
    public function joinWhere($table, $one, $operator, $two, $type = 'inner')
    {
        return $this->join($table, $one, $operator, $two, $type, true);
    }

    /**
     * Add a left join to the query.
     *
     * @param  string $table
     * @param  string $first
     * @param  string $operator
     * @param  string $second
     * @return $this
     */
    public function leftJoin($table, $first, $operator = null, $second = null)
    {
        return $this->join($table, $first, $operator, $second, 'left');
    }

    /**
     * Add a "join where" clause to the query.
     *
     * @param  string $table
     * @param  string $one
     * @param  string $operator
     * @param  string $two
     * @return $this
     */
    public function leftJoinWhere($table, $one, $operator, $two)
    {
        return $this->joinWhere($table, $one, $operator, $two, 'left');
    }

    /**
     * Add a right join to the query.
     *
     * @param  string $table
     * @param  string $first
     * @param  string $operator
     * @param  string $second
     * @return $this
     */
    public function rightJoin($table, $first, $operator = null, $second = null)
    {
        return $this->join($table, $first, $operator, $second, 'right');
    }

    /**
     * Add a "right join where" clause to the query.
     *
     * @param  string $table
     * @param  string $one
     * @param  string $operator
     * @param  string $two
     * @return $this
     */
    public function rightJoinWhere($table, $one, $operator, $two)
    {
        return $this->joinWhere($table, $one, $operator, $two, 'right');
    }

    /**
     * Add a basic where clause to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  mixed $value
     * @param  string $boolean
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        if (is_array($column)) {
            return $this->whereNested(function (Builder $query) use ($column) {
                foreach ($column as $key => $value) {
                    $query->where($key, '=', $value);
                }
            }, $boolean);
        }

        if (func_num_args() == 2) {
            list($value, $operator) = array($operator, '=');
        } elseif ($this->invalidOperatorAndValue($operator, $value)) {
            throw new \InvalidArgumentException("Value must be provided.");
        }

        if ($column instanceof \Closure) {
            return $this->whereNested($column, $boolean);
        }

        if (!in_array(strtolower($operator), $this->operators, true)) {
            list($value, $operator) = array($operator, '=');
        }

        if ($value instanceof \Closure) {
            return $this->whereSub($column, $operator, $value, $boolean);
        }

        if (is_null($value)) {
            return $this->whereNull($column, $boolean, $operator != '=');
        }

        $type = 'Basic';

        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');

        if (!$value instanceof Expression) {
            $this->addBinding($value, 'where');
        }

        return $this;
    }

    /**
     * Add an "or where" clause to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  mixed $value
     * @return $this
     */
    public function orWhere($column, $operator = null, $value = null)
    {
        return $this->where($column, $operator, $value, 'or');
    }

    /**
     * Determine if the given operator and value combination is legal.
     *
     * @param  string $operator
     * @param  mixed $value
     * @return bool
     */
    protected function invalidOperatorAndValue($operator, $value)
    {
        $isOperator = in_array($operator, $this->operators);

        return ($isOperator && $operator != '=' && is_null($value));
    }

    /**
     * Add a raw where clause to the query.
     *
     * @param  string $sql
     * @param  array $bindings
     * @param  string $boolean
     * @return $this
     */
    public function whereRaw($sql, array $bindings = array(), $boolean = 'and')
    {
        $type = 'raw';

        $this->wheres[] = compact('type', 'sql', 'boolean');

        $this->addBinding($bindings, 'where');

        return $this;
    }

    /**
     * Add a raw or where clause to the query.
     *
     * @param  string $sql
     * @param  array $bindings
     * @return $this
     */
    public function orWhereRaw($sql, array $bindings = array())
    {
        return $this->whereRaw($sql, $bindings, 'or');
    }

    /**
     * Add a where between statement to the query.
     *
     * @param  string $column
     * @param  array $values
     * @param  string $boolean
     * @param  bool $not
     * @return $this
     */
    public function whereBetween($column, array $values, $boolean = 'and', $not = false)
    {
        $type = 'between';

        $this->wheres[] = compact('column', 'type', 'boolean', 'not');

        $this->addBinding($values, 'where');

        return $this;
    }

    /**
     * Add an or where between statement to the query.
     *
     * @param  string $column
     * @param  array $values
     * @return $this
     */
    public function orWhereBetween($column, array $values)
    {
        return $this->whereBetween($column, $values, 'or');
    }

    /**
     * Add a where not between statement to the query.
     *
     * @param  string $column
     * @param  array $values
     * @param  string $boolean
     * @return $this
     */
    public function whereNotBetween($column, array $values, $boolean = 'and')
    {
        return $this->whereBetween($column, $values, $boolean, true);
    }

    /**
     * Add an or where not between statement to the query.
     *
     * @param  string $column
     * @param  array $values
     * @return $this
     */
    public function orWhereNotBetween($column, array $values)
    {
        return $this->whereNotBetween($column, $values, 'or');
    }

    /**
     * Add a nested where statement to the query.
     *
     * @param  \Closure $callback
     * @param  string $boolean
     * @return $this
     */
    public function whereNested(\Closure $callback, $boolean = 'and')
    {
        $query = $this->newQuery();

        $query->table($this->table);

        call_user_func($callback, $query);

        return $this->addNestedWhereQuery($query, $boolean);
    }

    /**
     * Add another query builder as a nested where to the query builder.
     *
     * @param $query
     * @param string $boolean
     * @return $this
     */
    public function addNestedWhereQuery($query, $boolean = 'and')
    {
        if (count($query->wheres)) {
            $type = 'Nested';

            $this->wheres[] = compact('type', 'query', 'boolean');

            $this->mergeBindings($query);
        }

        return $this;
    }

    /**
     * Add a full sub-select to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  \Closure $callback
     * @param  string $boolean
     * @return $this
     */
    protected function whereSub($column, $operator, \Closure $callback, $boolean)
    {
        $type = 'Sub';

        $query = $this->newQuery();

        call_user_func($callback, $query);

        $this->wheres[] = compact('type', 'column', 'operator', 'query', 'boolean');

        $this->mergeBindings($query);

        return $this;
    }

    /**
     * Add an exists clause to the query.
     *
     * @param callable|\Closure $callback
     * @param  string $boolean
     * @param  bool $not
     * @return $this
     */
    public function whereExists(\Closure $callback, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotExists' : 'Exists';

        $query = $this->newQuery();

        call_user_func($callback, $query);

        $this->wheres[] = compact('type', 'operator', 'query', 'boolean');

        $this->mergeBindings($query);

        return $this;
    }

    /**
     * Add an or exists clause to the query.
     *
     * @param  \Closure $callback
     * @param  bool $not
     * @return $this
     */
    public function orWhereExists(\Closure $callback, $not = false)
    {
        return $this->whereExists($callback, 'or', $not);
    }

    /**
     * Add a where not exists clause to the query.
     *
     * @param  \Closure $callback
     * @param  string $boolean
     * @return $this
     */
    public function whereNotExists(\Closure $callback, $boolean = 'and')
    {
        return $this->whereExists($callback, $boolean, true);
    }

    /**
     * Add a where not exists clause to the query.
     *
     * @param  \Closure $callback
     * @return $this
     */
    public function orWhereNotExists(\Closure $callback)
    {
        return $this->orWhereExists($callback, true);
    }

    /**
     * Add a "where in" clause to the query.
     *
     * @param  string $column
     * @param  mixed $values
     * @param  string $boolean
     * @param  bool $not
     * @return $this
     */
    public function whereIn($column, $values, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotIn' : 'In';

        if ($values instanceof \Closure) {
            return $this->whereInSub($column, $values, $boolean, $not);
        }

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');

        $this->addBinding($values, 'where');

        return $this;
    }

    /**
     * Add an "or where in" clause to the query.
     *
     * @param  string $column
     * @param  mixed $values
     * @return $this
     */
    public function orWhereIn($column, $values)
    {
        return $this->whereIn($column, $values, 'or');
    }

    /**
     * Add a "where not in" clause to the query.
     *
     * @param  string $column
     * @param  mixed $values
     * @param  string $boolean
     * @return $this
     */
    public function whereNotIn($column, $values, $boolean = 'and')
    {
        return $this->whereIn($column, $values, $boolean, true);
    }

    /**
     * Add an "or where not in" clause to the query.
     *
     * @param  string $column
     * @param  mixed $values
     * @return $this
     */
    public function orWhereNotIn($column, $values)
    {
        return $this->whereNotIn($column, $values, 'or');
    }

    /**
     * Add a where in with a sub-select to the query.
     *
     * @param  string $column
     * @param  \Closure $callback
     * @param  string $boolean
     * @param  bool $not
     * @return $this
     */
    protected function whereInSub($column, \Closure $callback, $boolean, $not)
    {
        $type = $not ? 'NotInSub' : 'InSub';

        call_user_func($callback, $query = $this->newQuery());

        $this->wheres[] = compact('type', 'column', 'query', 'boolean');

        $this->mergeBindings($query);

        return $this;
    }

    /**
     * Add a "where null" clause to the query.
     *
     * @param  string $column
     * @param  string $boolean
     * @param  bool $not
     * @return $this
     */
    public function whereNull($column, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotNull' : 'Null';

        $this->wheres[] = compact('type', 'column', 'boolean');

        return $this;
    }

    /**
     * Add an "or where null" clause to the query.
     *
     * @param  string $column
     * @return $this
     */
    public function orWhereNull($column)
    {
        return $this->whereNull($column, 'or');
    }

    /**
     * Add a "where not null" clause to the query.
     *
     * @param  string $column
     * @param  string $boolean
     * @return $this
     */
    public function whereNotNull($column, $boolean = 'and')
    {
        return $this->whereNull($column, $boolean, true);
    }

    /**
     * Add an "or where not null" clause to the query.
     *
     * @param  string $column
     * @return $this
     */
    public function orWhereNotNull($column)
    {
        return $this->whereNotNull($column, 'or');
    }

    /**
     * Add a "where day" statement to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  int $value
     * @param  string $boolean
     * @return $this
     */
    public function whereDay($column, $operator, $value, $boolean = 'and')
    {
        return $this->addDateBasedWhere('Day', $column, $operator, $value, $boolean);
    }

    /**
     * Add a "where month" statement to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  int $value
     * @param  string $boolean
     * @return $this
     */
    public function whereMonth($column, $operator, $value, $boolean = 'and')
    {
        return $this->addDateBasedWhere('Month', $column, $operator, $value, $boolean);
    }

    /**
     * Add a "where year" statement to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  int $value
     * @param  string $boolean
     * @return $this
     */
    public function whereYear($column, $operator, $value, $boolean = 'and')
    {
        return $this->addDateBasedWhere('Year', $column, $operator, $value, $boolean);
    }

    /**
     * Add a date based (year, month, day) statement to the query.
     *
     * @param  string $type
     * @param  string $column
     * @param  string $operator
     * @param  int $value
     * @param  string $boolean
     * @return $this
     */
    protected function addDateBasedWhere($type, $column, $operator, $value, $boolean = 'and')
    {
        $this->wheres[] = compact('column', 'type', 'boolean', 'operator', 'value');
        $this->addBinding($value, 'where');
        return $this;
    }


    /**
     * Add a "group by" clause to the query.
     *
     * @return $this
     */
    public function groupBy()
    {
        foreach (func_get_args() as $arg) {
            $this->groups = array_merge((array)$this->groups, is_array($arg) ? $arg : [$arg]);
        }

        return $this;
    }

    /**
     * Add a "having" clause to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  string $value
     * @param  string $boolean
     * @return $this
     */
    public function having($column, $operator = null, $value = null, $boolean = 'and')
    {
        $type = 'basic';
        $this->havings[] = compact('type', 'column', 'operator', 'value', 'boolean');
        $this->addBinding($value, 'having');
        return $this;
    }

    /**
     * Add a "or having" clause to the query.
     *
     * @param  string $column
     * @param  string $operator
     * @param  string $value
     * @return $this
     */
    public function orHaving($column, $operator = null, $value = null)
    {
        return $this->having($column, $operator, $value, 'or');
    }

    /**
     * Add a raw having clause to the query.
     *
     * @param  string $sql
     * @param  array $bindings
     * @param  string $boolean
     * @return $this
     */
    public function havingRaw($sql, array $bindings = array(), $boolean = 'and')
    {
        $type = 'raw';
        $this->havings[] = compact('type', 'sql', 'boolean');
        $this->addBinding($bindings, 'having');
        return $this;
    }

    /**
     * Add a raw or having clause to the query.
     *
     * @param  string $sql
     * @param  array $bindings
     * @return $this
     */
    public function orHavingRaw($sql, array $bindings = array())
    {
        return $this->havingRaw($sql, $bindings, 'or');
    }

    /**
     * Add an "order by" clause to the query.
     *
     * @param  string $column
     * @param  string $direction
     * @return $this
     */
    public function orderBy($column, $direction = 'asc')
    {
        $direction = strtolower($direction) == 'asc' ? 'asc' : 'desc';
        $this->orders[] = compact('column', 'direction');
        return $this;
    }

    /**
     * Add a raw "order by" clause to the query.
     *
     * @param  string $sql
     * @param  array $bindings
     * @return $this
     */
    public function orderByRaw($sql, $bindings = array())
    {
        $type = 'raw';
        $this->orders[] = compact('type', 'sql');
        $this->addBinding($bindings, 'order');
        return $this;
    }

    /**
     * Set the "offset" value of the query.
     *
     * @param  int $value
     * @return $this
     */
    public function offset($value)
    {
        $this->offset = max(0, $value);
        return $this;
    }

    /**
     * Set the "limit" value of the query.
     *
     * @param  int $value
     * @return $this
     */
    public function limit($value)
    {
        if ($value > 0) $this->limit = $value;

        return $this;
    }

    /**
     * Lock the selected rows in the table.
     *
     * @param  bool $value
     * @return $this
     */
    public function lock($value = true)
    {
        $this->lock = $value;

        return $this;
    }

    /**
     * Lock the selected rows in the table for updating.
     *
     * @return $this
     */
    public function lockForUpdate()
    {
        return $this->lock(true);
    }

    /**
     * Share lock the selected rows in the table.
     *
     * @return $this
     */
    public function sharedLock()
    {
        return $this->lock(false);
    }

    /**
     * Get the SQL representation of the query with bindings.
     *
     * @return array
     */
    public function toSql()
    {
        $method = 'toSql' . ucfirst($this->command);
        return $this->$method();
    }

    protected function toSqlSelect()
    {
        return [$this->grammar->compileSelect($this), $this->getBindings()];
    }

    protected function getBindings()
    {
        return self::flatten($this->bindings);
    }

    protected static function flatten($array)
    {
        $return = array();
        array_walk_recursive($array, function ($x) use (&$return) {
            $return[] = $x;
        });
        return $return;
    }

    /**
     * Retrieve the "count" result of the query.
     *
     * @param  string $columns
     * @return $this
     */
    public function count($columns = '*')
    {
        if (!is_array($columns)) {
            $columns = array($columns);
        }

        return $this->aggregate(__FUNCTION__, $columns);
    }

    /**
     * Retrieve the minimum value of a given column.
     *
     * @param  string $column
     * @return $this
     */
    public function min($column)
    {
        return $this->aggregate(__FUNCTION__, array($column));
    }

    /**
     * Retrieve the maximum value of a given column.
     *
     * @param  string $column
     * @return $this
     */
    public function max($column)
    {
        return $this->aggregate(__FUNCTION__, array($column));
    }

    /**
     * Retrieve the sum of the values of a given column.
     *
     * @param  string $column
     * @return $this
     */
    public function sum($column)
    {
        return $this->aggregate(__FUNCTION__, array($column));
    }

    /**
     * Retrieve the average of the values of a given column.
     *
     * @param  string $column
     * @return $this
     */
    public function avg($column)
    {
        return $this->aggregate(__FUNCTION__, array($column));
    }

    /**
     * Execute an aggregate function on the database.
     *
     * @param  string $function
     * @param  array $columns
     * @return $this
     */
    public function aggregate($function, $columns = array('*'))
    {
        $this->aggregate = compact('function', 'columns');
        $this->select($columns);
        return $this;
    }

    /**
     * Insert a new record into the database.
     *
     * @param  array $values
     * @return $this
     */
    public function insert(array $values)
    {
        $this->command('insert');
        $this->values = $values;
        return $this;
    }

    protected function toSqlInsert()
    {
        $values = $this->values;

        if (!is_array(reset($values))) {
            $values = array($values);
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        $bindings = array();

        foreach ($values as $record) {
            foreach ($record as $value) {
                $bindings[] = $value;
            }
        }

        return [$this->grammar->compileInsert($this, $values), $this->cleanBindings($bindings)];
    }

    /**
     * Update a record in the database.
     *
     * @param  array $values
     * @return $this
     */
    public function update(array $values)
    {
        $this->command('update');
        $this->values = $values;
        return $this;
    }

    protected function toSqlUpdate()
    {
        $values = $this->values;
        $bindings = array_values(array_merge($values, $this->getBindings()));
        return [$this->grammar->compileUpdate($this, $values), $this->cleanBindings($bindings)];
    }

    /**
     * Delete a record from the database.
     *
     * @param  mixed $id
     * @return $this
     */
    public function delete($id = null)
    {
        $this->command('delete');

        if (!is_null($id)) $this->where('id', '=', $id);

        return $this;
    }

    protected function toSqlDelete()
    {
        return [$this->grammar->compileDelete($this), $this->getBindings()];
    }

    /**
     * Remove all of the expressions from a list of bindings.
     *
     * @param  array $bindings
     * @return array
     */
    protected function cleanBindings(array $bindings)
    {
        return array_values(array_filter($bindings, function ($binding) {
            return !$binding instanceof Expression;
        }));
    }

    /**
     * Create a raw database expression.
     *
     * @param  mixed $value
     * @return Expression
     */
    public function raw($value)
    {
        return new Expression($value);
    }

    /**
     * Set the bindings on the query builder.
     *
     * @param  array $bindings
     * @param  string $type
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    protected function setBindings(array $bindings, $type = 'where')
    {
        if (!array_key_exists($type, $this->bindings)) {
            throw new \InvalidArgumentException("Invalid binding type: {$type}.");
        }

        $this->bindings[$type] = $bindings;

        return $this;
    }

    /**
     * Add a binding to the query.
     *
     * @param  mixed $value
     * @param  string $type
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function addBinding($value, $type = 'where')
    {
        if (!array_key_exists($type, $this->bindings)) {
            throw new \InvalidArgumentException("Invalid binding type: {$type}.");
        }

        if (is_array($value)) {
            $this->bindings[$type] = array_values(array_merge($this->bindings[$type], $value));
        } else {
            $this->bindings[$type][] = $value;
        }

        return $this;
    }

    /**
     * Merge an array of bindings into our bindings.
     *
     * @param  Builder $query
     * @return $this
     */
    protected function mergeBindings(Builder $query)
    {
        $this->bindings = array_merge_recursive($this->bindings, $query->bindings);

        return $this;
    }

    protected function newQuery()
    {
        return new self($this->grammar);
    }

    public function __toString()
    {
        return json_encode($this->toSql());
    }
}
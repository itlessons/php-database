<?php

namespace Database\Query;

class Grammar
{
    protected $selectComponents = array(
        'aggregate',
        'columns',
        'table',
        'joins',
        'wheres',
        'groups',
        'havings',
        'orders',
        'limit',
        'offset',
        'lock',
    );

    /**
     * Compile a select query into SQL.
     *
     * @param  Builder
     * @return string
     */
    public function compileSelect(Builder $query)
    {
        if (is_null($query->columns)) $query->columns = array('*');
        return trim($this->concatenate($this->compileComponents($query)));
    }

    /**
     * Compile the components necessary for a select clause.
     *
     * @param  Builder
     * @return array
     */
    protected function compileComponents(Builder $query)
    {
        $sql = array();

        foreach ($this->selectComponents as $component) {
            if (!is_null($query->$component)) {
                $method = 'compile' . ucfirst($component);
                $sql[$component] = $this->$method($query, $query->$component);
            }
        }

        return $sql;
    }

    /**
     * Compile an aggregated select clause.
     *
     * @param  Builder $query
     * @param  array $aggregate
     * @return string
     */
    protected function compileAggregate(Builder $query, $aggregate)
    {
        $column = $this->columnize($aggregate['columns']);

        if ($query->distinct && $column !== '*') {
            $column = 'distinct ' . $column;
        }

        return 'select ' . $aggregate['function'] . '(' . $column . ') as aggregate';
    }

    /**
     * Compile the "select *" portion of the query.
     *
     * @param  Builder $query
     * @param  array $columns
     * @return string|null
     */
    protected function compileColumns(Builder $query, $columns)
    {
        if (!is_null($query->aggregate)) return;

        $select = $query->distinct ? 'select distinct ' : 'select ';

        return $select . $this->columnize($columns);
    }

    /**
     * Compile the "table" portion of the query.
     *
     * @param  Builder $query
     * @param  string $table
     * @return string
     */
    protected function compileTable(Builder $query, $table)
    {
        return 'from ' . $this->wrapTable($table);
    }

    /**
     * Compile the "join" portions of the query.
     *
     * @param  Builder $query
     * @param  array $joins
     * @return string
     */
    protected function compileJoins(Builder $query, $joins)
    {
        $sql = array();

        foreach ($joins as $join) {
            $table = $this->wrapTable($join->table);
            $clauses = array();

            foreach ($join->clauses as $clause) {
                $clauses[] = $this->compileJoinConstraint($clause);
            }

            $clauses[0] = $this->removeLeadingBoolean($clauses[0]);
            $clauses = implode(' ', $clauses);
            $type = $join->type;
            $sql[] = "$type join $table on $clauses";
        }

        return implode(' ', $sql);
    }

    /**
     * Create a join clause constraint segment.
     *
     * @param  array $clause
     * @return string
     */
    protected function compileJoinConstraint(array $clause)
    {
        $first = $this->wrap($clause['first']);

        $second = $clause['where'] ? '?' : $this->wrap($clause['second']);

        return "{$clause['boolean']} $first {$clause['operator']} $second";
    }

    /**
     * Compile the "where" portions of the query.
     *
     * @param  Builder $query
     * @return string
     */
    protected function compileWheres(Builder $query)
    {
        $sql = array();

        if (is_null($query->wheres)) return '';

        foreach ($query->wheres as $where) {
            $method = "where{$where['type']}";

            $sql[] = $where['boolean'] . ' ' . $this->$method($query, $where);
        }

        if (count($sql) > 0) {
            $sql = implode(' ', $sql);

            return 'where ' . preg_replace('/and |or /', '', $sql, 1);
        }

        return '';
    }

    /**
     * Compile a nested where clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereNested(Builder $query, $where)
    {
        $nested = $where['query'];

        return '(' . substr($this->compileWheres($nested), 6) . ')';
    }

    /**
     * Compile a where condition with a sub-select.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereSub(Builder $query, $where)
    {
        $select = $this->compileSelect($where['query']);

        return $this->wrap($where['column']) . ' ' . $where['operator'] . " ($select)";
    }

    /**
     * Compile a basic where clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereBasic(Builder $query, $where)
    {
        $value = $this->parameter($where['value']);

        return $this->wrap($where['column']) . ' ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a "between" where clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereBetween(Builder $query, $where)
    {
        $between = $where['not'] ? 'not between' : 'between';

        return $this->wrap($where['column']) . ' ' . $between . ' ? and ?';
    }

    /**
     * Compile a where exists clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereExists(Builder $query, $where)
    {
        return 'exists (' . $this->compileSelect($where['query']) . ')';
    }

    /**
     * Compile a where exists clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereNotExists(Builder $query, $where)
    {
        return 'not exists (' . $this->compileSelect($where['query']) . ')';
    }

    /**
     * Compile a "where in" clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereIn(Builder $query, $where)
    {
        $values = $this->parameterize($where['values']);

        return $this->wrap($where['column']) . ' in (' . $values . ')';
    }

    /**
     * Compile a "where not in" clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereNotIn(Builder $query, $where)
    {
        $values = $this->parameterize($where['values']);

        return $this->wrap($where['column']) . ' not in (' . $values . ')';
    }

    /**
     * Compile a where in sub-select clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereInSub(Builder $query, $where)
    {
        $select = $this->compileSelect($where['query']);

        return $this->wrap($where['column']) . ' in (' . $select . ')';
    }

    /**
     * Compile a where not in sub-select clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereNotInSub(Builder $query, $where)
    {
        $select = $this->compileSelect($where['query']);

        return $this->wrap($where['column']) . ' not in (' . $select . ')';
    }

    /**
     * Compile a "where null" clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereNull(Builder $query, $where)
    {
        return $this->wrap($where['column']) . ' is null';
    }

    /**
     * Compile a "where not null" clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereNotNull(Builder $query, $where)
    {
        return $this->wrap($where['column']) . ' is not null';
    }

    /**
     * Compile a "where day" clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereDay(Builder $query, $where)
    {
        return $this->dateBasedWhere('day', $query, $where);
    }

    /**
     * Compile a "where month" clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereMonth(Builder $query, $where)
    {
        return $this->dateBasedWhere('month', $query, $where);
    }

    /**
     * Compile a "where year" clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereYear(Builder $query, $where)
    {
        return $this->dateBasedWhere('year', $query, $where);
    }

    /**
     * Compile a date based where clause.
     *
     * @param  string $type
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function dateBasedWhere($type, Builder $query, $where)
    {
        $value = $this->parameter($where['value']);

        return $type . '(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    /**
     * Compile a raw where clause.
     *
     * @param  Builder $query
     * @param  array $where
     * @return string
     */
    protected function whereRaw(Builder $query, $where)
    {
        return $where['sql'];
    }

    /**
     * Compile the "group by" portions of the query.
     *
     * @param  Builder $query
     * @param  array $groups
     * @return string
     */
    protected function compileGroups(Builder $query, $groups)
    {
        return 'group by ' . $this->columnize($groups);
    }

    /**
     * Compile the "having" portions of the query.
     *
     * @param  Builder $query
     * @param  array $havings
     * @return string
     */
    protected function compileHavings(Builder $query, $havings)
    {
        $sql = implode(' ', array_map(array($this, 'compileHaving'), $havings));

        return 'having ' . preg_replace('/and /', '', $sql, 1);
    }

    /**
     * Compile a single having clause.
     *
     * @param  array $having
     * @return string
     */
    protected function compileHaving(array $having)
    {
        if ($having['type'] === 'raw') {
            return $having['boolean'] . ' ' . $having['sql'];
        }

        return $this->compileBasicHaving($having);
    }

    /**
     * Compile a basic having clause.
     *
     * @param  array $having
     * @return string
     */
    protected function compileBasicHaving($having)
    {
        $column = $this->wrap($having['column']);

        $parameter = $this->parameter($having['value']);

        return $having['boolean'] . ' ' . $column . ' ' . $having['operator'] . ' ' . $parameter;
    }

    /**
     * Compile the "order by" portions of the query.
     *
     * @param  Builder $query
     * @param  array $orders
     * @return string
     */
    protected function compileOrders(Builder $query, $orders)
    {
        return 'order by ' . implode(', ', array_map(function ($order) {
                if (isset($order['sql'])) return $order['sql'];

                return $this->wrap($order['column']) . ' ' . $order['direction'];
            }
            , $orders));
    }

    /**
     * Compile the "limit" portions of the query.
     *
     * @param  Builder $query
     * @param  int $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit)
    {
        return 'limit ' . (int)$limit;
    }

    /**
     * Compile the "offset" portions of the query.
     *
     * @param  Builder $query
     * @param  int $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
        return 'offset ' . (int)$offset;
    }

    /**
     * Compile the "union" queries attached to the main query.
     *
     * @param  Builder $query
     * @return string
     */
    protected function compileUnions(Builder $query)
    {
        $sql = '';

        foreach ($query->unions as $union) {
            $sql .= $this->compileUnion($union);
        }

        return ltrim($sql);
    }

    /**
     * Compile a single union statement.
     *
     * @param  array $union
     * @return string
     */
    protected function compileUnion(array $union)
    {
        $joiner = $union['all'] ? ' union all ' : ' union ';

        return $joiner . $union['query']->toSql();
    }

    /**
     * Compile an insert statement into SQL.
     *
     * @param  Builder $query
     * @param  array $values
     * @return string
     */
    public function compileInsert(Builder $query, array $values)
    {
        $table = $this->wrapTable($query->table);

        if (!is_array(reset($values))) {
            $values = array($values);
        }

        $columns = $this->columnize(array_keys(reset($values)));

        $parameters = $this->parameterize(reset($values));

        $value = array_fill(0, count($values), "($parameters)");

        $parameters = implode(', ', $value);

        return "insert into $table ($columns) values $parameters";
    }

    /**
     * Compile an insert and get ID statement into SQL.
     *
     * @param  Builder $query
     * @param  array $values
     * @param  string $sequence
     * @return string
     */
    public function compileInsertGetId(Builder $query, $values, $sequence)
    {
        return $this->compileInsert($query, $values);
    }

    /**
     * Compile an update statement into SQL.
     *
     * @param  Builder $query
     * @param  array $values
     * @return string
     */
    public function compileUpdate(Builder $query, $values)
    {
        $table = $this->wrapTable($query->table);

        $columns = array();

        foreach ($values as $key => $value) {
            $columns[] = $this->wrap($key) . ' = ' . $this->parameter($value);
        }

        $columns = implode(', ', $columns);

        if (isset($query->joins)) {
            $joins = ' ' . $this->compileJoins($query, $query->joins);
        } else {
            $joins = '';
        }

        $where = $this->compileWheres($query);

        $sql = trim("update {$table}{$joins} set $columns $where");

        if (isset($query->orders)) {
            $sql .= ' ' . $this->compileOrders($query, $query->orders);
        }
        if (isset($query->limit)) {
            $sql .= ' ' . $this->compileLimit($query, $query->limit);
        }

        return rtrim($sql);
    }

    /**
     * Compile a delete statement into SQL.
     *
     * @param  Builder $query
     * @return string
     */
    public function compileDelete(Builder $query)
    {
        $table = $this->wrapTable($query->table);

        $where = is_array($query->wheres) ? $this->compileWheres($query) : '';

        return trim("delete from $table " . $where);
    }

    /**
     * Compile a truncate table statement into SQL.
     *
     * @param  Builder $query
     * @return array
     */
    public function compileTruncate(Builder $query)
    {
        return array('truncate ' . $this->wrapTable($query->table) => array());
    }

    /**
     * Compile the lock into SQL.
     *
     * @param  Builder $query
     * @param  bool|string $value
     * @return string
     */
    protected function compileLock(Builder $query, $value)
    {
        if (is_string($value)) return $value;
        return $value ? 'for update' : 'lock in share mode';
    }

    /**
     * Concatenate an array of segments, removing empties.
     *
     * @param  array $segments
     * @return string
     */
    protected function concatenate($segments)
    {
        return implode(' ', array_filter($segments, function ($value) {
            return (string)$value !== '';
        }));
    }

    /**
     * Remove the leading boolean from a statement.
     *
     * @param  string $value
     * @return string
     */
    protected function removeLeadingBoolean($value)
    {
        return preg_replace('/and |or /', '', $value, 1);
    }

    /**
     * /**
     * Wrap an array of values.
     *
     * @param  array $values
     * @return array
     */
    public function wrapArray(array $values)
    {
        return array_map(array($this, 'wrap'), $values);
    }

    /**
     * Wrap a table in keyword identifiers.
     *
     * @param  string $table
     * @return string
     */
    public function wrapTable($table)
    {
        if ($this->isExpression($table)) return $this->getValue($table);

        return $this->wrap($table);
    }

    /**
     * Wrap a value in keyword identifiers.
     *
     * @param  string $value
     * @return string
     */
    public function wrap($value)
    {
        if ($this->isExpression($value)) return $this->getValue($value);

        if (strpos(strtolower($value), ' as ') !== false) {
            $segments = explode(' ', $value);

            return $this->wrap($segments[0]) . ' as ' . $this->wrap($segments[2]);
        }

        $wrapped = array();

        $segments = explode('.', $value);

        foreach ($segments as $key => $segment) {
            if ($key == 0 && count($segments) > 1) {
                $wrapped[] = $this->wrapTable($segment);
            } else {
                $wrapped[] = $this->wrapValue($segment);
            }
        }

        return implode('.', $wrapped);
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param  string $value
     * @return string
     */
    protected function wrapValue($value)
    {
        if ($value === '*') return $value;
        return '`' . str_replace('`', '``', $value) . '`';
    }

    /**
     * Convert an array of column names into a delimited string.
     *
     * @param  array $columns
     * @return string
     */
    public function columnize(array $columns)
    {
        return implode(', ', array_map(array($this, 'wrap'), $columns));
    }

    /**
     * Create query parameter place-holders for an array.
     *
     * @param  array $values
     * @return string
     */
    public function parameterize(array $values)
    {
        return implode(', ', array_map(array($this, 'parameter'), $values));
    }

    /**
     * Get the appropriate query parameter place-holder for a value.
     *
     * @param  mixed $value
     * @return string
     */
    public function parameter($value)
    {
        return $this->isExpression($value) ? $this->getValue($value) : '?';
    }

    /**
     * Get the value of a raw expression.
     *
     * @param  Expression $expression
     * @return string
     */
    public function getValue(Expression $expression)
    {
        return $expression->getValue();
    }

    /**
     * Determine if the given value is a raw expression.
     *
     * @param  mixed $value
     * @return bool
     */
    public function isExpression($value)
    {
        return $value instanceof Expression;
    }

    /**
     * Get the format for database stored dates.
     *
     * @return string
     */
    public function getDateFormat()
    {
        return 'Y-m-d H:i:s';
    }
} 
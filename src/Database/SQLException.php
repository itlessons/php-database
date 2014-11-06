<?php

namespace Database;

class SQLException extends \Exception
{
    const CODE_SUCCESS = '00000';
    const CODE_CONSTRAINT_VIOLATION = '23000';

    public function __construct($errorInfo, $query, $bindings)
    {
        $this->errorInfo = $errorInfo;

        $message = sprintf('SQLException: %s/%s. Query: %s. Binds: %s. %s',
            $errorInfo[2],
            $errorInfo[0],
            $query,
            self::dataToString($bindings),
            $errorInfo[1]);

        parent::__construct($message, $errorInfo[1]);
    }

    private static function dataToString($data)
    {
        if (null === $data || is_scalar($data)) {
            return $data;
        }

        if (is_array($data) || $data instanceof \Traversable) {
            $normalized = array();

            foreach ($data as $key => $value) {
                $normalized[$key] = self::dataToString($value);
            }

            return self::toJson($normalized);
        }

        if (is_object($data)) {
            return sprintf("[object] (%s: %s)", get_class($data), self::toJson($data));
        }

        if (is_resource($data)) {
            return '[resource]';
        }

        return '[unknown(' . gettype($data) . ')]';
    }

    private static function toJson($data)
    {
        if (version_compare(PHP_VERSION, '5.4.0', '>=')) {
            return json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        }

        return json_encode($data);
    }
} 
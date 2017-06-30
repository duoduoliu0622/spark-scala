<?php

namespace App\Model\Behavior\Queries;

class RowQuoting
{
    public function doubleQuote(array $row = [])
    {
        $tmp = [];

        foreach ($row as $value) {
            if (is_numeric($value)) {
                $tmp[] = $value;
                continue;
            }

            $value = preg_replace('/"/', "'", $value);
            $tmp[] = '"' . $value . '"';
        }

        return $tmp;
    }
}
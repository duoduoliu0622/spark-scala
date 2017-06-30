<?php

namespace App\Model\Behavior;

class Credentials
{
    protected $ini;

    /**
     * Credentials constructor.
     */
    public function __construct($ini)
    {
        $this->ini = $ini;
    }

    public function get($section, $field = null)
    {
        $data = parse_ini_file($this->ini, true);

        if(empty($field)) {
            return $data[$section];
        }

        if (isset($data[$section][$field])) {
            return $data[$section][$field];
        }

        throw new \Exception("Can NOT find $section - $field in: " . $this->ini);
    }
}

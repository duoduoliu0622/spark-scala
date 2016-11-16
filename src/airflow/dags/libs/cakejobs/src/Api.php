<?php

require_once dirname(__DIR__) . DS . "config/container.php";

class Api{
    public static function get($containerId)
    {
        global $app;

        if (isset($app[$containerId])) {
            return $app[$containerId];
        }

        throw new \Exception("Can NOT find container id: $containerId");
    }
}


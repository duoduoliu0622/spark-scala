<?php

use App\Model\Behavior\Credentials;
use App\Model\Behavior\Queries\QueryEngine;
use App\Model\Behavior\Queries\RowQuoting;
use Pimple\Container;

$app = new Container();

$app['mysql_username'] = 'root';

$app['curl'] = function () {
    return new Curl\Curl();
};

$app['credentials'] = function () {
    $ini = "/data/credentials.ini";
    return new Credentials($ini);
};

$app['query.engine'] = function () {
    return new QueryEngine();
};

$app['query.row.quoting'] = function () {
    return new RowQuoting();
};

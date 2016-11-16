<?php

touch("/tmp/php_touched.txt") or die("touch failed...");

$dbh = new PDO('mysql:host=localhost;dbname=test', $user, $pass);

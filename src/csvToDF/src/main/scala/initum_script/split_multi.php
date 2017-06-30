<?php

ini_set('memory_limit', '-1');

if (count($argv) < 3) die("Usage: php split_multi.php old.csv new.csv" . "\n");

$oldFile = $argv[1];
$newFile = $argv[2];

$lines = file($oldFile);

$newLines = [];

foreach ($lines as $line) {
    $_line = preg_replace(['/"/', '/;/'], ['', PHP_EOL], $line);
    $newLines[] = $_line;
}

file_put_contents($newFile, $newLines);


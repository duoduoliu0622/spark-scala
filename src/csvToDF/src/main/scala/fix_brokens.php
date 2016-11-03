<?php

ini_set('memory_limit', '-1');

if (count($argv) < 3) die("Usage: php fixer.php old.csv new.csv" . "\n");

$oldFile = $argv[1];
$newFile = $argv[2];

$newLines = [];

$handle = fopen($oldFile, "r") or die("Couldn't get handle");
while (!feof($handle)) {
   $line = fgets($handle);
    if (preg_match('/^("\d)/', $line)) {
        $newLines[] = $line;
        continue;
    }
        $lastNewLine = array_pop($newLines);
        $lastNewLine = trim($lastNewLine, PHP_EOL) . trim($line, PHP_EOL);
        $newLines[] = $lastNewLine;
}
fclose($handle);

file_put_contents($newFile, implode('', $newLines));




/*

#!/bin/bash
  2
  3 cat not_live.csv | awk -F'\t' 'NR>1 {print $2}' >not_live_name.txt
  4
  5 names=`cat not_live_name.txt`
  6
  7 for name in $names; do
  8     if [ "x$name" = "x0" ];then
  9         continue
 10     fi
 11     echo $name
 12     grep $name s6export-fix.csv >>matches.csv
 13 done
 14
 15 echo "done."
 16 rm -f not_live_name.txt
 17

 */

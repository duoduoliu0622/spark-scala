<?php

ini_set('memory_limit', '-1');

if (count($argv) < 3) die("Usage: php generate_fields.php input.txt  fields.csv" . "\n");

$oldFile = $argv[1];
$newFile = $argv[2];

$newLines = [];

$lines = file($oldFile);

foreach($lines as $line) {
    $fields = explode('";"', $line);
    echo count($fields) . PHP_EOL;

    foreach ($fields as $index => $field) {
        $newLines[] = $index . ";" . preg_replace('/"/', '', $field);
    }
}

file_put_contents($newFile, implode("\n", $newLines));




/*
     0    "<ID>"
     1    "<Name>"
     2    "<Parent ID>"
     3    "<Object Type Name>"
   184    "Colour"
   185    "Colour"
   186    "Colour"
   187    "Colour"
   188    "Colour"
   189    "Colour No Display"
   190    "Colour Number"
   191    "Colour/finish"
   192    "Colour/pattern"
   453    "Image Sources"
   671    "Primary Image ID"
   793    "Supplier Colour Name"
   795    "Swatch Reference"
   921    "WPMMediaCurrent"
   930    "Alternate Image 1 Asset Reference ID"
   931    "Alternate Image 10 Asset Reference ID"
   932    "Alternate Image 11 Asset Reference ID"
   933    "Alternate Image 12 Asset Reference ID"
   934    "Alternate Image 2 Asset Reference ID"
   935    "Alternate Image 3 Asset Reference ID"
   936    "Alternate Image 4 Asset Reference ID"
   937    "Alternate Image 5 Asset Reference ID"
   938    "Alternate Image 6 Asset Reference ID"
   939    "Alternate Image 7 Asset Reference ID"
   940    "Alternate Image 8 Asset Reference ID"
   941    "Alternate Image 9 Asset Reference ID"
   942    "Primary Image Asset Reference ID"
   943    "Swatch Image Asset Reference ID"
 */

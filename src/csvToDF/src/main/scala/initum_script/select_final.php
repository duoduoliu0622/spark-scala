<?php

ini_set('memory_limit', '-1');

if (count($argv) < 3) die("Usage: php select_final.php old.csv new.csv" . "\n");

$oldFile = $argv[1];
$newFile = $argv[2];

//$indexes = [0,1,2,3,184,185,186,187,188,189,190,191,192,453,671,793,795,921,930,931,932,933,934,935,936,937,938,939,940,941,942,943];
$indexes = [0,1,2,3,103,313,378,475,476,477,478,479,480,481,482,483,484,485,486,493,494];

$newLines = [];

$lines = file($oldFile);

foreach($lines as $line) {
    $arr = explode('";"', $line);
    echo count($arr) . PHP_EOL;
    $newArr = [];

    foreach ($indexes as $index) {
        $newArr[] = isset($arr[$index]) ? $arr[$index] : "";
    }

    $newLines[] = implode('";"', $newArr) . '"';
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



/*  new file
     0    "<ID>"
     1    "<Name>"
     2    "<Parent ID>"
     3    "<Object Type Name>"
   103    "Colour"
   313    "Primary Image ID"
   378    "Supplier Colour Name"
   475    "Alternate Image 1 Asset Reference ID"
   476    "Alternate Image 10 Asset Reference ID"
   477    "Alternate Image 11 Asset Reference ID"
   478    "Alternate Image 12 Asset Reference ID"
   479    "Alternate Image 2 Asset Reference ID"
   480    "Alternate Image 3 Asset Reference ID"
   481    "Alternate Image 4 Asset Reference ID"
   482    "Alternate Image 5 Asset Reference ID"
   483    "Alternate Image 6 Asset Reference ID"
   484    "Alternate Image 7 Asset Reference ID"
   485    "Alternate Image 8 Asset Reference ID"
   486    "Alternate Image 9 Asset Reference ID"
   493    "Primary Image Asset Reference ID"
   494    "Swatch Image Asset Reference ID"
 */

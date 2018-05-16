<?php
//Open the file.
$fileHandle = fopen("11.csv", "r");

$A = array();
$B = array();
$result = array();
//Loop through the CSV rows.
while (($row = fgetcsv($fileHandle, 0, ",")) !== FALSE) {
    //Dump out the row for the sake of clarity.
    $A[] = $row[0];
    $B[] = $row[2];
    echo "-";
}

foreach($A as $a){
    if(in_array($a, $B)){
        $C[] = $a;
        echo $a.'';
    }
}

$file = fopen("output_unique.csv","w");

foreach ($C as $c)
  {
  fputcsv($file,explode(',',$c));
  }

fclose($file);

echo "Finish...\n";
echo count($C);
<?php
use lib\Database\Database,
    Core\Config;
require_once dirname( __FILE__ ) . '/../../core/core.php';
Config::loadCustom('/etc/putong/putong-statistics-dashboard/config.ini');
$dbstats = Database::getDb(DB_STATS);

//***************************************************************
//Here are the things you need to modify:
//**************************************************************
$query = "SELECT
                *
          FROM
              reports.monthly_retention
          WHERE
              created_month >= '2017-01-01'
";

$outputFileName = "testCSV.csv";//Don't overwrite other's file!!
$header  = array('created_week','active_week','col1','col2','col3',);
//**************************************************************
//**************************************************************
$outputFileDirectory = "./shareFile/";
print_r(date("Y-m-d H:i:s"));
print_r("\nRetrive data...\n");
$content = $dbstats->getSql( $query );
if(empty($content)){
    print("Empty array, nothing to write to file...\n");
}else{
    print_r("Writing to file: ".$outputFileDirectory.$outputFileName." \n");
    try{
        $outputFile = fopen($outputFileDirectory.$outputFileName, "w");
        fputcsv($outputFile, $header,',','"');
        $numWrite = 0;
        foreach($content as $line){
            fputcsv($outputFile, $line,',','"');
            $numWrite++;
        }
        print_r(sizeof($content)." rows returnd      ");
        print_r($numWrite." rows writed\n");
    }catch(Exception $e){
        echo 'Caught exception: ',  $e->getMessage(), "\n";
    }finally{
        fclose($outputFile);
    }
}

print_r(date("Y-m-d H:i:s"));
echo("    DONE...\n");
print_r("\nFile link:\n");
print_r("https://stats.p1staff.com/tantan/shareFile/".$outputFileName);
echo("\n");
?>
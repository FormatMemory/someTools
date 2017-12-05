<?php
use lib\Database\Database,
    Core\Config;
require_once dirname( __FILE__ ) . '/../../core/core.php';
Config::loadCustom('/etc/putong/putong-statistics-dashboard/config.ini');
$dbstats = Database::getDb(DB_STATS);



function writeToCSV($query, $outputFileName, $header){  
    $dbstats = Database::getDb(DB_STATS);   
    $outputFileDirectory = "./";
    print_r(date("Y-m-d H:i:s"));
    print_r("\nRetrive data...\n");
    $content = $dbstats->getSql( $query );
    if(empty($content)){
        print("Empty array, nothing to write to file...\n");
        return 0;
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
        return 1;
    }

    print_r(date("Y-m-d H:i:s"));
    echo("    DONE...\n");
    //print_r("\nFile link:\n");
    //print_r("https://stats.p1staff.com/tantan/shareFile/".$outputFileName);
    echo("\n");
}


//***************************************************************
//Here are the things you need to modify:
//**************************************************************
$cityList = array('Beijing','Guangzhou','Shenzhen','Shanghai','Chengdu','Chongqing','Xi\'\'an','Suzhou,Jiangsu','Zhengzhou',
'Hangzhou','Tianjin','Wuhan','Kunming','Nanjing','Changsha',
'Shenyang','Qingdao','Harbin','Changchun','Dalian','Jinan',
'Guiyang','Foshan','Wulumuqi','Ningbo','Shijiazhuang',
'Nanning','Wenzhou','Wuxi','Hefei');

$genders = array('male','female');
$count = 0;
foreach($genders AS $gender){
    foreach($cityList AS $city){
        $query = "SELECT
                            user_id,
                            gender,
                            city_name,
                            age,
                            received_likes
                    FROM
                            kevin.user_gender_received_likes
                    WHERE
                            city_name = '".$city."'
                    AND gender = '".$gender."'
                    AND received_likes IS NOT NULL
                    ORDER BY received_likes DESC
                    LIMIT 100
        ";
        if(strcmp($city, 'Xi\'\'an') == 0 ){
            $city = 'XiAn';
        }
        if(strcmp($city, 'Suzhou,Jiangsu') == 0 ){
            $city = 'Suzhou_Jiangsu';
        }
        $outputFileName = "top100ReceiveLikesNumActiveUsers_".$city."_".$gender.".csv";//Don't overwrite other's file!!
        $header  = array('user_id','gender','city_name','age','received_likes',);
        //echo $query."\n";
        echo $outputFileName."\n\n";
        $count += writeToCSV($query, $outputFileName, $header);
    }
}
echo "\n\n\n".$count." files have been created...";

//**************************************************************
//**************************************************************
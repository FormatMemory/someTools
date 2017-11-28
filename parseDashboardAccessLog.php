<?php
use lib\Database\Database,
    Core\Config;
require_once dirname( __FILE__ ) . '/../../../core/core.php';
Config::loadCustom('/etc/putong/putong-statistics-dashboard/config.ini');
parse_str(implode('&', array_slice($argv, 1)), $_GET);
$dbstats = Database::getDb(DB_STATS);
$filePathHead = "/tmp/stats-access-log/stats.access.log-";
$filePathHead = "/Users/yusheng/WebServer/www/stats/src/scripts/tantan/log/accessLog/stats.access.log-";
$filePathTail = ".gz";
$tableName = "yay.dashboard_access_log";
$latestDate = $dbstats->getOneNumber( "SELECT date_time FROM ".$tableName." ORDER BY date_time DESC LIMIT 1" );

function get_string_between($string, $start, $end){
    $string = " ".$string;
    $ini = strpos($string,$start);
    if ($ini == 0) return "";
    $ini += strlen($start);
    $len = strpos($string,$end,$ini) - $ini;
    return substr($string,$ini,$len);
}

if( empty( $latestDate ) )
{
    $begin = new DateTime( '2017-11-21' );
}
else
{
    if(DATE( 'Y-m-d', strtotime($latestDate, time()) ) == DATE( 'Y-m-d', strtotime('-1 day', time()) ))
    {
        exit();
    }
    $begin = new DateTime( $latestDate );
    $begin = $begin->modify( '+1 day' );
}
$end = new DateTime( DATE( 'Y-m-d' ) );
$interval = new DateInterval('P1D');
$daterange = new DatePeriod($begin, $interval ,$end);
foreach($daterange as $fileDate)
{
    $pushArray = array();
    $fileFullPath = $filePathHead.$fileDate->format('Y-m-d').$filePathTail;
    while(!is_file($fileFullPath)){
            echo "No file ".$fileFullPath."\n";
        sleep(60);
    }
    $file = new SplFileObject("compress.zlib://".$fileFullPath);
    $i = 0;
    while (!$file->eof()){
        $row = $file->fgets();
        if( strpos( $row,'stats.p1staff.com' )  === false ) {
            continue;
        }

        $newRow = explode(' ', $row, 3);
        list($host, $ip, $content) = $newRow;
        $dateTime = get_string_between($row, ' [', ' +0800]');
        $dateTime = DATE( 'Y-m-d H:i:s', strToTime($dateTime. ' +0800'));
        $userName = get_string_between($row, ' - ', ' [');
        $finalStateStr = get_string_between($row, '/ HTTP/1.', ' ');
        preg_match('/([0-9]+\s[0-9]+\s\")/', $row, $matches);
        $finalStateAry = explode(' ',  $matches[0]);
        $finalState = intval($finalStateAry[0]);
        $refererPage = get_string_between($row, '"https://', '"');
        $userAgent = get_string_between($row, '" "', '" ');
        preg_match('/([0-9]+\s[0-9]+)\n/', $row, $matches);
        list($receivedBytes, $sentBytes) = explode(' ',  $matches[0]);

        if($userName == '-'){
            $userName = NULL;
        }
        if(empty($refererPage)){
            $refererPage = NULL;
        }

        $pushArray[] = array(
                $dateTime,
                $ip,
                $userName,
                $finalState,
                $refererPage,
                $userAgent,
                intval($receivedBytes),
                intval($sentBytes),
        );
        $i++;
        
        if( $i >= 10000 )
        {
            $dbstats->insertOnConflictIgnore($pushArray, array(
                                        'date_time',
                                        'ip',
                                        'user_name',
                                        'final_state',
                                        'referer_page',
                                        'user_agent',
                                        'received_bytes',
                                        'sent_bytes',
                                                    ),
                        $tableName,
            true);
            $pushArray = array();
            $i = 0;
        }
    }
    if(!empty( $pushArray) )
    {
        $dbstats->insertOnConflictIgnore($pushArray, array(
                                        'date_time',
                                        'ip',
                                        'user_name',
                                        'final_state',
                                        'referer_page',
                                        'user_agent',
                                        'received_bytes',
                                        'sent_bytes',
                                        ),
                $tableName,
	true);
    }
}



// CREATE TABLE IF NOT EXISTS yay.dashboard_access_log(
//     date_time          timestamp without time zone  not null,
//     ip                 inet,
//     user_name          character varying(50) default null,
//     final_state        integer,
//     referer_page       text,
//     user_agent         text,
//     received_bytes     integer,
//     sent_bytes         integer
//    )
//
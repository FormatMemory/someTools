<?php
use lib\Database\Database,
    Core\Config;

require_once dirname( __FILE__ ) . '/../../core/core.php';

Config::loadCustom('/etc/putong/putong-statistics-dashboard/config.ini');

        if (! function_exists('pcntl_fork'))
            die('PCNTL functions not available on this PHP installation');

parse_str(implode('&', array_slice($argv, 1)), $_GET);
function process($x)
{
            $sourceTable = "tbh_feeds";
            $targetTable = "stats.tbh_feeds";
            $conf = Config::get( 'DB_MAIN_SHARD_'.($x+1) );
            $db = Database::getDb('DB_MAIN_SHARD_'.($x+1), false );
            $dbstats = Database::getDb(DB_STATS, false);

            foreach( range( $conf['SHARD_START'], $conf['SHARD_STOP'] ) as $shard )
            //foreach( range( 1, 1 ) as $shard )
            {

                $query = "SELECT
                                id,
                                user_id,
                                poll_id,
                                voted_user_id,
                                voted_time,
                                created_time + '8 hours'::interval AS created_time,
                                updated_time + '8 hours'::interval AS updated_time,
                                status
                        FROM
                              rel_8192_".$shard.".".$sourceTable;

                    $dBData = $db->getSql( $query );
                    if(!empty($dBData))
                    {
                        $dbstats->insert($dBData, array(
                                         'id',
                                         'user_id',
                                         'poll_id',
                                         'voted_user_id',
                                         'voted_time',
                                         'created_time',
                                         'updated_time',
                                         'status',
                                ),
                                $targetTable);
                    }
                    echo ("rel_8192_".$shard.".".$sourceTable." --> ".$targetTable."\n");
            }
    exit();
}

$pid_arr  = array();


for ( $x = 0; $x < Config::get( 'SHARD_SERVERS'); $x++ )
//for ( $x = 0; $x < 3; $x++ )
{
    switch ($pid = pcntl_fork())
    {
        case -1:
            // @fail
            die('Fork failed');
            break;

        case 0:
            // child
                process($x);
            break;

        default:
            array_push($pid_arr,$pid);
            break;
    }
}

while( 0 < count( $pid_arr ) )
{
    $myId = pcntl_waitpid(-1, $status, WNOHANG);
    foreach( $pid_arr as $key => $pid )
    {
            if( $myId == $pid )
            {
                unset( $pid_arr[$key] );
            }
    }

    usleep(100);
}
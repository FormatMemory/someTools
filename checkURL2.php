<?php

use lib\Database\Database,
    Core\Config;

require_once dirname( __FILE__ ) . '/../../../core/core.php';

Config::loadCustom('/etc/putong/putong-statistics-dashboard/config.ini');
$dbstats = Database::getDb(DB_STATS);
$dbMain = Database::getDb(DB_MAIN);
$tableSource = "stats.messages_by_users_tmp";
$tableTarget = "stats.messages_by_users_url_temp";
$largestId = $dbstats->getOneNumber("SELECT id FROM ".$tableSource." ORDER BY id DESC LIMIT 1");
echo "largestId: ".$largestId."\n";
$id = 0;
$array = array();

function isURL($text){
    //wait to be done
    //   ....
    // $regex = "((https?|ftp)\:\/\/)?"; // SCHEME 
    // $regex .= "([a-z0-9+!*(),;?&=\$_.-]+(\:[a-z0-9+!*(),;?&=\$_.-]+)?@)?"; // User and Pass 
    // $regex .= "([a-z0-9-.]*)\.([a-z]{2,3})"; // Host or IP 
    // $regex .= "(\:[0-9]{2,5})?"; // Port 
    // $regex .= "(\/([a-z0-9+\$_-]\.?)+)*\/?"; // Path 
    // $regex .= "(\?[a-z+&\$_.-][a-z0-9;:@&%=+\/\$_.-]*)?"; // GET Query 
    // $regex .= "(#[a-z_.-][a-z0-9+\$_.-]*)?"; // Anchor 
    //$regex = "@(https?|ftp)://(-\.)?([^\s/?\.#-]+\.?)+(/[^\s]*)?$@iS";

    //$regex = "/\b((?:[a-z][\w-]+:(?:\/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'\".,<>?«»“”‘’]))/ig";
    //$regex = '%^(?:(?:https?|ftp)://)(?:\S+(?::\S*)?@|\d{1,3}(?:\.\d{1,3}){3}|(?:(?:[a-z\d\x{00a1}-\x{ffff}]+-?)*[a-z\d\x{00a1}-\x{ffff}]+)(?:\.(?:[a-z\d\x{00a1}-\x{ffff}]+-?)*[a-z\d\x{00a1}-\x{ffff}]+)*(?:\.[a-z\x{00a1}-\x{ffff}]{2,6}))(?::\d+)?(?:[^\s]*)?$%iu';
    //$regex = "@(-\.)?([^\s/?\.#-]+\.?)+(/[^\s]*)?$@iS";
    //$regex = "/(http|https|ftp|ftps)\:\/\/[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,3}(\/\S*)?/";
    //regular expression source: https://mathiasbynens.be/demo/url-regex
    $regex = "~(?:\b[a-z\d.-]+://[^<>\s]+|\b(?:(?:(?:[^\s!@#$%^&*()_=+[\]{}\|;:'\",.<>/?]+)\.)+(?:ac|ad|aero|ae|af|ag|ai|al|am|an|ao|aq|arpa|ar|asia|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|biz|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|cat|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|coop|com|co|cr|cu|cv|cx|cy|cz|de|dj|dk|dm|do|dz|ec|edu|ee|eg|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gov|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|info|int|in|io|iq|ir|is|it|je|jm|jobs|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mil|mk|ml|mm|mn|mobi|mo|mp|mq|mr|ms|mt|museum|mu|mv|mw|mx|my|mz|name|na|nc|net|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|org|pa|pe|pf|pg|ph|pk|pl|pm|pn|pro|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sk|sl|sm|sn|so|sr|st|su|sv|sy|sz|tc|td|tel|tf|tg|th|tj|tk|tl|tm|tn|to|tp|travel|tr|tt|tv|tw|tz|ua|ug|uk|um|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|xn--0zwm56d|xn--11b5bs3a9aj6g|xn--80akhbyknj4f|xn--9t4b11yi5a|xn--deba0ad|xn--g6w251d|xn--hgbk6aj7f53bba|xn--hlcj6aya9esc7a|xn--jxalpdlp|xn--kgbechtv|xn--zckzah|ye|yt|yu|za|zm|zw)|(?:(?:[0-9]|[1-9]\d|1\d{2}|2[0-4]\d|25[0-5])\.){3}(?:[0-9]|[1-9]\d|1\d{2}|2[0-4]\d|25[0-5]))(?:[;/][^#?<>\s]*)?(?:\?[^#<>\s]*)?(?:#[^<>\s]*)?(?!\w))~iS";
    $newString = str_replace(" ","",$text);
    $type = null;
    if(preg_match($regex, $text)) 
    {                   
            $ret = true;
    } else {
            $ret = false;
    }
    return $ret;
}

function getUrlType($url){
    $type = "other";
    $httpRegex = "/(https|http)/";
    $FtpRegex = "/(ftp|ftps)/";
    $IpRegex = "/(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)(?:[.](?:25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)){3}/";
    $wwwWapRegex = "/(www|wap).[a-zA-Z]{2,3}(\/\S*)?/";
    $EmailRegex = "/[-0-9a-zA-Z.+_]+\@[-0-9a-zA-Z.+_]+.[a-zA-Z]{2,4}/";
    if(preg_match($httpRegex, $url)){
        $type = "http_https";
        return $type;
    }
    elseif(preg_match($FtpRegex, $url)){
        $type = "ftp_ftps";
        return $type;
    }
    elseif(preg_match($IpRegex, $url)){
        $type = "ip";
        return $type;
    }
    elseif(preg_match($wwwWapRegex, $url)){
        $type = "www_wap";
        return $type;
    }
   
    if(preg_match($EmailRegex, $url)){
        $type = "email";
        return $type;
    }
    
    return $type;
}

function testURLFunction($testCases){
    echo("\n\n Pass the test: \n");
    $invalidAry = array();
    foreach($testCases as $value){
        if(isURL($value)){
            $type = getUrlType($value);
            echo($type.": ".$value."\n");
        }else{
            $invalidAry[] = $value;
        }
    }
    echo("\n\n Does not pass the test: \n");
    foreach($invalidAry as $invalid){
        echo($invalid."\n");
    }
    echo("\n\n");
}

$testCases = array();
$testCases[] = "www.baidu.com";
$testCases[] = "www.baidu.com/12312/4123#";
$testCases[] = "122.22.111.11";
$testCases[] = "http://22.22.22.221";
$testCases[] = "https://22.22.22.221";
$testCases[] = "https://22.22.22.221/asqw/ax";
$testCases[] = "http://22.22.22.221/asqw/ax";
$testCases[] = "baidu.com";
$testCases[] = "baidu.com/1231/12321sadasd";
$testCases[] = "http://example.org/a␠b";
$testCases[] = "http://example.org/a%20b";
$testCases[] = "http://example.org/a%";
$testCases[] = "http://example.org␠";
$testCases[] = "http://example.org/%<>\^`{|}";
$testCases[] = "http://example.org/a-umlaut-ä";
$testCases[] = "http://example.org/a-umlaut-%C3%A4";
$testCases[] = "http://example.org/a-umlaut-%c3%a4";
$testCases[] = "http://example.org/a#b␠c";
$testCases[] = "http://example.org/a#b#c";
$testCases[] = "http://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]/";
$testCases[] = "https://walala.io";
$testCases[] = "wadadada.com/aaa/wqe12/";
$testCases[] = "wadadada.com/aaa/wqe12";
$testCases[] = "wadadada.com/aaa/wqe12-%C3%A4";
$testCases[] = "ftp://qqq.112.wqd/12112/#";
$testCases[] = "ftp://127.2.1.2/213/sad/#";
$testCases[] = "ftp://qqq.112.wqd";
$testCases[] = "ftps://qqq.112.wqd/12112/#";
$testCases[] = "ftps://127.2.1.2/213/sad/#";
$testCases[] = "a.b.c";
$testCases[] = "test.tantan.com";
$testCases[] = "asdkalk;.27.2.1.2/213/sad/#";

testURLFunction($testCases);

$array = array();
while($id < $largestId){
    $data =  $dbstats->getSql("SELECT user_id, value FROM ".$tableSource." WHERE id = ".$id);    
    if($id%10000 == 0) echo " ".$id;
    $newdata = $data[0];
    //var_dump($newdata['value']);
    if(isURL($newdata['value'])){
        $type = getUrlType($newdata['value']);
        echo "\n\nCatch:".$type." \n";
        var_dump($newdata['value']);
        $newdata['type'] = $type;
        var_dump($newdata);
        $array[] = $newdata;
    }
    if(count($array)>= 2){
        $dbstats->insert($array, array(
                        'date_time',
                        'user_id',
                        'value',
                        'type',
                ),
                $tableTarget);
        unset($array);            
    }
    $id += 1;
}


if(!empty($array))
{
    $dbstats->insert($array, array(
                    'date_time',
                    'user_id',
                    'value',
                    'type',
            ),
            $tableTarget);
}

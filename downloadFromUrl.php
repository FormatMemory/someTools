<?php
      //You need to change here
      $directory = "./overSeaPopTop100";
      $filename = "oversea_pop_top100_pictures.csv";
      //.....................
      
      echo "\nAll Start...";
      try{
          readCSVDownload($directory, $filename);
      }catch(Exception $e){
          error_log("Caught $e");
          writeErroLog("readCSVDownload".$e->getMessage(), 'errorLog.txt');
      }
      echo "\n\n All Done...";
      
      
      
      
      
      
      function writeErroLog($ermsg, $filename){
            $myfile = fopen($filename, "w+") or die("Unable to open file!");
            $txt = $ermsg."\n\r";
            fwrite($myfile, $txt);
            fclose($myfile);
      }
      
      //read csv file(in a specific format), and will download file from the url in csv file
      //@directory, folder contains csv file's folder path
      //@filename, csv file's name
      function readCSVDownload($directory, $filename){
          try{
                echo "\nReadCSV Start...\n";
                $csvFilePath = "./".$filename;
                ini_set('auto_detect_line_endings',TRUE);
                $handle = fopen($csvFilePath,'r');
                while ( ($data = fgetcsv($handle) ) !== FALSE ) {
                    //process
                    //print_r($data);
                    //echo $data[0]."             ".$data[3]."\n";
                    $cityDirectory = $directory."/".$data[1];
                    downloadIMG($cityDirectory, $data[0], $data[3]);
                }
                ini_set('auto_detect_line_endings',FALSE);
                echo "\n\n readCSVDownload Done...\n";
          }catch(Exception $e){
              error_log("Caught $e");
              writeErroLog("readCSVDownload".$e->getMessage(), 'errorLog.txt');
          }
      }
      
      // Download files from a $url to ./$id folder
      // Folder would be created if doesn't exist
      // Filee would be renamed to 1.*, 2.*, 3.* ...
      //@rootPath: folder you want to put the download files
      //@currrent $id, for creating a folder in this name
      //@url the url for the file you want to download
      function downloadIMG($rootPath, $id, $url){
          try{
                //$rootPath = "./DownloadIMG";
                if (!file_exists($rootPath)) {
                      mkdir($rootPath, 0777, true);
                      echo "\n".$rootPath." has been created...\n";
                }
                $path =  $rootPath."/".$id;
                //create folder
                $url = "http://tantan-cloud.p1.cn/v1/images/".$url;
                if (!file_exists($path)) {
                      mkdir($path, 0777, true);
                      echo "\n\n".$path." has been created...\n";
                }
                  $oldName = substr($url, -20);
                  $filepath = $path."/".$oldName;
                  file_put_contents($filepath, file_get_contents($url));
                  $number = getFileNumbers($path, "*.*")+1;
                  //$ext = getFileExtention($filepath);
                  $ext = "jpeg";//"mandatory to change extention to .jpeg"
                  $newFile = $path."/".$number.".".$ext;
                  rename($filepath, $newFile);
                  echo "      ".$newFile."   Done...\n";
            }catch(Exception $e){
                error_log("Caught $e");
                writeErroLog("readCSVDownload".$e->getMessage(), 'errorLog.txt');
            }
      }
      
      //return number of files of $fileType
      //@para: directory path, $fileType
      function getFileNumbers($directory, $fileType){
          $files = glob($directory ."/". $fileType);
          if ( $files !== false ){
            $filecount = count( $files );
          }
          else{
            $filecount = 0;
          }
          return $filecount;
      }
      
      //return a files' extention, no '.' included
      //more file info can be provided, see more: pathinfo() @php
      //$para filepath
      function getFileExtention($filepath){
          $path_parts = pathinfo($filepath);
          //print_r($path_parts);
          return $path_parts['extension'];
      }
?>

<?php
      //You need to change here
      $directory = "/Users/yusheng/Tantan/tasks/downloadImg";
      $filename = "user_popularity_rank_urls.csv";
      //.....................
      
      echo "\nAll Start...";
      readCSV($directory, $filename);
      echo "\n\n All Done...";
      
      
      
      
      
      
      
      
      //read csv file(in a specific format), and will download file from the url in csv file
      //@directory, folder contains csv file's folder path
      //@filename, csv file's name
      function readCSV($directory, $filename){
          echo "\nReadCSV Start...\n";
          $csvFilePath = $directory."/".$filename;
          ini_set('auto_detect_line_endings',TRUE);
          $handle = fopen($csvFilePath,'r');
          while ( ($data = fgetcsv($handle) ) !== FALSE ) {
              //process
              //print_r($data);
              //echo $data[0]."             ".$data[3]."\n";
              downloadIMG($directory, $data[0], $data[3]);
          }
          ini_set('auto_detect_line_endings',FALSE);
          echo "\n\n ReadCSV Done...\n";
      }
      
      // Download files from a $url to ./$id folder
      // Folder would be created if doesn't exist
      // Filee would be renamed to 1.*, 2.*, 3.* ...
      //@rootPath: folder you want to put the download files
      //@currrent $id, for creating a folder in this name
      //@url the url for the file you want to download
      function downloadIMG($rootPath, $id, $url){
          $rootPath = "./downloadImg";
          $path =  $rootPath."/".$id;
          //create folder
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

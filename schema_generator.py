import sys
import psycopg2
import os
import zipfile
import shutil

def get_column_str(columns_list):
    fields = []
    for col in sorted(columns_list, reverse=False):
        fields.append(col[1])
    return ',\n    '.join(fields)

def type_convert(field_type):
    if '[]' in field_type or 'character varying' in field_type.lower() or 'date' == field_type.lower():
        return 'STRING'
    elif 'integer' == field_type.lower() or 'smallint' == field_type.lower():
        return 'INTEGER'
    elif 'bigint' == field_type.lower():
        return 'BIGINT'
    elif 'timestamp without time zone' == field_type.lower():
        return 'TIMESTAMP'
    elif 'double precision' == field_type.lower():
        return 'DOUBLE'
    elif 'boolean' == field_type.lower():
        return 'BOOLEAN'
    else:
        return 'STRING'

def assemble_fields(fw, columns_list):
    last_record_idx=sorted(columns_list, reverse=True)[0][0]    
    for col in sorted(columns_list, reverse=False):
        if col[0] == last_record_idx:
            fw.write("    {0} {1} COMMENT ".format(col[1], type_convert(col[2])))
            try:
                fw.write(u"'{0}'".format(col[1]))
            except (LookupError, KeyError) as error:
                fw.write(u" '' ")
            fw.write('\n')
        else:
            fw.write("    {0} {1} COMMENT  ".format(col[1], type_convert(col[2])))
            try:
                fw.write(u"'{0}',".format(col[1]))
            except (LookupError, KeyError) as error:
                fw.write(u" '' ")
            fw.write('\n')

def generate_table_schema(path, layer_name, db_name, schema_name, tbl_name, import_type, columns_list, comment, tableOwner):
    #fw.write("############################ TABLE DDL ############################\n")
    
    if layer_name == 'dwd':
        #fw.write("############## dwd_{0}_{1}_{2}.sql ##############\n".format(db_name, schema_name, tbl_name))
        retFileName = "dwd_{0}_{1}_{2}.sql".format(db_name, schema_name, tbl_name)
    else:
        #fw.write("############## ods_{0}_{1}_{2}.sql ##############\n".format(db_name, schema_name, tbl_name))
        retFileName = "ods_{0}_{1}_{2}.sql".format(db_name, schema_name, tbl_name)
      
    fw = open(path+'/'+retFileName, "w")   
    #fw.write('DROP table IF EXISTS {0}.{1}_{2}_{3}_{4}_{5}_d;\n'.format(layer_name, layer_name, db_name, schema_name,
                                                                        #tbl_name, import_type))
    fw.write(
        'CREATE TABLE IF NOT EXISTS {0}.{1}_{2}_{3}_{4}_{5}_d(\n'.format(layer_name, layer_name, db_name, schema_name,
                                                                         tbl_name, import_type))
    assemble_fields(fw, columns_list)
    fw.write(')\n')
    fw.write("COMMENT \'{0}\'\n".format(comment))
    fw.write("PARTITIONED BY (dt string COMMENT \'date partition, format: yyyy-MM-dd\')\n")
    fw.write("ROW FORMAT DELIMITED\n")
    fw.write(r"    FIELDS TERMINATED BY '\001'")
    fw.write('\n')
    fw.write(r"    LINES TERMINATED BY '\n'")
    fw.write('\n')
    if layer_name == 'dwd':
        fw.write('STORED AS PARQUET\n')
    fw.write('tblproperties("BUSINESS_OWNER"=\'{0}\', "DATA_OWNER"=\'{0}\');\n\n\n'.format(tableOwner)) # 
    fw.flush()
    fw.close()
    return retFileName
    
def list_table_column(fw, columns_list):
    fw.write("\n\n############################ TABLE SCHEMA ############################\n")
    fw.write("COLUMN_NAME".ljust(20) + "COLUMN_TYPE\n")
    for col in columns_list:
        fw.write(col[1].ljust(20) + col[2] + '\n')
#     fw.write("\n\n############################ XXXXXXX ############################\n")
#     for col in columns_list:
#         fw.write(col[1]+',\n')
    fw.write("\n\n############################ TABLE SCHEMA ############################\n")
    
def generate_table_column_list(path, columns_list):
    retFileName = "table_schema_{0}_{1}_{2}.txt".format(db_name, schema_name, tbl_name)
    fw = open(path+'/'+retFileName, "w")
    fw.write("table_schema_{0}_{1}_{2}".format(db_name, schema_name, tbl_name))
    fw.write("\n\n############################ TABLE SCHEMA ############################\n")
    fw.write("COLUMN_NAME".ljust(20) + "COLUMN_TYPE\n")
    for col in columns_list:
        fw.write(col[1].ljust(20) + col[2] + '\n')
    fw.write("\n############################ TABLE SCHEMA ############################\n")
    fw.flush()
    fw.close()
        
def generate_sqoop_shell(path, host_name, user_name, password, db_name, schema_name, tbl_name, import_type, columns_list,layer_name):
    #fw.write("############################ SQOOP SCRIPT ############################\n")
    #fw.write("############## ods_{0}_{1}_{2}.sh ##############\n".format(db_name, schema_name, tbl_name))
    retFileName = "ods_{0}_{1}_{2}.sh".format(db_name, schema_name, tbl_name)    
    fw = open(path+'/'+retFileName, "w")   

    fw.write('#!/usr/bin/env bash\n\n')
    fw.write('source /etc/profile\n\n')
    
    fw.write("source <(grep = /data/share/airflow/secrets/production-systems-config.ini | sed 's/\[/_/g' | sed 's/\]/\ /g' | sed 's/ *= */=/g');\n")
    fw.write('export HADOOP_USER_NAME=hdp-data\n')
    fw.write('export PGTZ=UTC\n')
    fw.write('export PGHOSTADDR=$XXXXXXXX\n')
    fw.write('export PGDATABASE=$XXXXXXXX\n')
    fw.write('export PGUSER=$XXXXXXXX\n')
    fw.write('export PGPASSWORD=$XXXXXXXX\n')
    fw.write('export PGPORT=$XXXXXXXX\n\n')
    
    fw.write('set -o errexit\n')
    fw.write('set -o nounset\n\n')
    fw.write('if [ $# == 0 ];then\n')
    fw.write('    bizdate=`date  +"%Y-%m-%d" -d  "-1 days"`\n')
    fw.write('else\n')
    fw.write('    bizdate=$1\nfi\n\n')
    fw.write("echo 'bizdate:',$bizdate\n\n")
    
    fw.write('sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true \\\n')
    fw.write('-Dmapreduce.job.queuename=datalake-online \\\n')
    fw.write('-Dmapred.job.name={0}_{1}_{2}_{3}_{4}_d \\\n'.format(layer_name, db_name, schema_name,tbl_name, import_type))
    fw.write('--connect jdbc:postgresql://$PGHOSTADDR:$PGPORT/$PGDATABASE \\\n'.format(host_name, 5432, db_name))
    fw.write('--username $PGUSER \\\n')
    fw.write('--password $PGPASSWORD \\\n')
    column_str = get_column_str(columns_list)
    fw.write('########################################################################## \n')
    fw.write('##### to_char(created_time + \'8 h\'::interval, \'YYYY-MM-DD HH24:MI:SS.MS\') as created_time ## \n')
    fw.write('#### ST_AsText(latest_location) AS latest_location #### \n')
    fw.write('##### please do type converted according to the specific situations ###### \n') 
    fw.write('##### like geo, json or costom type                                 ###### \n') 
    #fw.write('##### ' + column_str + ' ######\n')
    fw.write('########################################################################## \n')
    list_table_column(fw, columns_list)
    fw.write('--hive-drop-import-delims \\\n')
    fw.write('--query "SELECT\n    {0}\nFROM\n    {1}.{2}\nWHERE\n    1=1\nAND    \$CONDITIONS" \\\n'.format(get_column_str(columns_list), schema_name, tbl_name))
    fw.write('--split-by "XXXXXXXX" \\\n') # primary key
    fw.write('-m 10 \\\n')
    
#     if import_type == 'i':
#         fw.write('--incremental XXXappend\lastmodifiedXXX \\\n')
#         fw.write('--check-column "XXXX" \\\n')
#         fw.write('--last-value XXXX \\\n')

    fw.write('--null-string \'\\\\N\' \\\n')
    fw.write('--null-non-string \'\\\\N\' \\\n')
    fw.write('--fields-terminated-by "\\001" \\\n')
    fw.write('--lines-terminated-by "\\n" \\\n')
    fw.write('--target-dir "/user/hive/warehouse/ods.db/ods_{0}_{1}_{2}_{3}_d/dt=$bizdate" \\\n'.format(db_name, schema_name, tbl_name, import_type))
    fw.write('-delete-target-dir \\\n')
    fw.write('--compression-codec org.apache.hadoop.io.compress.GzipCodec\n\n') 
    fw.write('hive -e \"ALTER TABLE ods.{0}_{1}_{2}_{3}_{4}_d ADD IF NOT EXISTS PARTITION (dt=\'$bizdate\')\"\n\n\n\n'.format(layer_name, db_name, schema_name,
                                                                        tbl_name, import_type))
    fw.flush()
    fw.close()

def generate_dwd_dml(path, db_name, schema_name, tbl_name, import_type, columns_list):
    retFileName = "dwd_{0}_{1}_{2}.sh".format(db_name, schema_name, tbl_name)    
    fw = open(path+'/'+retFileName, "w")
    #fw.write("############################ DWD ETL SCRIPT ############################\n")
    #fw.write("############## dwd_{0}_{1}_{2}.sh ##############\n".format(db_name, schema_name, tbl_name))
    fw.write('#!/usr/bin/env bash\n\n')
    
    fw.write('source /etc/profile\n')
    fw.write('export HADOOP_USER_NAME=hdp-data\n\n')
    
    fw.write('set -o errexit\n')
    fw.write('set -o nounset\n\n')
    
    fw.write('if [ $# == 0 ];then\n')
    fw.write('    bizdate=`date  +"%Y-%m-%d" -d  "-1 days"`\n')
    fw.write('else\n')
    fw.write('    bizdate=$1\nfi\n\n')
    
    fw.write("echo 'bizdate:',$bizdate\n\n")
    fw.write("#To use hive1.0\n")
    fw.write("export HIVE_HOME=/data/software/app/apache-hive-1.2.2-bin\n")
    fw.write('hive -e "\n')
    fw.write('set hive.execution.engine=MR;\n')
    fw.write('set parquet.compression=GZIP;\n')
    fw.write('set mapreduce.job.queuename=datalake-online;\n')
    fw.write('set mapred.queue.names=datalake-online;\n')
    fw.write('set mapred.job.name=dwd_{0}_{1}_{2};\n'.format(db_name, schema_name, tbl_name))
    fw.write('set spark.executor.instances=10;\n')
    fw.write('set spark.executor.memory=4g;\n')
    fw.write('set spark.executor.cores=4;\n')
    fw.write('INSERT OVERWRITE TABLE dwd.dwd_{0}_{1}_{2}_{3}_d '.format(db_name, schema_name, tbl_name, import_type))
    fw.write('PARTITION (dt=\'${bizdate}\') \n')
    column_str = get_column_str(columns_list)
    fw.write('SELECT\n    {0}\nFROM\n    ods.ods_{1}_{2}_{3}_{4}_d '.format(column_str, db_name, schema_name, tbl_name, import_type))
    fw.write('WHERE dt=\'${bizdate}\'\"\n\n')
    fw.flush()
    fw.close()

def generate_dag_script(fw, db_name, schema_name, tbl_name, import_type, tableOwner):
    #fw.write("############################ DAG SCRIPT ############################\n")
    #fw.write("############## etl_{0}_{1}_{2}.py ##############\n".format(db_name, schema_name, tbl_name))
    retFileName = "etl_{0}_{1}_{2}.py".format(db_name, schema_name, tbl_name)    
    fw = open(path+'/'+retFileName, "w")

    fw.write('# -*- coding: utf-8 -*-\n')
    fw.write('from airflow import utils\n')
    fw.write('from airflow import DAG\n')
    fw.write('from airflow.operators.bash_operator import BashOperator\n')
    fw.write('from datetime import timedelta\n')
    fw.write('"""\n')
    fw.write('INPUT: pg_{0}_{1}_{2}\n'.format(db_name, schema_name, tbl_name))
    fw.write('OUTPUT: ods.ods_{0}_{1}_{2}_{3}_d\n'.format(db_name, schema_name, tbl_name, import_type))
    fw.write('INPUT: ods.ods_{0}_{1}_{2}_{3}_d\n'.format(db_name, schema_name, tbl_name, import_type))
    fw.write('OUTPUT: dwd.dwd_{0}_{1}_{2}_{3}_d\n'.format(db_name, schema_name, tbl_name, import_type))
    fw.write('"""\n')

    fw.write('DAG_NAME = \'etl_{0}_{1}_{2}\'\n'.format(db_name, schema_name, tbl_name))
    fw.write('BIZ_DATE = \'{{ ds }}\'\n')
    fw.write('default_args = {\n')
    fw.write('    \'owner\': \'{0}\',\n'.format(tableOwner))
    fw.write('    \'depends_on_past\': False,\n')
    fw.write('    \'start_date\': utils.dates.days_ago(1),\n')
    fw.write('    \'retries\': 2,\n')
    fw.write('    \'email\': [\'{0}@p1.com\'],\n'.format(tableOwner))
    fw.write('    \'email_on_failure\': True,\n')
    fw.write('    \'email_on_retry\': True,\n')
    fw.write('    \'retry_delay\': timedelta(minutes=10),\n')
    fw.write('}\n\n')
    
    fw.write('dag = DAG(DAG_NAME, schedule_interval=\'0 1 * * *\', default_args=default_args)\n')
    
    fw.write('etl2odsCommand = \'bash $AIRFLOW_DATA/scripts/hdp-data/ods/ods_{0}_{1}_{2}.sh \'\n'.format(db_name, schema_name, tbl_name))
    
    fw.write('etl2dwdCommand = \'bash $AIRFLOW_DATA/scripts/hdp-data/dwd/dwd_{0}_{1}_{2}.sh \'\n'.format(db_name, schema_name, tbl_name))
    
    fw.write('etl2odsTask = BashOperator(\n')
    fw.write('    task_id=\'ods_{0}_{1}_{2}\',\n'.format(db_name, schema_name, tbl_name))
    fw.write('    dag=dag,\n')
    fw.write('    bash_command=etl2odsCommand + BIZ_DATE)\n')
    
    fw.write('etl2dwdTask = BashOperator(\n')
    fw.write('    task_id=\'dwd_{0}_{1}_{2}\',\n'.format(db_name, schema_name, tbl_name))
    fw.write('    dag=dag,\n')
    fw.write('    bash_command=etl2dwdCommand + BIZ_DATE)\n')
    
    fw.write('etl2dwdTask.set_upstream(etl2odsTask)\n')
    fw.flush()
    fw.close()

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
            
if __name__ == '__main__':
    #############################
    ##########Fill out###########
    host_name='XXXXXX'
    user_name='XXXXXX'
    password='XXXXXX'
    
    db_name='XXXX'  
    schema_name= 'XXXX'
    tbl_name='XXXXXX'
    import_type='a' # a measn all(全量导入),i means increment(增量导入)
    comment = 'XXXXXXXXX'#descript this table (in ddl)
    isOmi = False #Import data for omi
    tableOwner = 'Your Name'
    ############Fill out##########
    ##############################
    try:
        db = psycopg2.connect(database=db_name, user=user_name, password=password, host=host_name, port="5432")
        db.autocommit = True
        cursor = db.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    query_sql='''
    SELECT
                     col.ordinal_position, 
                     col.COLUMN_NAME, 
                     col.data_type, 
                     des.description, 
                     col.table_schema, 
                     col.table_name
    FROM 
                     information_schema.COLUMNS col 
    LEFT JOIN 
                    pg_description des ON col.TABLE_NAME::regclass = des.objoid
    AND                col.ordinal_position = des.objsubid 
    WHERE 
                     table_schema='{0}' 
    AND                  table_name='{1}' 
    ORDER BY             ordinal_position;'''
    print(query_sql.format(schema_name, tbl_name))
    cursor.execute(query_sql.format(schema_name, tbl_name))
    columns_list = cursor.fetchall()
    
    # db name or table name may has horizontal line, which does not meet our naming conventions and needs to be replaced with an underscore
    if '-' in db_name:
        db_name = db_name.replace('-', '_')
    if '-' in tbl_name:
        tbl_name = tbl_name.replace('-', '_')
    
    if isOmi:
        db_name = 'omi_'+db_name
    
    
    #fw = open("{0}_{1}_{2}.sql".format(db_name, schema_name, tbl_name), "w")
    path = "{0}_{1}_{2}".format(db_name, schema_name, tbl_name)
    try:
        shutil.rmtree(path)
    except:
        pass
    os.mkdir(path);
    
    #ddl layer
    generate_table_schema(path, 'ods', db_name, schema_name, tbl_name, import_type, columns_list, comment, tableOwner)
    generate_table_schema(path, 'dwd', db_name, schema_name, tbl_name, import_type, columns_list, comment, tableOwner)
    
    generate_sqoop_shell(path, host_name, user_name, password, db_name, schema_name, tbl_name, import_type, columns_list, 'ods')

    generate_dwd_dml(path, db_name, schema_name, tbl_name, import_type, columns_list)
    
    generate_dag_script(path, db_name, schema_name, tbl_name, import_type, tableOwner)
    
    generate_table_column_list(path, columns_list)
    
    zipf = zipfile.ZipFile(path+'.zip', 'w', zipfile.ZIP_DEFLATED)
    zipdir(path, zipf)
    zipf.close()
    shutil.rmtree(path)
    #fw.close()
    print(path+".zip\n")
    
    print("ods_{0}_{1}_{2}.sh\n".format(db_name, schema_name, tbl_name))
    
    print("dwd_{0}_{1}_{2}.sh\n".format(db_name, schema_name, tbl_name))
    
    print("etl_{0}_{1}_{2}.py\n".format(db_name, schema_name, tbl_name))
    
    print("dwd_{0}_{1}_{2}_ddl.sql\n".format(db_name, schema_name, tbl_name))
    
    print("ods_{0}_{1}_{2}_ddl.sql\n".format(db_name, schema_name, tbl_name))
    
    print("DONE...")
    



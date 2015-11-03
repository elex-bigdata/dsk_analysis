#######################################数据etl#####################################################
1.
#用户上报数据json etl表
CREATE EXTERNAL TABLE user_software_json_etl (
           json string
)
PARTITIONED BY (day STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ;
#加载本地数据
LOAD DATA LOCAL INPATH '/home/hadoop/export_data/data2.json.gz' INTO TABLE user_software_json_etl PARTITION(day='2015-10-27');
#加载HDFS数据
LOAD DATA INPATH '/home/hadoop/upusers_5.json' INTO TABLE user_software_json_etl PARTITION(day='2015-10-27');

2.
#用户上报数据etl表
CREATE EXTERNAL TABLE user_software_etl (
id string,
ts string,
nation string,
version string,
attr4 string,
attr5 string,
items string
)
PARTITIONED BY (day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
#添加分区
ALTER TABLE user_software_etl ADD PARTITION (day ='2015-10-27');
#######################################数据 proccess#####################################################
3.
#添加UDF
add jar /home/hadoop/yankun/udf/dsk_analysis-0.0.1-SNAPSHOT.jar
create temporary function raw_json_etl as 'hive.udf.RawDataETLJsonUDF';
#用户上报数据etl语句，导入etl表
#insert into table user_software_etl  partition (day='2015-10-27')
insert overwrite  directory '/user/hive/warehouse/user_software_etl/day=2015-10-27'
select raw_json_etl(json)
from user_software_json_etl ;
4.
#######################################数据导入kudu#####################################################
CREATE TABLE user_software_kudu
DISTRIBUTE BY HASH (id) INTO 3 BUCKETS
TBLPROPERTIES(
'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
'kudu.table_name' = 'user_software_kudu',
'kudu.master_addresses' = '10.1.3.55',
'kudu.key_columns' = 'id'
 ) AS select distinct(id),ts,nation,version,attr4,attr5,items FROM user_software_etl;

########################################数据 elt#######################################################
1.
CREATE EXTERNAL TABLE user_software_name_json_etl (
           json string
)
PARTITIONED BY (day STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ;
#加载数据到分区
LOAD DATA LOCAL INPATH '/home/hadoop/export_data/data3.json.gz' INTO TABLE user_software_name_json_etl PARTITION(day='2015-10-27');

2.
# 数据ETL表
CREATE EXTERNAL TABLE user_software_name_etl (
sname string,
sid string
)
PARTITIONED BY (day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
# 添加分区
ALTER TABLE user_software_name_etl ADD PARTITION (day ='2015-10-27');

3.
# ETL数据
insert overwrite table user_software_name_etl  partition (day='2015-10-27')
select 
get_json_object(t.json,'$._id'),
get_json_object(t.json,'$.id')
from user_software_name_json_etl t;
########################################数据 入kudu#######################################################
CREATE TABLE user_software_name_kudu
DISTRIBUTE BY HASH (sname) INTO 3 BUCKETS
TBLPROPERTIES(
'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
'kudu.table_name' = 'user_software_name_kudu',
'kudu.master_addresses' = '10.1.3.55',
'kudu.key_columns' = 'sname,sid'
 ) AS  select distinct sname,sid FROM user_software_name_etl where sid rlike '^\\d+$' and (sname is not null or sname !="");
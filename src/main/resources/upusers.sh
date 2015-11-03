#######################################数据etl#####################################################
#用户上报数据json etl表
CREATE EXTERNAL TABLE upusers_json_etl (
           json string
)
PARTITIONED BY (day STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ;
#加载本地数据
LOAD DATA LOCAL INPATH '/home/hadoop/upusers_4.json.gz' INTO TABLE upusers_json_etl PARTITION(day='2015-10-27');
#加载HDFS数据
LOAD DATA INPATH '/home/hadoop/upusers_5.json' INTO TABLE upusers_json_etl PARTITION(day='2015-10-27');

#用户上报数据etl表
CREATE EXTERNAL TABLE upusers (
uid string,
ptid string,
sid string,
n string,
ln string,
ver string,
pid string,
geoip_n string,
first_tm string,
first_date string,
last_tm string,
last_date string,
days string
)
PARTITIONED BY (day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;

#用户上报数据etl语句，导入etl表
insert into table upusers_etl  partition (day='2015-10-27')
select 
get_json_object(get_json_object(t.json,'$.attr'),'$.uid'),
get_json_object(get_json_object(t.json,'$.attr'),'$.ptid'),
get_json_object(get_json_object(t.json,'$.attr'),'$.sid'),
get_json_object(get_json_object(t.json,'$.attr'),'$.n'),
get_json_object(get_json_object(t.json,'$.attr'),'$.ln'),
get_json_object(get_json_object(t.json,'$.attr'),'$.ver'),
get_json_object(get_json_object(t.json,'$.attr'),'$.pid'),
(CASE WHEN get_json_object(get_json_object(t.json,'$.attr'),'$.geopi_n')="false" THEN ''
WHEN get_json_object(get_json_object(t.json,'$.attr'),'$.geopi_n') is not null THEN lower(get_json_object(get_json_object(t.json,'$.attr'),'$.geopi_n'))
WHEN get_json_object(get_json_object(t.json,'$.attr'),'$.geoip_n')="false" THEN ''
WHEN get_json_object(get_json_object(t.json,'$.attr'),'$.geoip_n') is not null THEN lower(get_json_object(get_json_object(t.json,'$.attr'),'$.geoip_n'))
ELSE "" END),
get_json_object(get_json_object(t.json,'$.attr'),'$.first_tm'),
get_json_object(get_json_object(t.json,'$.attr'),'$.first_date'),
get_json_object(get_json_object(t.json,'$.attr'),'$.last_tm'),
get_json_object(get_json_object(t.json,'$.attr'),'$.last_date'),
regexp_replace(regexp_replace(get_json_object(t.json,'$.days'),"(\\[)|(\\])",""),",","#")
from upusers_json_etl t  ;

#######################################数据导入hbase#####################################################
#创建hbase分区表
create 'upusers',{NAME => 'f',NUMREGIONS => 100, SPLITALGO => 'HexStringSplit'}
disable 'upusers'
alter 'upusers', NAME => 'f', COMPRESSION => 'snappy'
enable 'upusers'

#生成HFile
sudo -u hdfs hadoop fs -rmr hdfs://namenode:8020/tmp/hbase/bulkload/upusers
HADOOP_CLASSPATH=$(hbase classpath):/etc/hbase/conf/ \
hadoop jar /opt/cloudera/parcels/CDH-5.4.7-1.cdh5.4.7.p0.3/jars/hbase-server-1.0.0-cdh5.4.7.jar importtsv  \
-Dimporttsv.separator=,  \
-Dimporttsv.bulk.output=/tmp/hbase/bulkload/upusers  \
-Dimporttsv.columns=HBASE_ROW_KEY,\
f:ptid,f:sid,f:n,f:ln,f:ver,f:pid,f:geoip_n,f:first_tm,f:first_date,f:last_tm,f:last_date,f:days \
upusers /user/hive/warehouse/upusers/day=2015-10-27 
#导入hbase表
sudo -u hdfs hadoop fs -chown -R hbase /tmp/hbase/bulkload/upusers
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/hbase/bulkload/upusers upusers
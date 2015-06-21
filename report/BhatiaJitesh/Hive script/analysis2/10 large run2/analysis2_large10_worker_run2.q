create external table postdata1(id BIGINT,type BIGINT,parentid BIGINT,cdate Timestamp,viewcount BIGINT,ownerid BIGINT,closedate Timestamp,tag string,answercount BIGINT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 stored as textfile location 's3://bhatia.ay.hive/analysis2/TABLE1';

load data inpath '${INPUT}' overwrite into table postdata1;

create external table postdata2(id BIGINT,type BIGINT,parentid BIGINT,cdate Timestamp,viewcount BIGINT,ownerid BIGINT,closedate Timestamp,tag string,answercount BIGINT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 stored as textfile location 's3://bhatia.ay.hive/analysis2/TABLE2';

load data inpath '${INPUT1}' overwrite into table postdata2;

INSERT OVERWRITE DIRECTORY 's3n://bhatia.ay.hive/analysis2/ANALYSIS2OUTPUTLARGE10run2'
 select tag,count(tag) as c, avg(cast(round(cast((e-s)as DECIMAL)) as DECIMAL ))latency from
 (select *,row_number() over (partition by parentid,tag order by e asc ) as row_number from 
 (select t1.id,t2.parentid,t1.tag, cast(t1.cdate as DECIMAL) s,cast(t2.cdate as DECIMAL) e from 
 postdata1 t1 join postdata2 t2 on (t1.id = t2.parentid AND t1.tag=t2.tag))b ) a
 where row_number=1 group by tag order by c desc;


create external table postdata(id BIGINT,type BIGINT,parentid BIGINT,cdate Timestamp,viewcount BIGINT,ownerid BIGINT,closedate Timestamp,tag string,answercount BIGINT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 stored as textfile location 's3://bhatia.ay.hive/analysis1/TABLE1';

load data inpath '${INPUT}' overwrite into table postdata;

create external table user(userid BIGINT,reputation BIGINT,cdate Timestamp,username string,accessdate Timestamp,age BIGINT,views BIGINT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 stored as textfile location 's3://bhatia.ay.hive/analysis1/TABLE2';

load data inpath '${INPUT1}' overwrite into table user;

INSERT OVERWRITE DIRECTORY 's3n://bhatia.ay.hive/analysis1/ANALYSIS1OUTPUTLARGE5' 
select tag,username,countuser from
 (select *,row_number() over(partition by tag order by tag,countuser desc) as row_number from
 (select postdata.tag,user.username,count(user.userid) countuser from user join postdata on(user.userid = postdata.ownerid)
 group by postdata.tag,user.username order by tag,countuser desc)a)b where row_number between 0 and 5;



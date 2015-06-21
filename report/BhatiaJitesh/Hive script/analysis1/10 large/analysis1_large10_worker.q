create external table postdata(id INT,type INT,parentid INT,cdate Timestamp,viewcount INT,ownerid INT,closedate Timestamp,tag string,answecount INT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 stored as textfile location 's3://bhatia.ay.hive/analysis1/TABLE1';

load data inpath '${INPUT}' overwrite into table postdata;

create external table user(userid INT,reputation INT,cdate Timestamp,username string,accessdate Timestamp,age INT,views INT)
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
 stored as textfile location 's3://bhatia.ay.hive/analysis1/TABLE2';

load data inpath '${INPUT1}' overwrite into table user;

INSERT OVERWRITE DIRECTORY 's3n://bhatia.ay.hive/analysis1/ANALYSIS1OUTPUT' 
select tag,username,countuser from
 (select *,row_number() over(partition by tag order by tag,countuser desc) as row_number from
 (select postdata.tag,user.username,count(user.userid) countuser from user join postdata on(user.userid = postdata.ownerid)
 group by postdata.tag,user.username order by tag,countuser desc)a)b where row_number between 0 and 5;



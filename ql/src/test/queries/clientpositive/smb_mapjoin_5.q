drop table smb_bucket_3;
drop table smb_bucket_2;
drop table smb_bucket_1;

create table smb_bucket_1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS; 
create table smb_bucket_2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS; 
create table smb_bucket_3(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 1 BUCKETS;

load data local inpath '../data/files/smbbucket_1.txt' overwrite into table smb_bucket_1;
load data local inpath '../data/files/smbbucket_2.txt' overwrite into table smb_bucket_2;
load data local inpath '../data/files/smbbucket_3.txt' overwrite into table smb_bucket_3;

set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
 
explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key left outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key left outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key right outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key right outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key full outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a left outer join smb_bucket_2 b on a.key = b.key full outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key left outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key left outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key right outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key right outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key full outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a right outer join smb_bucket_2 b on a.key = b.key full outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key left outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key left outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key right outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key right outer join smb_bucket_3 c on b.key=c.key;

explain
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key full outer join smb_bucket_3 c on b.key=c.key;
select /*+mapjoin(a,c)*/ * from smb_bucket_1 a full outer join smb_bucket_2 b on a.key = b.key full outer join smb_bucket_3 c on b.key=c.key;

 
drop table smb_bucket_3;
drop table smb_bucket_2;
drop table smb_bucket_1;

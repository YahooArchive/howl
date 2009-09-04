drop table t1;
drop table t2;

create table t1(key string, value string) partitioned by (ds string);
create table t2(key string, value string) partitioned by (ds string);

insert overwrite table t1 partition (ds='1')
select key, value from src;

insert overwrite table t1 partition (ds='2')
select key, value from src;

insert overwrite table t2 partition (ds='1')
select key, value from src;

set hive.test.mode=true;
set hive.mapred.mode=strict;

explain
select count(1) from t1 join t2 on t1.key=t2.key where t1.ds='1' and t2.ds='1';

select count(1) from t1 join t2 on t1.key=t2.key where t1.ds='1' and t2.ds='1';

set hive.test.mode=false;

drop table t1;
drop table t2;

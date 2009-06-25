-- test for loading into tables with the correct file format
-- test for loading into partitions with the correct file format

DROP TABLE T1;
CREATE TABLE T1(name STRING) STORED AS RCFILE;
LOAD DATA LOCAL INPATH '../data/files/kv1.seq' INTO TABLE T1;
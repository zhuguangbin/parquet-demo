
REGISTER hdfs:///data/piglib/mvad/*.jar;
REGISTER hdfs:///data/piglib/thrift/*.jar;
REGISTER hdfs:///data/piglib/common/*.jar;
REGISTER hdfs:///data/piglib/elephantbird/*.jar;

set job.name '[adhoc][parquet-demo][$DATE][00:00][1440]';
set mapred.job.queue.name default;
set mapred.job.priority HIGH;

set pig.maxCombinedSplitSize 1073741824;

set pig.exec.reducers.bytes.per.reducer	10000000000;

cookieEvents = LOAD '/mvad/warehouse/session/dspan/date=$DATE/*' USING org.apache.parquet.pig.ParquetLoader();

describe cookieEvents;
a = FOREACH cookieEvents GENERATE publisherId, 1;
b = GROUP a BY publisherId;
c = FOREACH b GENERATE group, COUNT(a);

STORE c INTO '/tmp/parquet-pigdemo' USING org.apache.parquet.pig.ParquetStorer();


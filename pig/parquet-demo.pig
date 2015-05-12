/*
 Usage:  pig --param "date=2015-02-01" parquet-demo.pig
*/

-- 1. please register piglibs on hdfs. 
----  NOTE: we won't maintain piglibs on every gateway any more.

REGISTER hdfs:///data/piglib/mvad/*.jar;
REGISTER hdfs:///data/piglib/thrift/*.jar;
REGISTER hdfs:///data/piglib/common/*.jar;
REGISTER hdfs:///data/piglib/elephantbird/*.jar;

-- 2. set job name/queuename/priority etc.

set job.name '[adhoc][parquet-demo][$date][00:00][1440]';
set mapred.job.queue.name default;
set mapred.job.priority HIGH;

-- 3. this param will combine split so as to reduce num of MapTask. 
----  set to 1G or higher if your MapTask is very light.

set pig.maxCombinedSplitSize 1073741824;

-- 4. this param decides num of ReduceTask if you did not set reduce num. Pig will calculate reduce num automatically by input size.
----  if your job ReduceTask num is too large, increase this param

set pig.exec.reducers.bytes.per.reducer	10000000000;

-- 5. set parquet related params. 
----  You should always set parquet.task.side.metadata to be true is your job is reading large size of files like sessionlog/ambitionlog/dsplog etc, otherwise  OOM.

set parquet.task.side.metadata true;

----  set output parquet file compression codec. 
----  options are : UNCOMPRESSED(default)/SNAPPY/GZIP/LZO .
 
set parquet.compression LZO;

-- 6. load data using ParquetLoader. 
----  /user/hadoop/thriftlog converted to parquet format daily which located in  /mvad/warehouse/session/dspan
----  /mvad/rawlog/ambition converted to parquet format hourly which located in /mvad/warehouse/ods/ambition
----  /mvad/rawlog/dsp converted to parquet format hourly which located in /mvad/warehouse/ods/dsp
----  /mvad/rawlog/dsp-extended converted to parquet format hourly which located in /mvad/warehouse/ods/dsp-extended
----  /mvad/rawlog/exchange converted to parquet format hourly which located in /mvad/warehouse/ods/exchange
----  /mvad/rawlog/exchange-extended converted to parquet format hourly which located in /mvad/warehouse/ods/exchange-extended

---- see more at https://dev.corp.mediav.com/projects/hadoop/wiki/Parquet

---- sessionlog
---- NOTE: dspan parquet format is flattened cookieevents , dspan-history format is flattened historyEvents.

cookieEvents = LOAD '/mvad/warehouse/session/dspan/date=$date/*' USING parquet.pig.ParquetLoader();
-- historyEvents = LOAD '/mvad/warehouse/session/dspan-history/date=$date/*' USING parquet.pig.ParquetLoader();

---- ambitionlog
---- NOTE: ambition parquet format keeps the same as /mvad/rawlog/ambition
---- /mvad/warehouse/ods/ambition/date={YYYY-mm-dd}/hour={HH}/type={a.s.3/a.c.s/a.p.3}

-- ambitionlog = LOAD '/mvad/warehouse/ods/ambition/date=$date/hour=*/type=*/*' USING parquet.pig.ParquetLoader();

---- dsplog
---- NOTE: dsp parquet format keeps the same as /mvad/rawlog/dsp
---- /mvad/warehouse/ods/dsp/date={YYYY-mm-dd}/hour={HH}/type={d.s.3/d.s.3.m/d.c.3/d.c.3.m/d.u.3/d.u.3.m/d.x.3/d.x.3.m/d.b.3/d.b.3.m}/device={pc/mobile}

-- dsplog = LOAD '/mvad/warehouse/ods/dsp/date=$date/hour=*/type=*/device=*/*' USING parquet.pig.ParquetLoader();

--  7. your own job logic 

describe cookieEvents;
a = FOREACH cookieEvents GENERATE publisherId, 1;
b = GROUP a BY publisherId;
c = FOREACH b GENERATE group, COUNT(a);

--  8. save as parquet
STORE c INTO '/tmp/parquet-demo' USING parquet.pig.ParquetStorer();


package com.mvad.hadoop.parquetdemo.mapreduce;

import com.mediav.data.log.CookieEvent;
import com.twitter.elephantbird.mapreduce.input.LzoThriftBlockInputFormat;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.thrift.ParquetThriftInputFormat;

import java.io.IOException;

/**
 * Created by guangbin on 15-3-21.
 * <p/>
 * This is an example of Using ParquetThriftInputFormat,
 * which explains reading CookieEvent Parquet File and count events number group by publiserId
 */
public class CookieEventCountByPublisher extends Configured implements Tool {


  //
  public static class CookieEventMapper extends Mapper<Object, CookieEvent, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private IntWritable publishId = new IntWritable();

    @Override
    protected void map(Object key, CookieEvent value, Context context) throws IOException, InterruptedException {
      publishId.set(value.getPublisherId());
      context.write(publishId, one);
    }
  }

  public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }


  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: CookieEventCountByPubliser <in> <out>");
      System.exit(2);
    }
    Path in = new Path(otherArgs[0]);
    Path out = new Path(otherArgs[1]);

    // this step is very important for Parquet PPD.
    // readSchema is part of whole CookieEvent Schema, be sure only including required fields your job reads.
    // In this demo, we only count cookieEvents group by publisherId, so we only read two fields: cookie and publisherId.

    // CookieEvent schema is thrift , whole thrift schema can be archived by the following command:
    // hadoop parquet.tools.Main schema /mvad/sessionlog/dspan-parquet/2015-03-20/part-m-00000.lzo.parquet

    String readSchema = "message ParquetSchema {\n" +
            "required group cookie (LIST) { \n" +
            "repeated int32 cookie_tuple; \n" +
            "}\n" +
            "optional int32 publisherId;\n" +
            "}";

    conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
    conf.setBoolean(ParquetInputFormat.TASK_SIDE_METADATA, true);

    conf.setBoolean("elephantbird.use.combine.input.format", true);
    conf.setInt("elephantbird.combine.split.size",1073741824);

    // setup job
    Job job = Job.getInstance(conf);
    job.setJobName("CookieEventCountByPublisher");
    job.setJarByClass(CookieEventCountByPublisher.class);
    job.setMapperClass(CookieEventMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    // set InputFormatClass to be DelegateCombineFileInputFormat to Combine Small Splits
    job.setInputFormatClass(DelegateCombineFileInputFormat.class);
    DelegateCombineFileInputFormat.setCombinedInputFormatDelegate(job.getConfiguration(), ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.addInputPath(job, in);

    // be sure to set ParquetThriftInputFormat ReadSupportClass and ThriftClass
    ParquetThriftInputFormat.setReadSupportClass(job, CookieEvent.class);
    ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), CookieEvent.class);

    FileOutputFormat.setOutputPath(job, out);
    return job.waitForCompletion(true) ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CookieEventCountByPublisher(), args);
  }


}

package com.mvad.hadoop.parquetdemo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
 *
 *  This is an example of Using ParquetThriftInputFormat,
 *  which explains reading ambitionlog Parquet File and count events number group by IP
 */
public class AmbitionLogCountByIP extends Configured implements Tool {


    //
    public static class AmbitionLogMapper extends Mapper<Object, ambition.thrift.LogInfo, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text ip = new Text();

        @Override
        protected void map(Object key, ambition.thrift.LogInfo value, Context context) throws IOException, InterruptedException {
            ip.set(value.getReqInfo().getIp());
            context.write(ip, one);
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
            System.err.println("Usage: AmbitionLogCountByIP <in> <out>");
            System.exit(2);
        }
        Path in = new Path(otherArgs[0]);
        Path out = new Path(otherArgs[1]);



        // this step is very important for Parquet PPD.
        // readSchema is part of whole ambition.thrift.LogInfo Schema, be sure only including required fields your job reads.
        // In this demo, we only count logs group by ip, so we only read two fields: reqInfo.ip and respInfo.reqType

        // AmbitionLog schema is thrift , whole thrift schema can be archived by the following command:
        // hadoop parquet.tools.Main schema /mvad/sessionlog/dspan-parquet/2015-03-20/part-m-00000.lzo.parquet

        String readSchema = "message ParquetSchema {\n" +
                "required group reqInfo { \n"+
                    "required binary ip (UTF8); \n"+
                    "}\n"+
                "required group respInfo { \n" +
                    "required binary reqType (ENUM);\n"+
                    "}\n"+
              "}";

        conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
        conf.setBoolean(ParquetInputFormat.TASK_SIDE_METADATA, true);

        // setup job
        Job job = Job.getInstance(conf);
        job.setJobName("AmbitionLogCountByIP");
        job.setJarByClass(AmbitionLogCountByIP.class);
        job.setMapperClass(AmbitionLogMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // be sure to use ParquetThriftInputFormat
        job.setInputFormatClass(ParquetThriftInputFormat.class);
        ParquetThriftInputFormat.addInputPath(job, in);

        // be sure to set ParquetThriftInputFormat ReadSupportClass and ThriftClass
        ParquetThriftInputFormat.setReadSupportClass(job, ambition.thrift.LogInfo.class);
        ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), ambition.thrift.LogInfo.class);

        FileOutputFormat.setOutputPath(job, out);
        return  job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AmbitionLogCountByIP(), args);
    }


}

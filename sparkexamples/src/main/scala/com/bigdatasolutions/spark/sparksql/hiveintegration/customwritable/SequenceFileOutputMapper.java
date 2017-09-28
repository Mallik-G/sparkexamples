package com.bigdatasolutions.spark.sparksql.hiveintegration.customwritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by madhu on 9/1/15.
 */
public class SequenceFileOutputMapper extends Mapper<LongWritable,Text,NullWritable,SalesRecordWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            SalesRecordWritable salesRecordWritable = SalesRecordParser.parse(value);
            context.write(NullWritable.get(), salesRecordWritable);
        } catch (MalformedRecordException e) {
            e.printStackTrace();
        }
    }
}

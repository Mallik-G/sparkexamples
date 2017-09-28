package com.bigdatasolutions.spark.sparksql.hiveintegration.customwritable;

import org.apache.hadoop.io.Text;

/**
 * Created by madhu on 7/1/15.
 */
public class SalesRecordParser {
    public static SalesRecordWritable parse(Text record) throws MalformedRecordException{
        String [] values = record.toString().split(",");
        if(values.length <4 ) throw  new MalformedRecordException();
        else {
            String transactionId = values[0];
            String customerId = values[1];
            String itemId = values[2];
            double itemValue = Double.parseDouble(values[3]);
            return new SalesRecordWritable(transactionId,customerId,itemId,itemValue);
        }
    }
}

package com.bigdatasolutions.spark.sparksql.hiveintegration;

import com.madhukaraphatak.sparktraining.sparksql.hiveintegration.customwritable.SalesRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 *Only deserializer added
 */
public class SalesRecordSerde extends AbstractSerDe{


    StructObjectInspector rowOI;
    ArrayList<Object> row;


    @Override
    public void initialize(Configuration configuration, Properties tbl) throws SerDeException {


        String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
        int numColumns = columnNames.size();

        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
                numColumns);
        //transaction id
        columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //customer id
        columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        // item id
        columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        // amount
        columnOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);

        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
                columnNames, columnOIs);

        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
            row.add(null);
        }


    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return null;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
         SalesRecordWritable salesRecordWritable = (SalesRecordWritable) writable;
         row.set(0,salesRecordWritable.getTransactionId());
         row.set(1,salesRecordWritable.getCustomerId());
         row.set(2,salesRecordWritable.getItemId());
         row.set(3,salesRecordWritable.getItemValue());
         return  row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }
}

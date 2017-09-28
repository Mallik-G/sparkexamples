package com.bigdatasolutions.spark.sparksql.hiveintegration;


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


/**
 * Created by madhu on 8/1/15.
 */
public class SumUDAF extends AbstractGenericUDAFResolver {


    public static class SumUDAFEvaluator extends GenericUDAFEvaluator {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;//ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        }


        public static class Sum extends AbstractAggregationBuffer {
            double rows_sum = 0;
        }


        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new Sum();
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            if (aggregationBuffer == null) return;
            Sum sum = (Sum) aggregationBuffer;
            sum.rows_sum = 0;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            Sum sum = (Sum) aggregationBuffer;
            double value = (Double)objects[0];
            sum.rows_sum += value;

        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            System.out.println("in terminatePartial");
            Sum sum = (Sum) aggregationBuffer;
            return new DoubleWritable(sum.rows_sum);
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object other) throws HiveException {
            if (other == null) return;
            Sum sum = (Sum) aggregationBuffer;
            if (other instanceof DoubleWritable) {
                DoubleWritable doubleWritable = (DoubleWritable) other;
                sum.rows_sum += doubleWritable.get();
            } else if(other instanceof LazyDouble){
                double value = ((LazyDouble) other).getWritableObject().get();
                sum.rows_sum += value;
            }


        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            Sum sum = (Sum) aggregationBuffer;
            double resultDouble = sum.rows_sum;
            return new DoubleWritable(resultDouble);
        }


    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return new SumUDAFEvaluator();
    }
}

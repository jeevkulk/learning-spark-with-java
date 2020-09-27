package jeevkulk.sparksql.functions;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public class MyAverage extends UserDefinedAggregateFunction {

    @Override
    public StructType inputSchema() {
        return null;
    }

    @Override
    public StructType bufferSchema() {
        return null;
    }

    @Override
    public DataType dataType() {
        return null;
    }

    /**
     * Indicates if the program returns the same output for given input
     * @return
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {

    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {

    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

    }

    @Override
    public Object evaluate(Row buffer) {
        return null;
    }
}

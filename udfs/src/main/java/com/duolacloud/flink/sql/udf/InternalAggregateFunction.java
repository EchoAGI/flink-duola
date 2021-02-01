package com.duolacloud.flink.sql.udf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import static org.apache.flink.table.types.inference.TypeStrategies.explicit;

@Internal
public abstract class InternalAggregateFunction<T, ACC> extends AggregateFunction<T, ACC> {

    public abstract DataType[] getInputDataTypes();

    public abstract DataType getAccumulatorDataType();

    public abstract DataType getOutputDataType();

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .typedArguments(getInputDataTypes())
                .accumulatorTypeStrategy(explicit(getAccumulatorDataType()))
                .outputTypeStrategy(explicit(getOutputDataType()))
                .build();
    }
}

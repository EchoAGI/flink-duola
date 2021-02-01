package com.duolacloud.flink.sql.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * 聚合 array 对象
 */


public class ArrayAggregateFunction extends AggregateFunction<Row[], List<Row>> {
    private final TypeInformation<?>[] types;

    public ArrayAggregateFunction(TypeInformation ...types) {
        Preconditions.checkNotNull(types, "Type information");
        this.types = types;
    }

    @Override
    public List<Row> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public Row[] getValue(List<Row> acc) {
        return acc.toArray(new Row[0]);
    }

    public void merge(List<Row> accumulator, Iterable<List<Row>> iterable) {
        for (List<Row> otherAcc : iterable) {
            accumulator.addAll(otherAcc);
        }
    }

    public void accumulate(List<Row> acc, Object... columns) {
        System.out.printf("accumulate: %s\n", columns);
        acc.add(Row.of(columns));
    }

    public void retract(List<Row> acc, Object... columns) {
        System.out.printf("retract\n");
    }

    @Override
    public TypeInformation<Row[]> getResultType() {
        return Types.OBJECT_ARRAY(Types.ROW(types));
    }

    @Override
    public TypeInformation<List<Row>> getAccumulatorType() {
        Class<List<Row>> clazz = (Class<List<Row>>) (Class) List.class;
        return TypeInformation.of(clazz);
    }
}

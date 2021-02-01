package com.duolacloud.flink.sql.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * 聚合 array 对象
 */


public class ArrayAggregateFunction extends InternalAggregateFunction<ArrayData, List<RowData>> {
    @Override
    public List<RowData> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public ArrayData getValue(List<RowData> acc) {
        GenericArrayData arr = new GenericArrayData(acc.toArray(new RowData[0]));
        return arr;
    }

    public void merge(List<RowData> accumulator, Iterable<List<RowData>> iterable) {
        for (List<RowData> otherAcc : iterable) {
            accumulator.addAll(otherAcc);
        }
    }

    public void accumulate(List<RowData> acc, Object... columns) {
        System.out.printf("accumulate: %s\n", columns);
        GenericRowData row = new GenericRowData(columns.length);
        for (int i=0;i<columns.length;i++) {
            row.setField(i, columns[i]);
        }
        acc.add(row);
    }

    public void retract(List<Row> acc, RowData value) {
        System.out.printf("retract\n");
    }

    @Override
    public DataType[] getInputDataTypes() {
        return new DataType[]{DataTypes.ROW().bridgedTo(RowData.class)};
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.ARRAY(DataTypes.ROW());
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.ARRAY(DataTypes.ROW()).bridgedTo(ArrayData.class);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}

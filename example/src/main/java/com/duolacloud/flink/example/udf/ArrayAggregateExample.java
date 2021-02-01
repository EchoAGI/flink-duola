package com.duolacloud.flink.example.udf;

import com.duolacloud.flink.sql.udf.ArrayAggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ArrayAggregateExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.registerFunction("ARRAY_AGG", new ArrayAggregateFunction(
                Types.STRING,
                Types.DOUBLE,
                Types.LONG
        ));

        tEnv.executeSql("CREATE TABLE orders(\n" +
                "  id STRING PRIMARY KEY NOT ENFORCED,\n" +
                "  user_id STRING,\n" +
                "  amount DECIMAL,\n" +
                "  status STRING,\n" +
                "  channel STRING,\n" +
                "  ctime TIMESTAMP,\n" +
                "  utime TIMESTAMP,\n" +
                "  proc_time AS PROCTIME()" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'debezium',\n" +
                "  'database-name' = 'ec',\n" +
                "  'table-name' = 'orders'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE order_items(\n" +
                "  id STRING PRIMARY KEY NOT ENFORCED,\n" +
                "  order_id STRING,\n" +
                "  product_id STRING,\n" +
                "  quantity BIGINT,\n" +
                "  price DECIMAL,\n" +
                "  amount DECIMAL,\n" +
                "  ctime TIMESTAMP,\n" +
                "  utime TIMESTAMP,\n" +
                "  proc_time AS PROCTIME()" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'debezium',\n" +
                "  'database-name' = 'ec',\n" +
                "  'table-name' = 'order_items'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE order_view_items (\n" +
                "  id STRING PRIMARY KEY NOT ENFORCED,\n" +
                "  `order.items` ARRAY<ROW<`product.id` STRING, price DOUBLE, quantity BIGINT>>,\n" +
                "  proc_time AS PROCTIME()" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'order_view'\n" +
                ")");

        tEnv.executeSql("INSERT INTO order_view_items SELECT order_id, ARRAY_AGG(product_id, price, quantity) FROM order_items GROUP BY order_id");

        try {
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

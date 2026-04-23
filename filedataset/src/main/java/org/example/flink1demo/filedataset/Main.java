package org.example.flink1demo.filedataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("flink1demo filedataset...");

        // 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 读取数据
        DataSource<String> lines = env.readTextFile("words.txt");

        // 分词
        FlatMapOperator<String, Tuple2<String, Integer>> wordPairs = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");

                for (String word : words) {
                    Tuple2<String, Integer> t = Tuple2.of(word, 1);
                    out.collect(t);
                }
            }
        });

        // 分组
        UnsortedGrouping<Tuple2<String, Integer>> wordGroups = wordPairs.groupBy(0);

        // 统计
        AggregateOperator<Tuple2<String, Integer>> wordCount = wordGroups.sum(1);

        // 打印结果
        wordCount.print("filedataset");

        System.out.println("flink1demo filedataset...done.");
    }
}
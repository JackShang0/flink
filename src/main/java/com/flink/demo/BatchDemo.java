package com.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @description: DataSet 读取文件
 * @author: shangqj
 * @date: 2025/1/15
 * @version: 1.0
 */
public class BatchDemo {

    /*
                需要注意的是，这种代码实现方式，是基于DataSet API实现的，也就是我们对数据的处理转换，是看作数据集来操作的。
            事实上，flink本身就是一种流批统一的处理框架，批量是数据集，本质上也是一种流，没有必要用两套api来实现。
                所以在flink1.2开始，官方推荐的方法是 直接使用DataStream API，在提交任务时通过将执行模式设为BATCH来进行
            批处理
                因此DataSet API就没什么用了
     */
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据
        DataSource<String> source = env.readTextFile("D:\\workspace2\\code\\2024\\github\\flink\\flink\\input\\word.txt");

        // 3.按行切分、转换（world，1）
        FlatMapOperator<String, Tuple2<String, Integer>> tupleResult = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //3.1 切割单词
                String[] words = value.split(" ");
                for (String word : words) {
                    //3.2 将单词转换为二元组  tuple2 的格式
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    //3.3 使用 collector 收集结果
                    collector.collect(tuple2);
                }
            }
        });

        // 4.按照单词分组  传入参数为tuple的字段索引
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = tupleResult.groupBy(0);

        // 5.各分组内聚合     传入参数为按照索引位置聚合求和
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        // 6.输出结果
        sum.print();

    }
}

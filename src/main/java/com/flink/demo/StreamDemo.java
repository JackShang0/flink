package com.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description:  DataStream 读取文件
 * @author: shangqj
 * @date: 2025/1/16
 * @version: 1.0
 */
public class StreamDemo {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("D:\\workspace2\\code\\2024\\github\\flink\\flink\\input\\word.txt");

        // 3.数据处理：切分单词、转换、分组、聚合
        //  3.1切换、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 按照空格切分
                String[] words = s.split(" ");

                // 转换成二元组 (word,1)
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    //通过采集器 收集结果
                    collector.collect(tuple2);
                }
            }
        });

        //  3.2 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = outputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;    //按照值的0位置进行分组
            }
        });


        //  3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        // 4.输出数据
        sum.print();

        // 5.执行，启动执行环境
        env.execute();


        /*
            5> (world,1)
            2> (java,1)
            3> (hello,1)
            3> (hello,2)
            7> (flink,1)
            3> (hello,3)

            1、与批处理的不同，能体现出整个数据是流的一个处理方式
            2、并且flink有自己的存储
            3、数字> 是体现的并行度
         */

    }
}

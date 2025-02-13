package com.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:  Socket 无界流数据的读取
 * @author: shangqj
 * @date: 2025/1/16
 * @version: 1.0
 */
public class StreamUnboundDemo {


    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);

        // 3.处理数据：切分、转换、聚合
        streamSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word,1));
            }
        })

                //java 的lambda表达式存在泛型丢失的问题，在flink中无法识别，需要做以下操作指定返回的泛型，对于二元组要分别指定泛型
                .returns(Types.TUPLE(Types.STRING,Types.INT))

                .keyBy(stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .sum(1)
                .print();

        env.execute();
    }
}

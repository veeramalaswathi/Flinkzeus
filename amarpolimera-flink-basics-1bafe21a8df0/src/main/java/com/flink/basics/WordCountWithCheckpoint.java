package com.flink.basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Paths;

public class WordCountWithCheckpoint {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        String checkPointPath = parameterTool.get("checkpoint-path");
        if (checkPointPath == null || checkPointPath.trim().isEmpty()) {
            throw new IllegalArgumentException("checkpoint-path is mandatory for storing state");
        }

        // Required to support recovery from checkpoint
        // MUST to Restart a job from a checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // File based Backend
        env.setStateBackend(new FsStateBackend(Paths.get(checkPointPath).toUri(), false));
        // Not to confuse the new flink users with more than one parallelism
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("localhost", 9999);
        DataStream<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        DataStream<Tuple2<String, Integer>> wordCount = words.keyBy((s) -> s).process(new StatefulReduceFunc());

        wordCount.print();
        env.execute("Word count streaming with state example");
    }

    private static class StatefulReduceFunc extends KeyedProcessFunction<String, String, Tuple2<String, Integer>> {

        private transient ValueState<Integer> count;

        public void processElement(String s, Context context, Collector<Tuple2<String, Integer>>
                collector) throws Exception {
            int currentCnt = count.value() == null ? 1 : 1 + count.value();
            count.update(currentCnt);
            collector.collect(new Tuple2<>(s, currentCnt));
        }

        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> valueStateDescriptor =
                    new ValueStateDescriptor<Integer>("count", Integer.class);
            count = getRuntimeContext().getState(valueStateDescriptor);
        }
    }
}

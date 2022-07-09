package com.flink.basics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.flink.basics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class FlinkJob {

    private static final Logger log = LoggerFactory.getLogger(FlinkJob.class);
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        String checkPointPath = "/tmp/wk-test";//parameterTool.get("checkpoint-path");
        if (checkPointPath == null || checkPointPath.trim().isEmpty()) {
            throw new IllegalArgumentException("checkpoint-path is mandatory for storing state");
        }

        // Required to support recovery from checkpoint
        // MUST to Restart a job from a checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // File based Backend
       // env.setStateBackend(new FsStateBackend(Paths.get(checkPointPath).toUri(), false));
        // Not to confuse the new flink users with more than one parallelism
        env.setParallelism(1);
     //   DataStream<String> text = env.socketTextStream("localhost", 9999);

        String path = "/Users/swathi.veeramalla/Downloads/amarpolimera-flink-basics-1bafe21a8df0/test-data/test.json";
        final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineFormat(), new Path(path))
                .processStaticFileSet()
                .build();

        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        DataStream<Tuple1<String>> text = dataStream.map(new FileInitialMap());


        DataStream<ZeusEvent> words = text
                .map(new ZeusRawEventsMapper())
                .filter(new ZeusFilterFunction(parameterTool.getLong("events-threshold-count",100)))
                .setParallelism(1)
                .name("Zeus Kafka Source");
        //words.print();

        DataStream<Tuple2<ZeusEvent, Integer>> wordCount = words.keyBy(new ZeusRawKeyByFunction())
                .process(new KeyedProcessFunction<String, ZeusEvent, Tuple2<ZeusEvent, Integer>>() {
                    private transient ValueState<Integer> crashcount;
                    private transient ValueState<Integer> startcount;

                    private transient ValueState<Long> startTs;
            @Override
                public void open (Configuration parameters){
                    ValueStateDescriptor<Integer> valueStateDescriptor =
                            new ValueStateDescriptor<Integer>("count", Integer.class);
                crashcount = getRuntimeContext().getState(valueStateDescriptor);
                ValueStateDescriptor<Integer> statusStateDescriptor =
                        new ValueStateDescriptor<>("status-state", Integer.class);
                startcount = getRuntimeContext().getState(statusStateDescriptor);

                startTs = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-start", Long.class));

                }

                @Override
                public void processElement(ZeusEvent zeusEvent,
                                           KeyedProcessFunction<String, ZeusEvent, Tuple2<ZeusEvent, Integer>>.Context context,
                                           Collector<Tuple2<ZeusEvent, Integer>> collector) throws Exception {
                Long stTs = startTs.value();
                if(stTs == null ){
                    startTs.update(System.currentTimeMillis());
                }
                if(zeusEvent.getEventTypeName().equalsIgnoreCase("videoheartbeat")) {
                    int currentCnt = crashcount.value() == null ? 1 : 1 + crashcount.value();
                    crashcount.update(currentCnt);
                    collector.collect(new Tuple2<>(zeusEvent, currentCnt));
                }else if(zeusEvent.getEventTypeName().equalsIgnoreCase("seekended")){
                    int cCnt = startcount.value() == null ? 1 : 1 + startcount.value();
                    startcount.update(cCnt);
                    System.out.println(zeusEvent.getEventTypeName()+"::"+crashcount.value()+"::"+startcount.value());
                }
                if(crashcount.value() !=null && startcount.value() != null ){
                        double crashPC = (crashcount.value() / startcount.value()) * 100;
                        System.out.println("Crash/Open percent: " + crashPC);
                        log.info("Crash/Open percent: " + crashPC);
                }
                    // collector.collect(new Tuple2<>(ZeusEvent, currentCnt));

            }
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, ZeusEvent,
                    Tuple2<ZeusEvent, Integer>>.OnTimerContext context, Collector<Tuple2<ZeusEvent, Integer>> collector) throws Exception {
                long CLEAN_UP_INTERVAL = 24 * 60 * 60 * 100 ;

                Long stTs = startTs.value();
                if(stTs != null && stTs >= timestamp - CLEAN_UP_INTERVAL){
                    startTs.clear();
                    crashcount.clear();
                    startcount.clear();
                }

            }
        });

        wordCount.print();
        env.execute("App Stat Test");
    }
}

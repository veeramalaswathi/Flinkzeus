package com.flink.basics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class FlinkJob1 {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.getConfig().registerPojoType(AppStats.class);
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


        DataStream<ZeusEvent> stats = text
                .map(new ZeusRawEventsMapper())
                .filter(new ZeusFilterFunction(parameterTool.getLong("events-threshold-count",100)))
                .setParallelism(1)
                .name("Zeus Kafka Source");
        //stats.print();

        DataStream<Tuple2<ZeusEvent, AppStats>> appCrashPercent = stats.keyBy(new ZeusRawKeyByFunction())
                .process(new AppStatProcessFunction());

        appCrashPercent.print();
        env.execute("App Stats Counter ");
    }
}


class AppStatProcessFunction extends KeyedProcessFunction<String, ZeusEvent, Tuple2<ZeusEvent, AppStats>>{

    private static final Logger log = LoggerFactory.getLogger(AppStatProcessFunction.class);
    //private transient ValueState<Integer> crashcount;
    //private transient ValueState<Integer> startcount;
    //private transient ValueState<Long> startTs;
    static final String APP_CRASH_STR = "seekended";
    static final String APP_START_STR ="videoheartbeat";

    private transient ValueState<AppStats> appStatsState;

    @Override
    public void open (Configuration parameters){
        appStatsState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-start", AppStats.class));
    }

    @Override
    public void processElement(ZeusEvent zeusEvent,
                               KeyedProcessFunction<String, ZeusEvent, Tuple2<ZeusEvent, AppStats>>.Context context,
                               Collector<Tuple2<ZeusEvent, AppStats>> collector) throws Exception {

        AppStats  appStats = appStatsState.value();
        if(appStats == null ){
            appStats =  new AppStats();
            appStats.setStartTime(System.currentTimeMillis());
        }

        if(zeusEvent.getEventTypeName().equalsIgnoreCase(APP_START_STR)) {
            int startCount = appStats == null ? 1 : 1 + appStats.getStartCount();
            appStats.setStartCount(startCount);

        }
        else if(zeusEvent.getEventTypeName().equalsIgnoreCase(APP_CRASH_STR)){
            int crashCount = appStats == null ? 1 : 1 + appStats.getCrashCount();
            appStats.setCrashCount(crashCount);
        }

        if(appStats != null && appStats.getStartCount() > 0){
            int percent = appStats.getCrashCount() / appStats.getStartCount() *100;
            appStats.setPercent(percent);
            //System.out.println("Crash/Open percent: " + percent);
            log.info("Crash/Open percent: " + percent);
        }
        appStatsState.update(appStats);
        collector.collect(new Tuple2<>(zeusEvent, appStats));
    }
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, ZeusEvent, Tuple2<ZeusEvent, AppStats>>.OnTimerContext context, Collector<Tuple2<ZeusEvent, AppStats>> collector) throws Exception {
        long CLEAN_UP_INTERVAL = 24 * 60 * 60 * 1000 ;
        AppStats appstats = appStatsState.value();
        if(appstats != null &&  appstats.getStartTime() >= timestamp - CLEAN_UP_INTERVAL){
            appStatsState.clear();;
        }
    }
}

class AppStats{
    private int startCount;
    private int crashCount;
    private long startTime;

    private double percent;

    public AppStats() {
    }

    public int getStartCount() {
        return startCount;
    }

    public void setStartCount(int startCount) {
        this.startCount = startCount;
    }

    public int getCrashCount() {
        return crashCount;
    }

    public void setCrashCount(int crashCount) {
        this.crashCount = crashCount;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public double getPercent() {
        return percent;
    }

    public void setPercent(double percent) {
        this.percent = percent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AppStats appStats = (AppStats) o;
        return startCount == appStats.startCount && crashCount == appStats.crashCount && startTime == appStats.startTime && Double.compare(appStats.percent, percent) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startCount, crashCount, startTime, percent);
    }
}

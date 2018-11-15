package cn.lee.demo.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.TimeUnit;


public class OperatorStateTest {

    public static void main(String argsp[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FsStateBackend backend = new FsStateBackend("file:///E///tmp///flink", true);
        System.out.println(backend.getCheckpointPath().getPath());
        env.setStateBackend(backend);
// start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
// advanced options:
// set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
// make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
// checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(2, Time.seconds(10), Time.seconds(10)));
// allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
// enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
        env.setParallelism(1);
        env.fromElements("1", "2", "3","1" ,"4", "5", "6", "7","1" , "8", "9", "10", "1" ,"11", "12", "13", "14", "15", "16", "17", "18", "19","1", "20").map(new MapFunction<String, Long>() {
            @Override
            public Long map(String s) throws Exception {
                return Long.parseLong(s);
            }
        })
                .flatMap(new FlatMapOperator())
                .print();
        env.execute("OperatorStateTest1");
    }
}

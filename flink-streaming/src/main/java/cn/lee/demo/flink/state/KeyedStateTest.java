package cn.lee.demo.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class KeyedStateTest {

    public static void main(String argsp[]) throws Exception {

        /**
         *
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        env.enableCheckpointing(20000);
        env.setStateBackend(new MemoryStateBackend(100,true));
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L),Tuple2.of(1L, 12L),
                        Tuple2.of(2L, 3L), Tuple2.of(2L, 5L), Tuple2.of(2L, 7L), Tuple2.of(2L, 4L), Tuple2.of(2L, 2L),Tuple2.of(2L, 12L)
//                Tuple2.of(3L, 3L), Tuple2.of(3L, 3L), Tuple2.of(3L, 3L),
//                Tuple2.of(4L, 5L), Tuple2.of(4L, 5L),Tuple2.of(4L, 5L),
//                Tuple2.of(5L, 7L),   Tuple2.of(5L, 7L),  Tuple2.of(5L, 7L),
//                Tuple2.of(6L, 4L), Tuple2.of(6L, 4L),Tuple2.of(6L, 4L),
//                Tuple2.of(7L, 2L),  Tuple2.of(7L, 2L),  Tuple2.of(7L, 2L)
                )
                .keyBy(0)
                .flatMap(new CountWindowAggregate()).uid("it is my job id")
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        System.out.println(value+"("+Thread.currentThread().getName()+")");
                    }
                }).disableChaining();

        env.execute("KeyedStateTest");
    }

}

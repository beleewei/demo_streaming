package cn.lee.demo.flink.state;

import cn.lee.demo.flink.Const;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import scala.util.parsing.json.JSONFormat;
import scala.util.parsing.json.JSONObject;

import java.util.Date;
import java.util.List;


public class OperatorStateTest {

    public static void main(String argsp[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(100000);
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(100000L);
        checkpointConf.setCheckpointTimeout(50000L);
        checkpointConf.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        env.fromElements("A","B","C","D","a","C","D","E","F","f","D","E","H","e","R","d","S","T","x")
//                .map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String s) throws Exception {
//                        return s;
//                    }
//                })
                .flatMap(new CountWithOperatorState())
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        System.out.println(value);
                    }
                });
        env.execute("OperatorStateTest");
    }
    private static class CountWithOperatorState extends RichFlatMapFunction<String,String> implements CheckpointedFunction {

        private transient ListState<String> checkPointCountList;
        private List<String> listBufferElements;

        @Override
        public void flatMap(String c, Collector<String> collector) throws Exception {
            System.out.println(c);
            if (!c.toUpperCase().equals(c)) {
                if (listBufferElements.size() > 0) {
                    StringBuffer buffer = new StringBuffer();
                    for(int i = 0 ; i < listBufferElements.size(); i ++) {
                        buffer.append(listBufferElements.get(i) + " ");
                    }
                    collector.collect(buffer.toString());
                    System.out.println(buffer);
                    listBufferElements.clear();
                }
            } else {
                listBufferElements.add(c);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkPointCountList.clear();
            for (int i = 0 ; i < listBufferElements.size(); i ++) {
                checkPointCountList.add(listBufferElements.get(i));
            }
            System.out.println("do snapshot at "+ Const.format.format(new Date())+"{"+ JSONFormat.defaultFormatter().apply(checkPointCountList));
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            ListStateDescriptor<String> listStateDescriptor =
                    new ListStateDescriptor<String>(
                            "listForThree",
                            TypeInformation.of(new TypeHint<String>() {}));

            checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
            if (functionInitializationContext.isRestored()) {
                for (String element : checkPointCountList.get()) {
                    listBufferElements.add(element);
                }
            }
            System.out.println("do initializeState at "+ Const.format.format(new Date())+"{"+ JSONFormat.defaultFormatter().apply(listBufferElements));
        }
    }

}

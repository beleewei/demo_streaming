package cn.lee.demo.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Project Name:demo_streaming
 * Package Name:cn.lee.demo.flink.state
 * ClassName: RichMapOpertor &lt;br/&gt;
 * date: 2018/11/14 18:26 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class RichMapOpertor extends RichFlatMapFunction<String, String> implements CheckpointedFunction{
    ListState<String> listState;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
       String s= Thread.currentThread().getName()+"|"+System.currentTimeMillis();
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<String>(
                "listStateName1",
                TypeInformation.of(new TypeHint<String>() {}));
        listState.addAll(Arrays.asList(s));
        StringBuffer sb=new StringBuffer();
        listState.get().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                sb.append(s).append("\n");
            }
        });
        System.out.println("---------checkpoint by "+s+"------------");
        System.out.println(sb.toString());
        System.out.println("**********" +s+"**************************");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<String>(
                        "listStateName1",
                        TypeInformation.of(new TypeHint<String>() {}));
       this.listState=context.getOperatorStateStore().getListState(listStateDescriptor);


    }

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        AtomicInteger i=new AtomicInteger(0);
        listState.get().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                i.getAndIncrement();
            }
        });
        collector.collect( i.get()+"_"+s+"("+Thread.currentThread().getName()+")");
    }
}

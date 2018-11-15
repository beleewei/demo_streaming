package cn.lee.demo.flink.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Project Name:demo_streaming
 * Package Name:cn.lee.demo.flink.state
 * ClassName: CountWindowAggregate &lt;br/&gt;
 * date: 2018/11/14 19:18 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class CountWindowAggregate extends RichFlatMapFunction<Tuple2<Long, Long>, String> implements CheckpointedFunction {


    private transient ValueState<Tuple2<Long, Long>> sum;
    private transient ReducingState<Tuple2<Long, Long>> min;
    private ListState<String> globleListState;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<String> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();
        if (currentSum == null) {
            currentSum = new Tuple2<Long, Long>(0l, 0l);
        }
        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);
        min.add(input);
//        System.out.println(Thread.currentThread().getName()+"|"+currentSum);
        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 3) {
//            this.globleListState.add(input.toString());
//            Iterator<String> itr = globleListState.get().iterator();
//            int i = 0;
//            while (itr.hasNext()) {
//                i += 1;
////                if (i>1000){
////                    break;
////                }
//            }
//            System.out.println(i);

            String msg = "key=" + input.f0 + "," + "avg=" + (currentSum.f1 / currentSum.f0) + ",min=" + min.get().f1 ;
            out.collect(msg);
            sum.clear();
            min.clear();
        }

    }

    @Override
    public void open(Configuration config) {
//        min=getRuntimeContext().getReducingState(getReducingStateDescriptor());
//        sum = getRuntimeContext().getState(getValueStateDescriptor());
//        System.out.println("do open ("+Thread.currentThread().getName()+")");
    }

    public ValueStateDescriptor<Tuple2<Long, Long>> getValueStateDescriptor() {
        return new ValueStateDescriptor<Tuple2<Long, Long>>("averageKeydState", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        }));
    }

    public ReducingStateDescriptor getReducingStateDescriptor() {
        return new ReducingStateDescriptor("maxKeydState", new ReduceFunction<Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> reduce(Tuple2<Long, Long> o, Tuple2<Long, Long> t1) throws Exception {
                return o.f1 > t1.f1 ? t1 : o;
            }
        }, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        }));
    }

    public ListStateDescriptor<String> getListStateDescriptor() {
        return new ListStateDescriptor<String>(
                "globleListState",
                TypeInformation.of(new TypeHint<String>() {
                }));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        System.out.println("do snapshotState (" + Thread.currentThread().getName() + ")");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        sum = context.getKeyedStateStore().getState(getValueStateDescriptor());
        min = context.getKeyedStateStore().getReducingState(getReducingStateDescriptor());
        this.globleListState = context.getOperatorStateStore().getListState(getListStateDescriptor());

        System.out.println("do initializeState (" + Thread.currentThread().getName() + ")");
    }
}


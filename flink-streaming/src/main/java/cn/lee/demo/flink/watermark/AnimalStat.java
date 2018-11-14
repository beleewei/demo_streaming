package cn.lee.demo.flink.watermark;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.mutable.StringBuilder;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Project Name:demo_streaming
 * Package Name:cn.lee.demo.flink.watermark
 * ClassName: AnimalStat &lt;br/&gt;
 * date: 2018/11/14 13:42 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class AnimalStat {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setBufferTimeout(1);
        env.getConfig().setGlobalJobParameters(params);
        List<Animal> dataSet=Arrays.asList(
                new Animal("dog", "2018-11-01 12:07"),
                new Animal("owl", "2018-11-01 12:08"),
                new Animal("dog", "2018-11-01 12:14"),
                new Animal("cat", "2018-11-01 12:09"),
                new Animal("cat", "2018-11-01 12:15"),
                new Animal("dog", "2018-11-01 12:08"),
                new Animal("owl", "2018-11-01 12:13"),
                new Animal("owl", "2018-11-01 12:21"),
                new Animal("owl", "2018-11-01 12:26"),
                new Animal("dog", "2018-11-01 12:04"),
                new Animal("owl", "2018-11-01 12:17"),
                new Animal("cat", "2018-11-01 12:09")
        );
        final SimpleDateFormat fmt=new SimpleDateFormat("yyyy-MM-dd HH:mm");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Animal> inputStream=
        env.addSource(new SourceFunction<Animal>() {
            @Override
            public void run(SourceContext<Animal> sourceContext) throws Exception {
                int i=0;
                while(true){
                    if (i>=dataSet.size()){
                        cancel();
                        break;
                    }
                    sourceContext.collect(dataSet.get(i));
                    i++;
                    TimeUnit.SECONDS.sleep(1);

                }
            }

            @Override
            public void cancel() {

            }
        });
        inputStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Animal>(Time.minutes(10)) {
            @Override
            public long extractTimestamp(Animal animal) {
                long t=0;
                try {
                    t= fmt.parse(animal.getEventTime()).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                System.out.println(t+"|"+animal+"|"+fmt.format(new Date(super.getCurrentWatermark().getTimestamp())));
                return t;
            }
        }).keyBy(new KeySelector<Animal, String>() {
            @Override
            public String getKey(Animal animal) throws Exception {
                return animal.getName();
            }
        })
                .window(SlidingEventTimeWindows.of(Time.minutes(10),Time.minutes(5)))
                .apply(new WindowFunction<Animal, String, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow timeWindow, Iterable<Animal> iterable, Collector<String> collector) throws Exception {
                AtomicInteger i=new AtomicInteger(0);
                StringBuilder sb=new StringBuilder();
                iterable.forEach(new Consumer<Animal>() {
                    @Override
                    public void accept(Animal animal) {
                        sb.append(animal.toString());
                        i.incrementAndGet();
                    }
                });
                SimpleDateFormat format=new SimpleDateFormat("HH:mm");
                String start=null;
                String end=null;
                String max=null;
                try {
                    start = format.format(new Date(timeWindow.getStart()));
                    end = format.format(new Date(timeWindow.getEnd()));
                    max= format.format(new Date(timeWindow.maxTimestamp()));
                }catch (Exception e){
                    System.out.println("fmt error "+key+"|"+timeWindow.getStart()+"|"+timeWindow.getEnd()+"|"+max);
                }
                String msg=start+"-"+end+"|"+max+"|"+key+"|"+i.get()+"|"+sb.toString();
//                System.out.println(msg);
                collector.collect(msg);
            }
        }).print();

        env.execute("window demo of animal count");
    }



}

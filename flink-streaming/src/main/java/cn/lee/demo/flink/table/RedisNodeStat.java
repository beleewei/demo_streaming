package cn.lee.demo.flink.table;

import cn.lee.demo.flink.Const;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Project Name:demo_streaming
 * Package Name:cn.lee.demo.flink.table
 * ClassName: RedisNodeStat &lt;br/&gt;
 * date: 2018/11/6 9:00 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class RedisNodeStat {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!");
            System.out.println("Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
            Map<String, String> param=new HashMap<>();
            param.put("bootstrap.servers", Const.KAFKA_SERVER_IP + ":" + Const.KAFKA_SERVER_PORT);
            param.put("topic", Const.TOPIC_REDIS_INPUT);
            param.put("group.id",Const.TOPIC_REDIS_INPUT+"_consume1");
            param.put("auto.offset.reset","earliest");
            param.put("client.id",Const.TOPIC_REDIS_INPUT+"_consume1");
            parameterTool=parameterTool.fromMap(param);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        TypeInformationSerializationSchema<RedisNode> type=new TypeInformationSerializationSchema<RedisNode>(PojoTypeInfo.of(RedisNode.class),env.getConfig());

        FlinkKafkaConsumer010 source=new FlinkKafkaConsumer010<RedisNode>(parameterTool.getRequired("topic"),type ,parameterTool.getProperties());

        // very simple data generator
        DataStream<RedisNode> messageStream = env.addSource(source).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RedisNode>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(RedisNode o) {
                return o.getTime();
            }
        });
        messageStream.keyBy("role").window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))).maxBy("tps").print();
        // write data into Kafka
//        messageStream.print();
        env.execute("Write into Kafka example");
    }
}

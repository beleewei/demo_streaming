package cn.lee.demo.flink.table;

import cn.lee.demo.flink.Const;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

import java.util.HashMap;
import java.util.Map;

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
   public static ParameterTool parameterTool=null;
    public static void main(String[] args) throws Exception {
        parameterTool = ParameterTool.fromArgs(args);

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


//        withStreamAPI(createKafkaSource(env));
//withTableAPI(createKafkaSource(env),env);
        withSQLAPI(createKafkaSource(env),env);
        env.execute("RedisNodeStat");
    }

  private static DataStream<RedisNode> createKafkaSource(  StreamExecutionEnvironment env){
      TypeInformationSerializationSchema<RedisNode> type=new TypeInformationSerializationSchema<RedisNode>(PojoTypeInfo.of(RedisNode.class),env.getConfig());
      FlinkKafkaConsumer010 source=new FlinkKafkaConsumer010<RedisNode>(parameterTool.getRequired("topic"),type ,parameterTool.getProperties());
      // very simple data generator
      DataStream<RedisNode> stream = env.addSource(source).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RedisNode>(Time.seconds(10)) {
          @Override
          public long extractTimestamp(RedisNode o) {
              return o.getEvtTime();
          }
      });
      return stream;
  }


    private static void withStreamAPI(DataStream<RedisNode> stream ){
        stream .keyBy("role").window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))).maxBy("tps").print();
    }
    private static void withTableAPI(DataStream<RedisNode> stream ,StreamExecutionEnvironment env){
        StreamTableEnvironment tEnv= TableEnvironment.getTableEnvironment(env);
        Table redisNode= tEnv.fromDataStream(stream,"evtTime,tps,inputBytes,outputBytes,size,role,address");
        redisNode.printSchema();
        WindowedTable window= redisNode.window(Tumble.over("5.seconds").on("evtTime").as("redisWindow"));
        System.out.println(window.window().alias()+"|"+window.window().timeField());

        Table result= window.groupBy("redisWindow,role").select("role,tps.max");
        tEnv.toAppendStream(result,result.getSchema().toRowType()).print();
    }
    private static void withSQLAPI(DataStream<RedisNode> stream ,StreamExecutionEnvironment env){
        StreamTableEnvironment tEnv= TableEnvironment.getTableEnvironment(env);
        tEnv.registerDataStream("redis", stream, "evtTime,tps,inputBytes,outputBytes,size,role,proctime.proctime,rowtime.rowtime");
        //TUMBLE_END(rowtime, INTERVAL '5' SECOND) AS evtTime,
        String sql ="SELECT role,AVG(inputBytes) as inputBytes, MAX(tps) as tps  FROM redis GROUP BY TUMBLE(rowtime, INTERVAL '5' SECOND) ,role";

        Table result = tEnv.sqlQuery(sql);
        tEnv.toAppendStream(result,result.getSchema().toRowType()).print();
    }
}

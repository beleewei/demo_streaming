package cn.lee.demo.flink.app;

import cn.lee.demo.flink.Const;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import scala.collection.mutable.StringBuilder;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Project Name:flink-parent
 * Package Name:org.apache.flink.streaming.examples.kafka.app
 * ClassName: ActiveUserStat &lt;br/&gt;
 * date: 2018/6/28 11:12 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class ActiveUserStat {

	public static DataStream<LogEvent> mapTransform(DataStream<String> stream) {
		DataStream<LogEvent> out = stream.filter(new FilterFunction<String>() {
			/**
			 * true 不过滤 ，false过滤
			 * @param value The value to be filtered.
			 * @return
			 * @throws Exception
			 */
			@Override
			public boolean filter(String value) throws Exception {
				String[] tokens = value.toLowerCase().split(",");
				if (tokens.length != 6) {
					System.out.println("error log:" + value);
					return false;
				}
				return true;
			}
		}).flatMap(new FlatMapFunction<String, LogEvent>() {
			@Override
			public void flatMap(String value, Collector<LogEvent> out) throws Exception {
				String[] tokens = value.toLowerCase().split(",");
				LogEvent log = new LogEvent();
				log.setId(tokens[0]);
				log.setProductId(tokens[1]);
				log.setProductVersion(tokens[2]);
				log.setChannelId(tokens[3]);
				log.setImei(tokens[4]);
				log.setDealTime(tokens[5]);
				log.setTime(System.currentTimeMillis()+(log.getImei().hashCode()%10)*1000);

				out.collect(log);
			}
		}).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(5)) {
			@Override
			public long extractTimestamp(LogEvent element) {
				System.out.println("event=" + element.getImei() + "| wm=" + super.getCurrentWatermark().getTimestamp()
						+ " |date_timestamp=" + Const.format.format(new Date(element.getTime()))
						+ " |date_watermark=" + Const.format.format(new Date(super.getCurrentWatermark().getTimestamp()))
						+ " | diff=" + (super.getCurrentWatermark().getTimestamp() - element.getTime()) / 1000 + "s"
				);
				return element.getTime();
			}
		});
		return out;
	}
	public static DataStream<String> stat(DataStream<LogEvent> stream){
		return stream.keyBy(new String[]{"productId", "channelId"}).timeWindow(Time.seconds(10)).process(new ProcessWindowFunction<LogEvent, String, Tuple, TimeWindow>() {
			@Override
			public void process(Tuple tuple, Context context, Iterable<LogEvent> iterable, Collector<String> collector) throws Exception {
				Set<String> set=new HashSet<String>();
				StringBuilder sb=new StringBuilder();
				sb.append("[");
				iterable.forEach(new Consumer<LogEvent>() {
					@Override
					public void accept(LogEvent logEvent) {
						set.add(logEvent.getImei());
						sb.append(Const.format.format(new Date(logEvent.getTime()))).append(",");
					}
				});
				sb.append("]");
				StringBuilder msg=new StringBuilder();
				msg.append("(dim=").append(tuple);
				msg.append(",metric=").append(set.size());
				msg.append(")");
				msg.append("(window ");
				msg.append("start=").append(Const.format.format(new Date(context.window().getStart())));
				msg.append(",end=").append(Const.format.format(new Date(context.window().getEnd())));
				msg.append(",maxTime=").append(Const.format.format(new Date(context.window().maxTimestamp())));
				msg.append(",wm=").append(Const.format.format(new Date(context.currentWatermark())));
				msg.append(",processTime=").append(Const.format.format(new Date(context.currentProcessingTime())));
				msg.append(")").append(sb);
				System.out.println(msg.toString());
				collector.collect(msg.toString());
			}
		});

	}

	public static void main(String[] args) throws Exception {
		 ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
				"--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
			Map<String, String> param = new HashedMap();
			param.put("bootstrap.servers", Const.KAFKA_SERVER_IP + ":" + Const.KAFKA_SERVER_PORT);
			param.put("zookeeper.connect", Const.ZK_CLIENT);
			param.put("input-topic", Const.TOPIC_INPUT);
			param.put("output-topic", Const.TOPIC_OUTPUT);
			param.put("group.id", Const.CONSUMER_GROUP_1);
			parameterTool=ParameterTool.fromMap(param);
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();//.getExecutionEnvironment();
//		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
		env.enableCheckpointing(5000 * 60); // create a checkpoint every 1000 *10 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		FlinkKafkaConsumer010 source=new FlinkKafkaConsumer010<String>(parameterTool.getRequired("input-topic"), new SimpleStringSchema(),parameterTool.getProperties());
		FlinkKafkaProducer010 sink=new FlinkKafkaProducer010<String>(parameterTool.getRequired("output-topic"), new SimpleStringSchema(),parameterTool.getProperties());

		DataStream<String> messageStream = env.addSource(source);
		stat(mapTransform(messageStream)).addSink(sink);
		env.execute("count active demo 1");
	}

}

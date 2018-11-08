package cn.lee.demo.flink.table;

import cn.lee.demo.flink.Const;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Project Name:flink-parent
 * Package Name:org.apache.flink.streaming.examples.kafka.app
 * ClassName: NodeProducer &lt;br/&gt;
 * date: 2018/6/28 9:18 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class NodeProducer {
	private static String[] ip = new String[]{"10.23.10.11:7000","10.23.10.11:7001","10.23.10.11:7002","10.23.10.11:7003","10.23.10.11:7004","10.23.10.11:7005"};

	public static RedisNode produceNode() {
		int tps = (int) (Math.random() * 100000);
		int idx=tps%ip.length;
		double inputBytes=Math.random() * 10000000;
		double outBytes=Math.random() * 10000000;
		String role=idx%2==0?"master":"slave";
		String address=ip[idx];
	    long time=System.currentTimeMillis();
		RedisNode node=new RedisNode();
		node.setAddress(address);
		node.setTps(tps);
		node.setSize(time/1000);
		node.setInputBytes(inputBytes);
		node.setOutputBytes(outBytes);
		node.setTime(time);
		node.setRole(role);
		return node;
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 2) {
			System.out.println("Missing parameters!");
			System.out.println("Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
			Map<String, String> param=new HashMap<>();
			param.put("bootstrap.servers", Const.KAFKA_SERVER_IP + ":" + Const.KAFKA_SERVER_PORT);
			param.put("topic", Const.TOPIC_REDIS_INPUT);
			parameterTool=parameterTool.fromMap(param);
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		TypeInformationSerializationSchema<RedisNode> type=new TypeInformationSerializationSchema<RedisNode>(PojoTypeInfo.of(RedisNode.class),env.getConfig());

		FlinkKafkaProducer010 source=new FlinkKafkaProducer010<RedisNode>(parameterTool.getRequired("topic"),type ,parameterTool.getProperties());

		// very simple data generator
		DataStream<RedisNode> messageStream = env.addSource(new SourceFunction<RedisNode>() {
			private static final long serialVersionUID = 6369260445318862378L;
			public boolean running = true;

			@Override
			public void run(SourceContext<RedisNode> ctx) throws Exception {
				while (this.running) {
					RedisNode log = produceNode();
					ctx.collect(log);
					System.out.println(log);
					TimeUnit.SECONDS.sleep(1);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		// write data into Kafka
		messageStream.addSink(source);

		env.execute("Write into Kafka example");
	}

}

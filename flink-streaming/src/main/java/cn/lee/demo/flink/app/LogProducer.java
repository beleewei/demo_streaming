package cn.lee.demo.flink.app;

import cn.lee.demo.flink.Const;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;


import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Project Name:flink-parent
 * Package Name:org.apache.flink.streaming.examples.kafka.app
 * ClassName: LogProducer &lt;br/&gt;
 * date: 2018/6/28 9:18 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class LogProducer {
	private static String[] productIds = new String[]{"1020","04","001","002"};
	private static String[] productVersions = new String[]{"v1","v2","v3"};
	private static String[] channelIds = new String[]{"channel001","channel002","channel003","channel004","channel004"};

	//product_id,product_version,sub_channel_id,imei,deal_time
	public static String produceLog() {
		int num = (int) (Math.random() * 100000);
		StringBuilder str = new StringBuilder();
		String id=UUID.randomUUID().toString().replace("-", "");
		str.append(id).append(",");
		str.append(productIds[num % productIds.length]).append(",");
		str.append(productVersions[num % productVersions.length]).append(",");
		str.append(channelIds[num % productVersions.length]).append(",");
		String imei =id.substring(0, 10) + num;
		str.append(imei).append(",");
		str.append(Const.format.format(new Date()));
		return str.toString();
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 2) {
			System.out.println("Missing parameters!");
			System.out.println("Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
			Map<String, String> param=new HashMap<>();
			param.put("bootstrap.servers", Const.KAFKA_SERVER_IP + ":" + Const.KAFKA_SERVER_PORT);
			param.put("topic", Const.TOPIC_INPUT);
			parameterTool=parameterTool.fromMap(param);
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

		FlinkKafkaProducer010 source=new FlinkKafkaProducer010<String>(parameterTool.getRequired("topic"), new SimpleStringSchema(),parameterTool.getProperties());

		// very simple data generator
		DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 6369260445318862378L;
			public boolean running = true;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (this.running) {
					String log = produceLog();
					ctx.collect(log);
					System.out.println(log);
					//制造重复数据
					if (log.hashCode() % 5 == 0) {
						String[] nodes=log.split(",");
						log=log.replaceAll(nodes[0],UUID.randomUUID().toString().replace("-", ""));
						ctx.collect(log);
					}
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

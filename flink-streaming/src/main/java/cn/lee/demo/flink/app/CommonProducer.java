package cn.lee.demo.flink.app;

import cn.lee.demo.flink.Const;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Project Name:flink-parent
 * Package Name:org.apache.flink.streaming.examples.kafka.app
 * ClassName: CommonProducer &lt;br/&gt;
 * date: 2018/7/18 18:34 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class CommonProducer {
	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		Map<String, String> param = parameterTool.toMap();

		if (parameterTool.getNumberOfParameters() < 2) {
			System.out.println("Missing parameters!");
			System.out.println("Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
			param.put("bootstrap.servers", Const.KAFKA_SERVER_IP + ":" + Const.KAFKA_SERVER_PORT);
			param.put("topic", Const.KAFKA_TOPIC_1);
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

		// very simple data generator
		DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 6369260445318862378L;
			public boolean running = true;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (this.running) {
					String log = "{\"imei\":\""+ UUID.randomUUID().toString().replace("-", "")+"\"}";
					System.out.println(log);
					ctx.collect(log);
					TimeUnit.SECONDS.sleep(1);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});
		FlinkKafkaProducer010 source=new FlinkKafkaProducer010<String>(parameterTool.getRequired("input-topic"), new SimpleStringSchema(),parameterTool.getProperties());

		// write data into Kafka
		messageStream.addSink(source);

		env.execute("Write into Kafka example");
	}
}

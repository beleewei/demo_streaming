package cn.lee.demo.flink.app;

import cn.lee.demo.flink.Const;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.HashMap;
import java.util.Map;

/**
 * Project Name:flink-parent
 * Package Name:org.apache.flink.streaming.examples.kafka.app
 * ClassName: LogConsumer &lt;br/&gt;
 * date: 2018/6/28 9:46 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class LogConsumer {

	public static void main(String[] args) throws Exception {
		// parse input arguments
		 ParameterTool parameterTool = ParameterTool.fromArgs(args);
		if(parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
				"--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
			Map<String, String> param = new HashMap<>();

			param.put("bootstrap.servers", Const.KAFKA_SERVER_IP + ":" + Const.KAFKA_SERVER_PORT);
			param.put("zookeeper.connect", Const.ZK_CLIENT);
			param.put("topic", Const.TOPIC_OUTPUT);
			param.put("group.id", Const.CONSUMER_GROUP_1);
			parameterTool = ParameterTool.fromMap(param);
		}


		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		FlinkKafkaConsumer010 sink=new FlinkKafkaConsumer010<String>(parameterTool.getRequired("topic"), new SimpleStringSchema(),parameterTool.getProperties());

		DataStream<String> messageStream = env.addSource(sink);

		// write kafka stream to standard out.
		messageStream.print();

		env.execute("Read from Kafka example");
	}
}

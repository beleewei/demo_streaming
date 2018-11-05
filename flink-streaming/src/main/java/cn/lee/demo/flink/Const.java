package cn.lee.demo.flink;

import java.text.SimpleDateFormat;

/**
 * Project Name:flink-parent
 * Package Name:PACKAGE_NAME
 * ClassName: Const &lt;br/&gt;
 * date: 2018/6/27 10:27 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class Const {
	public static final String IP="202.182.118.175";
	public static final int SOCKET_PORT=9000;
	public static final String KAFKA_SERVER_IP="202.182.118.175";
	public static final int KAFKA_SERVER_PORT=9092;
	public static final String KAFKA_TOPIC_1="topic";
	public static final String KAFKA_TOPIC_2="topic1";
	public static final String KAFKA_TOPIC_3="topic2";
	public static final String TOPIC_INPUT="topic1";
	public static final String TOPIC_OUTPUT="topic2";
	public static final String ZK_CLIENT="202.182.118.175:2181";
	public static final String CONSUMER_GROUP_1="LogConsumerGroup";
	public static final String CONSUMER_GROUP_2="LogConsumerGroup_count";
	public static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");


}

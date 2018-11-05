package cn.lee.demo.streaming.spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Project Name:demo_streaming
 * Package Name:cn.lee.demo.streaming.spark.kafka
 * ClassName: JavaDirectKafkaWordCount &lt;br/&gt;
 * date: 2018/7/16 13:06 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class JavaDirectKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        Map<String, Object> kafkaParams = new HashMap<>();
        String topics = null;
        if (args.length < 3) {
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Const.KAFKA_SERVER_IP + ":" + Const.KAFKA_SERVER_PORT);
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "SPARK_DEMO_GROUP");
            topics = Const.KAFKA_TOPIC_3;
        } else {
            kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
            kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, args[1]);
            topics = args[2];
        }

        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        StreamingExamples.setStreamingLogLevels();


        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));


        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}

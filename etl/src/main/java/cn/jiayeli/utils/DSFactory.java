package cn.jiayeli.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DSFactory {


    private static ParameterTool parameterTool;


    public static StreamExecutionEnvironment getEnv(Configuration config) {
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
    }

    public StreamExecutionEnvironment getEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }


    /**
     *
     * @param deserialize 用于在Kafka的字节消息和Flink的对象之间转换的键控反/序列化程序。
     * @param configPath kafka配置文件路径
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> FlinkKafkaConsumer<T> createKafkaConsumer (Class<? extends DeserializationSchema> deserialize, String configPath) throws Exception {
        parameterTool.createPropertiesFile(configPath);
        List<String> topics = Arrays.asList(parameterTool.get("").split(","));
        //kafkasource
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092");
        properties.setProperty("group.id", "topic_log");
        properties.setProperty("auto.offset.reset", "earliest");
        //没有开启checkpoint时，让flink提交偏移量的消费者定期自动提交偏移量
        properties.setProperty("enable.auto.commit", parameterTool.get("enable.auto.commit"));

        return new FlinkKafkaConsumer<T>(topics, deserialize.newInstance(), properties);

    }
}

package org.flink.study.practice02.souece;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * <p> kafka数据源测试 </p>
 *
 * @author hubo
 * @since 2021/10/31 21:36
 *
 * 启动参数： --kafka-server 49.232.64.199:20092 --group-id test --topic userclick
 *
 */
public class KafkaDataSourceTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaServers = parameterTool.get("kafka-server") == null ? "localhost:9092": parameterTool.get("kafka-server");
        String topic = parameterTool.get("topic");
        String groupId = parameterTool.get("group-id");

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaServers);
        properties.setProperty("group.id", groupId);

        DataStream<Object> streamSource = environment.addSource(
                new FlinkKafkaConsumer<Object>(topic, (DeserializationSchema) new SimpleStringSchema(), properties));

        streamSource.print("kafka");

        environment.execute("kafka-job");
    }
}

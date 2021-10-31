package org.flink.study.practice02.souece;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p> 从文件读取数据源 </p>
 *
 * @author hubo
 * @since 2021/10/31 21:02
 */
public class FileDataSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> streamSource = environment
                .readTextFile("E:\\Projects\\bigdata\\flink\\flink-study\\demo01\\src\\main\\resources\\userclickrecordtempdata.txt");

        streamSource.print("file");

        environment.execute("myJob");
    }
}

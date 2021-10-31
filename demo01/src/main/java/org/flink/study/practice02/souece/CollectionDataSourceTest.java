package org.flink.study.practice02.souece;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flink.study.entity.UserClickRecord;

import java.util.Arrays;

/**
 * <p> 通过集合类构建数据源测试 </p>
 *
 * @author hubo
 * @since 2021/10/31 19:21
 */
public class CollectionDataSourceTest {
    public static void main(String[] args) throws Exception {
        // 创建 Environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);

        DataStream<UserClickRecord> streamSource = environment.fromCollection(
                Arrays.asList(
                        new UserClickRecord("1", "1111", "1", "1", "1", "1"),
                        new UserClickRecord("2", "222", "1", "1", "1", "1"),
                        new UserClickRecord("3", "33", "1", "1", "1", "1"),
                        new UserClickRecord("4", "44", "1", "1", "1", "1"),
                        new UserClickRecord("5", "55", "1", "1", "1", "1"),
                        new UserClickRecord("6", "66", "1", "1", "1", "1")
                )
        );


        DataStream<UserClickRecord> elements = environment.fromElements(
                new UserClickRecord("7", "8", "1", "1", "1", "1"),
                new UserClickRecord("8", "99", "1", "1", "1", "1"),
                new UserClickRecord("9", "66", "1", "1", "1", "1")
        );



        streamSource.print("data");
        elements.print("element");

        environment.execute("myJob");

    }
}

package org.flink.study.practice02.souece;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.flink.study.entity.UserClickRecord;

import java.util.Random;
import java.util.UUID;

/**
 * <p> 自定义数据源测试 </p>
 * <p>
 *     常见常见：1.自定义连接器（官方没有提供连接器，需要自己做一个连接器）
 *             2.模拟数据源做测试数据源
 * </p>
 *
 * @author hubo
 * @since 2021/10/31 23:09
 */
public class UserDefineDataSourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<UserClickRecord> streamSource = environment.addSource(new MyUserClickSource());

        streamSource.print();

        environment.execute();

    }

    // 实现自定义的 SourceFunction
    public static class MyUserClickSource implements SourceFunction<UserClickRecord> {
        // 标志位，控制数据产生
        private boolean running = true;

        @Override
        public void run(SourceContext<UserClickRecord> ctx) throws Exception {
            Random random = new Random();
            while (running){
                ctx.collect(new UserClickRecord(
                        UUID.randomUUID().toString(),
                        System.currentTimeMillis()+"",
                        random.nextInt()+"",
                        UUID.randomUUID().toString(),
                        "features",
                        (int)(Math.random()*2) == 1 ? "1" : "0"
                ));
                Thread.sleep(5000);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

}

package org.flink.study.practice02.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.flink.study.entity.UserClickRecord;
import org.flink.study.practice02.souece.UserDefineDataSourceTest;

import java.util.Arrays;

/**
 * <p> 流数据合并转换算子测试 </p>
 *
 * @author hubo
 * @since 2021/11/5 17:20
 */
public class StreamMergeOperatorTest {
    public static void main(String[] args) throws Exception {
        String filePath = "E:\\Projects\\bigdata\\flink\\flink-study\\demo01\\src\\main\\resources\\userclickrecordtempdata.txt";
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<UserClickRecord> streamSourceFromMyUserClickSource = executionEnvironment.addSource(new UserDefineDataSourceTest.MyUserClickSource());

        DataStreamSource<String> streamSourceFromSocket = executionEnvironment.socketTextStream("192.168.116.100", 9999);

        DataStreamSource<String> streamSourceFromTextFile = executionEnvironment.readTextFile(filePath);

        DataStream<String> streamSourceFromCollection = executionEnvironment
                .fromCollection(Arrays.asList(
                    new UserClickRecord("1", "1111", "1", "1", "1", "1").toString(),
                    new UserClickRecord("2", "222", "1", "1", "1", "1").toString(),
                    new UserClickRecord("3", "33", "1", "1", "1", "1").toString(),
                    new UserClickRecord("4", "44", "1", "1", "1", "1").toString(),
                    new UserClickRecord("5", "55", "1", "1", "1", "1").toString(),
                    new UserClickRecord("6", "66", "1", "1", "1", "1").toString()
                )
        );

        /*
         * 1. Union   DataStream* → DataStream
         *      合并多个流，要求每个流中的数据类型要一致，合并同一个流时，流中的元素会得到两次，即相同的元素并不会被擦除
         **/
        DataStream<String> unionStringDataStream =
                streamSourceFromSocket.union(streamSourceFromTextFile,streamSourceFromCollection);

        unionStringDataStream.print("unionSocketAndFile");

        /*
         * 2. Connect                DataStream,DataStream → ConnectedStream
         *    CoMap, CoFlatMap       ConnectedStream → DataStream
         *    示例：streamSourceFromMyUserClickSource 是一个 UserClickRecord 类型的对象流
         *         streamSourceFromSocket 是一个 String 流，将他们连接
         *         再利用 CoMap, CoFlatMap 通过同一个类型输出
         *         (以 CoMap 为例，CoFlatMap 类似，就是 Map 和 flatMap 的区别)
         **/
        ConnectedStreams<UserClickRecord, String> connectedStream =
                streamSourceFromMyUserClickSource.connect(streamSourceFromSocket);
        SingleOutputStreamOperator<UserClickRecord> connectedStreamCoMap =
                connectedStream.map(new CoMapFunction<UserClickRecord, String, UserClickRecord>() {
            @Override
            public UserClickRecord map1(UserClickRecord value) throws Exception {
                value.setUuid("MyUserClickSource-" + value.getUuid());
                return value;
            }

            @Override
            public UserClickRecord map2(String value) throws Exception {
                String[] lineIrems = value.split(",");
                UserClickRecord record = new UserClickRecord("SocketSource-" + lineIrems[0],
                        lineIrems[1],
                        lineIrems[2],
                        lineIrems[3],
                        lineIrems[4],
                        lineIrems[5]);
                return record;
            }
        });
        connectedStreamCoMap.print("connectedStreamCoMap");


        /*
         * 3. Iterate                DataStream → IterativeStream → ConnectedStream
         *      (迭代).通过将一个算子的输出重定向到前一个算子，在流中创建一个 “反馈” 循环。这对于定义持续更新模型的算法特别有用。
         *      示例：下面的代码从一个 MyUserClickSource 流开始，并连续应用迭代体。label 为 1 的元素被发送回反馈通道，其余元素被转发到下游。
         *           out + feedback 可以形成一个反馈机制
         **/

        IterativeStream<UserClickRecord> iteration = streamSourceFromMyUserClickSource.iterate();
        SingleOutputStreamOperator<UserClickRecord> iterationBody =
                iteration.map((MapFunction<UserClickRecord, UserClickRecord>) value -> {
                    if ("1".equals(value.getLabel())){
                        value.setUuid("非正常点击：" + value.getUuid());  //对应反馈回来的数据做处理
                        value.setLabel("3"); // 重新标记
                    }
            return value;
        });
        DataStream<UserClickRecord> feedback = iterationBody.filter(
                (FilterFunction<UserClickRecord>) value -> "1".equals(value.getLabel()))
                .setParallelism(1);// 因为 streamSourceFromMyUserClickSource 的并行度为 1，所有 feedback 回去的并行度要一致
        iteration.closeWith(feedback);

        DataStream<UserClickRecord>  out = iterationBody.filter(
                (FilterFunction<UserClickRecord>) value -> !"1".equals(value.getLabel()));

        ConnectedStreams<UserClickRecord, UserClickRecord> connectOutWithFeedBack = out.connect(feedback);
        SingleOutputStreamOperator<String> connectedFeedBackStream = connectOutWithFeedBack.map(new CoMapFunction<UserClickRecord, UserClickRecord, String>() {
            @Override
            public String map1(UserClickRecord value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(UserClickRecord value) throws Exception {
                value.setUuid("非正常点击：" + value.getUuid());
                return value.toString();
            }
        });

        connectedFeedBackStream.print("connectedFeedBackStream");

        executionEnvironment.execute("unionStringDataStream");
    }
}

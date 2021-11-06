package org.flink.study.practice02.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.flink.study.entity.UserClickRecord;

/**
 * <p> 数据流分组转换测试 </p>
 *
 * @author hubo
 * @since 2021/11/4 19:37
 */
public class GroupTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputStream = executionEnvironment
                .readTextFile("E:\\Projects\\bigdata\\flink\\flink-study\\demo01\\src\\main\\resources\\userclickrecordtempdata.txt");

        // 想利用基本转换将接收到的数据装欢为 POJO 对象
        SingleOutputStreamOperator<UserClickRecord> objectStreamSource = inputStream.map((MapFunction<String, UserClickRecord>) line -> {
            String[] lineIrems = line.split(",");
            UserClickRecord record = new UserClickRecord(lineIrems[0],
                    lineIrems[1],
                    lineIrems[2],
                    lineIrems[3],
                    lineIrems[4],
                    lineIrems[5]);
            return record;
        });

        /*
         * 1. KeyBy
         * 从逻辑上将流划分为不相交的分区。  DataStream → KeyedStream
         *       key相同的记录会被分到同一个分区
         *       通过分割数据的 Hash 值实现
         * 实例：将用户点击记录根据 Label 标签分区,同一个区表示会进到用一个 TaskSlot 中
         **/
        KeyedStream<UserClickRecord, String> keyedStream = objectStreamSource.keyBy(
                (KeySelector<UserClickRecord, String>) value -> value.getLabel());

        keyedStream.print("UserClickRecordKeyedStream");

        /*
         * 2. Reduce
         * KeyedStream 流数据上的记录会按照分区，新来的数据回合之前的数据做合并，然后产生新的值。 KeyedStream → DataStream
         *             效果就像来一个消失一个
         * 实例：将前面 keyedStream 中的记录按照分区把 uuid 拼接起来（没有实际含义，做实验）
         **/
        SingleOutputStreamOperator<UserClickRecord> reduceResultSource = keyedStream.reduce((ReduceFunction<UserClickRecord>) (value1, value2) -> {
            // ur1 先到的数据，ur2 时后来的数据，会
            String uuid1 = value1.getUuid();
            value1.setUuid(uuid1 + "," + value2.getUuid());
            return value1;
        });
        reduceResultSource.print("reduceResultSource");


        /*
         * 3. Window 可以在已分区的KeyedStreams上定义窗口。
         *          KeyedStream → WindowedStream
         *    WindowAll
         *          DataStreamStream → AllWindowedStream
         * 4. Window Apply
         *          WindowedStream → DataStream  |||||||  AllWindowedStream → DataStream
         **/
        WindowedStream<UserClickRecord, String, TimeWindow> windowStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(1)));
        SingleOutputStreamOperator<Object> windowApplyStream = windowStream.apply(new WindowFunction<UserClickRecord, Object, String, TimeWindow>() {

            @Override
            public void apply(String s, TimeWindow window, Iterable<UserClickRecord> input, Collector<Object> out) throws Exception {
                StringBuffer uuids = new StringBuffer("");
                for (UserClickRecord record : input) {
                    uuids.append("," + record.getUuid()) ;
                }
                out.collect(uuids.toString());
            }
        });
        windowApplyStream.print("windApply");

        executionEnvironment.execute("GroupTransformTestJob");

    }
}

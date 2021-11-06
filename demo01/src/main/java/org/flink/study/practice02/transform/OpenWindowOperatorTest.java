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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.flink.study.entity.UserClickRecord;

/**
 * <p> 数据流开窗转换算子基础测试 </p>
 *
 * 需要针对无界流
 *
 * @author hubo
 * @since 2021/11/5 10:15
 */
public class OpenWindowOperatorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketDataSource = executionEnvironment.socketTextStream("192.168.250.55", 9999);

        // mapStream
        SingleOutputStreamOperator<UserClickRecord> mapStream = socketDataSource.map((MapFunction<String, UserClickRecord>) (line) -> {
            String[] lineItems = line.split(",");
            UserClickRecord record =
                    new UserClickRecord(lineItems[0], lineItems[1], lineItems[2], lineItems[3], lineItems[4], lineItems[5]);
            return record;
        });

        // KeyedStream
        KeyedStream<UserClickRecord, String> userClickRecordKeyedStream =
                mapStream.keyBy((KeySelector<UserClickRecord, String>) record -> record.getLabel());

        /*
         * 1. Window 可以在已分区的KeyedStreams上定义窗口。
         *          KeyedStream → WindowedStream
         *    WindowAll  （on non-keyed window stream）
         *          DataStreamStream → AllWindowedStream
         **/
        // 滚动处理窗口
        WindowedStream<UserClickRecord, String, TimeWindow> tumblWindowStream =
                userClickRecordKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 滑动处理窗口
        WindowedStream<UserClickRecord, String, TimeWindow> slidWindowStream =
                userClickRecordKeyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(5)));

        /*
         * 2. Window Apply  将常规 Function 应用于窗口
         *          WindowedStream → DataStream
         *          AllWindowedStream → DataStream
         **/
        SingleOutputStreamOperator<Object> windowApplyedStream = slidWindowStream.apply(new WindowFunction<UserClickRecord, Object, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<UserClickRecord> input, Collector<Object> out) throws Exception {
                StringBuffer uuids = new StringBuffer("");
                for (UserClickRecord record : input) {
                    uuids.append("," + record.getUuid()) ;
                }
                out.collect(uuids.toString());
            }
        });

        windowApplyedStream.print("windowApplyedStream");

        /*
         * 3. WindowReduce 将 ReduceFunction 应用到窗口中。   WindowedStream → DataStream
         **/
        SingleOutputStreamOperator<UserClickRecord> windowReducedStream = tumblWindowStream.reduce((ReduceFunction<UserClickRecord>) (valueOld, valueNew) -> {
            String newEventUuid = valueNew.getUuid();
            String uuids = valueOld.getUuid() + "," + newEventUuid;
            valueOld.setUuid(uuids);
            return valueOld;
        });

        windowReducedStream.print("windowReducedStream");

        executionEnvironment.execute();
    }
}
